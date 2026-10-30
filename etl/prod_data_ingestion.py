# ==============================================================================
# PopLens Data Ingestion Script - Production Version
# Purpose: Parquet + JSON -> PostgreSQL.
# Last Updated: 30.09.2026
# Functionality:
#  - Connect to PostgreSQL.
#  - For each Parquet file, read metadata from companion JSON.
#  - Insert a row into ingestion.ingestion_metadata.
#  - Load Parquet into pandas DataFrame.
#  - Apply source‑specific transformations (rename columns, add source, etc.) using your existing handlers.
#  - Resolve country_id and time_id for each row.
#  - Insert rows into the appropriate fact table.
#  - Handle idempotency (delete old rows for that dataset or use upsert).
# Reads every *.parquet under RAW_DATA_DIR/<source>/, resolves country_id
# and time_id against reference tables, and batch-inserts fact rows. Each
# Parquet gets one row in ingestion.ingestion_metadata; every fact row FKs
# to that row via metadata_id.

# Usage:
#     python data_ingestion.py                  # ingest everything
#     python data_ingestion.py --source wdi     # only one source
#     python data_ingestion.py --dry-run        # parse and report, no writes
# ==============================================================================

#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values

# ---------------------------------------------------------------------------
# CONFIG — all values come from env vars so nothing secret lives in the repo.
# ---------------------------------------------------------------------------
RAW_DATA_DIR = Path(os.getenv("POPLENS_RAW", "/root/data/parquet"))
LOG_DIR      = Path(os.getenv("POPLENS_LOG", "/var/log/poplens"))

DB = dict(
    host     = os.getenv("PGHOST",     "localhost"),
    port     = int(os.getenv("PGPORT", "5432")),
    dbname   = os.getenv("PGDATABASE", "populationlens_platform"),
    user     = os.getenv("PGUSER",     "pl_admin"),
    password = os.getenv("PGPASSWORD", ""),   # prefer ~/.pgpass over this
)

# ---------------------------------------------------------------------------
# SOURCE REGISTRY
# One entry per Parquet folder. Tells the ingester the country column name,
# how to look it up in dim_countries, the target fact table, and which
# columns are "dimensions" (go to `dims` JSONB) versus first-class.
# ---------------------------------------------------------------------------
SOURCES: Dict[str, Dict[str, Any]] = {
    "wdi": {
        "country_col":   "iso3c",
        "country_match": "iso3",
        "fact_table":    "staging.fact_indicators",
        "extra_cols":    ["age"],                     # explicit columns
        "dims_cols":     [],                          # JSONB
    },
    "wpp": {
        "country_col":   "country_code",
        "country_match": "wpp_country_code",
        "fact_table":    "staging.fact_indicators",
        "extra_cols":    ["age"],
        "dims_cols":     ["data_type"],
    },
    "sipri": {
        "country_col":   "country_code",
        "country_match": "iso3",
        "fact_table":    "staging.fact_indicators",
        "extra_cols":    [],
        "dims_cols":     ["dataset_id"],
    },
    "oecd": {
        "country_col":   "iso3",
        "country_match": "iso3",
        "fact_table":    "staging.fact_indicators",
        "col_alias":     {"AGE": "age_group"},   # rename before processing
        "extra_cols":    ["sex", "age_group"],
        "dims_cols":     ["MEASURE", "FREQ", "UNIT_MEASURE",
                          "dataset_name", "dataset_id", "valid_from"],
    },
    "eurostat": {
        "country_col":   "geo",
        "country_match": "iso2",
        "fact_table":    "staging.fact_indicators",
        "col_alias":     {"age": "age_group"},
        "extra_cols":    ["sex", "citizen", "age_group"],
        "dims_cols":     ["unit", "isced11", "applicant", "agedef",
                          "indic_de", "dataset_name", "dataset_id"],
        "skip_geo_prefixes": ["EU", "EA", "EFTA", "EEA", "CC"],
    },
    "unhcr": {
        # UNHCR has two country FKs (origin + asylum). Special-cased below.
        "country_col":   "coo_iso3",
        "coo_col":       "coo_iso3",
        "coa_col":       "coa_iso3",
        "country_match": "iso3",
        "fact_table":    "staging.fact_unhcr",
        "extra_cols":    [],
        "dims_cols":     ["procedure_type", "dec_level", "app_type",
                          "app_pc", "dec_pc", "dataset_name"],
        # UNHCR non-ISO codes to skip
        "skip_codes": ["UNK", "STA"],
    },
}

# ---------------------------------------------------------------------------
# LOGGING — one timestamped line per event, echoed to stdout for cron mail.
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / f"ingest_{datetime.now():%Y%m%d}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ingest")


# ---------------------------------------------------------------------------
# LOOKUP CACHES — build once, use for every row (avoids one query per row).
# ---------------------------------------------------------------------------
class Lookups:
    def __init__(self, cur):
        cur.execute("SELECT iso2, iso3, country_id, wpp_country_code "
                    "FROM reference.dim_countries")
        self.by_iso2, self.by_iso3, self.by_wpp = {}, {}, {}
        for iso2, iso3, cid, wpp in cur.fetchall():
            if iso2: self.by_iso2[iso2] = cid
            if iso3: self.by_iso3[iso3] = cid
            if wpp:  self.by_wpp[wpp]   = cid

        cur.execute("SELECT year, time_id FROM reference.dim_time")
        self.by_year = {int(y): t for y, t in cur.fetchall()}

    def country(self, match: str, value) -> Optional[int]:
        if value is None: return None
        v = str(value).strip()
        if not v: return None
        if match == "iso2": return self.by_iso2.get(v)
        if match == "iso3": return self.by_iso3.get(v)
        if match == "wpp_country_code":
            try: return self.by_wpp.get(int(v))
            except (ValueError, TypeError): return None
        return None

    def year(self, y) -> Optional[int]:
        try: return self.by_year.get(int(y))
        except (ValueError, TypeError): return None


# ---------------------------------------------------------------------------
# METADATA — one row per ingested Parquet.
# ---------------------------------------------------------------------------
def insert_metadata(cur, run_id, source, parquet: Path, meta: Dict,
                    row_count: int, row_inserted: int) -> int:
    cur.execute("""
        INSERT INTO ingestion.ingestion_metadata
          (source_name, dataset_name, schema_name, table_name,
           description, row_count, source_retrieved_at, retrieved_at,
           source_url, raw_metadata, parquet_uri, run_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING metadata_id
    """, (
        source,
        meta.get("parquet_file", parquet.stem),
        "staging",
        f"fact_{source}",
        meta.get("description"),
        row_inserted,
        meta.get("retrieved_at") or datetime.now(timezone.utc),
        datetime.now(timezone.utc),
        meta.get("source_url"),
        Json(meta),
        str(parquet),
        run_id,
    ))
    return cur.fetchone()[0]

# For sources with one parquet (wdi, wpp, sipri, oecd), it works. For sources with two (eurostat, unhcr), the loop ingests eurostat_by_citizenship.parquet → deletes all eurostat rows → inserts citizenship rows → then ingests eurostat_by_demographics.parquet → deletes the citizenship rows you just inserted → inserts demographics rows. 
# In order not to end up with half of the data missing and no error for these examples, key idempotency on the exact parquet file instead of the source:
def delete_prior_rows(cur, table: str, parquet_uri: str):
    """Idempotency: drop rows previously loaded from THIS parquet file."""
    cur.execute(f"""
        DELETE FROM {table} WHERE metadata_id IN (
            SELECT metadata_id FROM ingestion.ingestion_metadata
             WHERE parquet_uri = %s
        )
    """, (parquet_uri,))
    log.info("  removed %d prior rows (from %s)", cur.rowcount, Path(parquet_uri).name)


# ---------------------------------------------------------------------------
# INGESTION — one Parquet.
# ---------------------------------------------------------------------------
def ingest_parquet(cur, lookups: Lookups, source: str,
                   parquet: Path, run_id: str, dry_run: bool):
    cfg = SOURCES[source]
    json_path = parquet.with_suffix(".json")
    meta: Dict = {}
    if json_path.exists():
        try: meta = json.loads(json_path.read_text())
        except Exception as e:
            log.warning("  metadata JSON unreadable: %s", e)

    df = pd.read_parquet(parquet)
    # Apply per-source column renames before processing
    for old, new in cfg.get("col_alias", {}).items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    log.info("  %s -> %d rows, %d cols", parquet.name, len(df), len(df.columns))

    # Required columns for every source
    missing = [c for c in ("year", "indicator_code", "value")
               if c not in df.columns]
    if missing:
        log.error("  missing required columns %s — skipping", missing)
        return

    # Build the row list
    rows: List[Tuple] = []
    skipped_country = skipped_year = skipped_value = 0
    country_col = cfg["country_col"]
    match = cfg["country_match"]
    extra_cols = cfg.get("extra_cols", [])
    dims_cols  = cfg.get("dims_cols", [])
    skip_prefixes = cfg.get("skip_geo_prefixes", [])
    skip_codes    = cfg.get("skip_codes", [])

    for _, r in df.iterrows():
        # Country resolution (single-FK sources)
        if "coo_col" not in cfg:
            raw_country = r.get(country_col)
            if raw_country is None: skipped_country += 1; continue
            raw_country = str(raw_country)
            if skip_prefixes and any(raw_country.startswith(p) for p in skip_prefixes):
                skipped_country += 1; continue
            if raw_country in skip_codes:
                skipped_country += 1; continue
            cid = lookups.country(match, raw_country)
            if cid is None:
                skipped_country += 1; continue
        else:
            cid = None  # dual-FK handled below

        # Time resolution
        tid = lookups.year(r.get("year"))
        if tid is None: skipped_year += 1; continue

        # Value
        v = r.get("value")
        if pd.isna(v): skipped_value += 1; continue

        # Extra columns kept explicit
        extras = [r.get(c) if pd.notna(r.get(c)) else None for c in extra_cols]
        # Dimension columns bundled into JSONB
        dims = {c: (r.get(c) if pd.notna(r.get(c)) else None) for c in dims_cols if c in df.columns}

        ic = r.get("indicator_code")

        if "coo_col" in cfg:
            coo = lookups.country(match, str(r.get(cfg["coo_col"]) or ""))
            coa = lookups.country(match, str(r.get(cfg["coa_col"]) or ""))
            if coo is None or coa is None:
                skipped_country += 1; continue
            rows.append((coo, coa, tid, ic, float(v), Json(dims), 0))  # metadata filled below
        else:
            rows.append((cid, tid, source, ic, float(v), *extras, Json(dims), 0))

    log.info("  resolved %d rows  (skipped: %d country / %d year / %d value)",
             len(rows), skipped_country, skipped_year, skipped_value)

    if dry_run or not rows:
        return

    # Idempotency: re-running a single parquet replaces only that parquet's data
    delete_prior_rows(cur, cfg["fact_table"], str(parquet))
    mid = insert_metadata(cur, run_id, source, parquet, meta,
                          row_count=len(df), row_inserted=len(rows))
    # Patch metadata_id=mid into every row's last column
    rows = [row[:-1] + (mid,) for row in rows]

    # Batch insert
    if "coo_col" in cfg:
        sql = ("INSERT INTO staging.fact_unhcr "
               "(coo_country_id, coa_country_id, time_id, indicator_code, "
               " value, dims, metadata_id) VALUES %s")
    else:
        extra_defs = ", ".join(cfg.get("extra_cols", []))
        extra_ph   = (", " + extra_defs) if extra_defs else ""
        sql = (f"INSERT INTO {cfg['fact_table']} "
               f"(country_id, time_id, source_name, indicator_code, value{extra_ph}, "
               f" dims, metadata_id) VALUES %s")

    execute_values(cur, sql, rows, page_size=2000)
    log.info("  inserted %d rows into %s", len(rows), cfg["fact_table"])


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="Ingest only this source folder")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse and report; do not write to PostgreSQL")
    args = ap.parse_args()

    run_id = str(uuid.uuid4())
    log.info("run_id=%s  dry_run=%s  source=%s",
             run_id, args.dry_run, args.source or "ALL")

    sources = [args.source] if args.source else list(SOURCES.keys())
    sources = [s for s in sources if (RAW_DATA_DIR / s).exists()]

    if not sources:
        log.error("no source folders found under %s", RAW_DATA_DIR)
        sys.exit(1)

    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            lookups = Lookups(cur)
            for source in sources:
                log.info("[%s]", source)
                folder = RAW_DATA_DIR / source
                for pq in sorted(folder.glob("*.parquet")):
                    ingest_parquet(cur, lookups, source, pq, run_id, args.dry_run)
        if not args.dry_run:
            conn.commit()
            log.info("committed")
        else:
            conn.rollback()
            log.info("dry-run complete — rolled back")
    except Exception as e:
        conn.rollback()
        log.exception("ingestion failed: %s", e)
        sys.exit(2)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
