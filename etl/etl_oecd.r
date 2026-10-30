# ==============================================================================
# OECD ETL Script - Production Version
# Source: OECD.Stat via OECD::get_dataset() (SDMX REST)
# Output: oecd_all.parquet + oecd_all.json
# Schema: See CORE_COLS in Config
# Last Updated: 29.09.2026
# Notes: License is CC BY 4.0 (OECD, post July 2024)
# Config: sourced from _common.R (path resolution via POPLENS_RAW env var)

# TESTING
# remotes::install_github("expersso/OECD")
# packageVersion("OECD")
#install.packages("rsdmx")
#help(package = "OECD")
# library(rsdmx)
# library(remotes)
# library(broom)

# Pull a CSV of indicators:
# flows <- readSDMX("https://sdmx.oecd.org/public/rest/dataflow?detail=allcompletestubs")
# flows_df <- as.data.frame(flows)
# write.csv(flows_df, file="oecd_indicators.csv")

# Find SOCX-related flows
#subset(flows_df, grepl("SOCX|Social expenditure", Name, ignore.case = TRUE))

# dataset_pag <- "OECD.ELS.SPD,DSD_PAG@DF_PAG,1.0"
# 
# dsd <- get_data_structure(dataset_pag)
# 
# dataset <- "OECD.SDD.NAD,DSD_NAAG@DF_NAAG_I,1.0"
# filter <- "A.USA+EU.B1GQ_R_POP+B1GQ_R_GR.USD_PPP_PS+PC."
# 
# 
# dsd <- get_data_structure("SOCX_AGG")
# names(dsd)
# 
# 
# df <- get_dataset(dataset, filter)
# head(df)
# 
# data_structure <- get_data_structure(dataset)
# str(data_structure, max.level = 1)
# 
# df_pag <- OECD::get_dataset(
#   dataset_pag,
#   start_time = 2000,
#   end_time = 2020,          # Get all available years up to present
# )
# df_pag <- as.data.frame(df_pag)
# glimpse(df_pag)
# unique(data$MEASURE)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Clean environment
rm(list = ls(all.names = TRUE))
`%||%` <- function(a, b) if (is.null(a)) b else a

# Load packages
library(OECD)
library(arrow)
library(jsonlite)
library(data.table)
library(xml2)

# Configuation
# output_dir <- "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/data/raw_data/oecd"
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
source("_common.R")
output_dir <- out_dir_for("oecd")

# OECD dataset IDs use the full SDMX path format:
# "<agencyID>,<dataflowID>,<version>"
# Version 1.0 confirmed for DF_PAG; assumed 1.0 for siblings in same agency.
DEFAULT_AGENCY  <- "OECD.ELS.SPD"
DEFAULT_VERSION <- "1.0"

# Columns to drop before saving — metadata scaffolding not needed downstream
DROP_COLS <- c("DECIMALS", "OBS_STATUS", "OPTIONALITY", "UNIT_MULT")

# Every row carries these columns.
CORE_COLS <- c(
  "iso3", "year", "indicator_code", "value",
  "MEASURE", "AGE", "SEX", "FREQ", "UNIT_MEASURE",
  "dataset_name", "dataset_id", "valid_from", "obs_count"
)

# ==============================================================================
# INDICATORS
# ==============================================================================
indicators <- list(
  # ── Pension system structure & adequacy ─────────────────────────────────────
  list(
    dataflow    = "DSD_PAG@DF_PAG",
    output_name = "pag_pensions_at_a_glance",
    description = paste(
      "Pensions at a Glance — flagship overview: effective retirement ages,",
      "net replacement rates, public pension spending as % of GDP.",
      "MEASURE dimension separates the underlying indicators."
    ),
    start_time  = 2000
  ),
  list(
    dataflow    = "DSD_PAG@DF_PRR",
    output_name = "pag_pension_replacement_rates",
    description = paste(
      "Gross and net pension replacement rates by earnings level and sex.",
      "Adequacy indicator: retirement income vs. pre-retirement earnings."
    ),
    start_time  = 2000
  ),
  list(
    dataflow    = "DSD_PAG@DF_PW",
    output_name = "pag_pension_wealth",
    description = paste(
      "Pension wealth — lifetime present value of pension entitlements,",
      "by sex and earnings level. Multiples of annual gross earnings."
    ),
    start_time  = 2000
  ),
  # ── Demographic & economic context ──────────────────────────────────────────
  list(
    dataflow    = "DSD_PAG@DF_DEC",
    output_name = "pag_demographic_economic_context",
    description = paste(
      "Demographic and economic context for pensions: old-age dependency",
      "ratios, life expectancy at 65, GDP per capita, employment rates",
      "by age group."
    ),
    start_time  = 1990
  ),
  # ── Fiscal sustainability (SOCX; different DSD, different shape) ────────────
  list(
    dataflow    = "DSD_SOCX_AGG@DF_NET_GDP",
    output_name = "socx_net_social_expenditure_pct_gdp",
    description = paste(
      "Net total social expenditure as % of GDP — mandatory + voluntary",
      "public and private. Fiscal comparison across OECD members."
    ),
    start_time  = 1980
  )
)

# ==============================================================================
# FETCH ONE DATASET + NORMALISE TO CORE SCHEMA
# ==============================================================================
fetch_oecd <- function(dataflow, output_name, start_time,
                       agency = DEFAULT_AGENCY, version = DEFAULT_VERSION,
                       attempts = 3) {
  
  dataset_id <- paste(agency, dataflow, version, sep = ",")
  cat("\nProcessing:", output_name, "\n")
  cat("  Dataset ID:", dataset_id, "\n")
  cat("  Start year:", start_time, "\n")
  
  # Contentconstraint metadata: per-dataflow version marker + observation count
  cc <- fetch_contentconstraint(agency, dataflow)
  cat("  ValidFrom:  ",
      if (is.na(cc$valid_from)) "(unavailable)" else cc$valid_from,
      "\n", sep = "")
  if (!is.na(cc$obs_count))
    cat("  obs_count:  ", format(cc$obs_count, big.mark = ","), "\n", sep = "")
  
  # Retry loop with backoff — the OECD SDMX endpoint occasionally times out
  # on large pulls, and a single failed attempt should not kill the run.
  d <- NULL
  for (i in seq_len(attempts)) {
    d <- tryCatch(
      OECD::get_dataset(dataset = dataset_id, start_time = start_time),
      error   = function(e) { cat("    error:", e$message, "\n"); NULL },
      warning = function(w) { cat("    warning:", w$message, "\n"); NULL }
    )
    if (!is.null(d) && nrow(d) > 0) break
    if (i < attempts) {
      delay <- 5 * i
      cat("    retry", i, "of", attempts - 1, "- waiting", delay, "s...\n")
      Sys.sleep(delay)
    }
  }
  if (is.null(d) || nrow(d) == 0) {
    cat("  No data returned - skipping\n")
    return(NULL)
  }
  
  dt <- as.data.table(d)
  
  # Standard SDMX column names -> our canonical names
  ren <- c(REF_AREA = "iso3", TIME_PERIOD = "year", ObsValue = "value")
  for (old in names(ren)) {
    if (old %in% names(dt) && !(ren[[old]] %in% names(dt)))
      setnames(dt, old, ren[[old]])
  }
  
  # Type coercion
  if ("year"  %in% names(dt)) dt[, year  := as.integer(year)]
  if ("value" %in% names(dt)) dt[, value := suppressWarnings(as.numeric(value))]
  if ("AGE"   %in% names(dt)) dt[, AGE   := as.character(AGE)]
  if ("SEX"   %in% names(dt)) dt[, SEX   := as.character(SEX)]
  
  # Drop scaffolding columns
  drop <- intersect(DROP_COLS, names(dt))
  if (length(drop) > 0) dt[, (drop) := NULL]
  
  # Composite indicator code: OECD.<dataset>.<MEASURE>
  base <- toupper(paste0("OECD.", output_name))
  if ("MEASURE" %in% names(dt) && uniqueN(dt$MEASURE) > 1) {
    # Multi-measure dataset: suffix with MEASURE so codes stay unique.
    dt[, indicator_code := paste0(base, ".", MEASURE)]
  } else {
    # Single-measure dataset: the dataset name already describes the code.
    dt[, indicator_code := base]
    if (!"MEASURE" %in% names(dt)) dt[, MEASURE := NA_character_]
  }
  
  # Provenance columns carried per row
  dt[, dataset_name := output_name]
  dt[, dataset_id   := dataset_id]
  dt[, valid_from   := cc$valid_from]
  dt[, obs_count    := cc$obs_count]
  
  # Ensure every core column exists (fill missing with typed NA)
  for (c in CORE_COLS) if (!c %in% names(dt)) dt[, (c) := NA]
  
  # Narrow to core schema before returning
  dt <- dt[, ..CORE_COLS]
  
  # Drop rows without a value or year — keeps the write lean
  before <- nrow(dt)
  dt <- dt[!is.na(value) & !is.na(year)]
  if (nrow(dt) < before)
    cat("  Dropped", format(before - nrow(dt), big.mark = ","),
        "rows with NA value/year\n")
  
  # Per-dataset summary
  cat("  Rows:", format(nrow(dt), big.mark = ","), "\n")
  cat("  Countries:", uniqueN(dt$iso3, na.rm = TRUE), "\n")
  cat("  Years:", paste(range(dt$year, na.rm = TRUE), collapse = " - "), "\n")
  if (any(!is.na(dt$MEASURE)))
    cat("  MEASURE codes:", uniqueN(dt$MEASURE, na.rm = TRUE), "\n")
  
  dt
}

# ------------------------------------------------------------------------------
# fetch_contentconstraint(): version marker + observation count for a dataflow
# ------------------------------------------------------------------------------
# OECD updates each dataflow independently — there is no single "database
# release date" the way there is for WDI. Instead, each dataflow carries an
# SDMX contentconstraint with:
#   - validFrom  : ISO 8601 timestamp of the last successful data update
#   - obs_count  : total observations OECD reports for the dataflow
#
# Endpoint pattern (from OECD API best-practices docs):
#   https://sdmx.oecd.org/public/rest/contentconstraint/{AGENCY}/CR_A_{DATAFLOW}/
#
# Example:
#   https://sdmx.oecd.org/public/rest/contentconstraint/OECD.ELS.SPD/CR_A_DSD_SOCX_AGG@DF_SOCX_AGG/
#
# Returns list(valid_from=<chr>, obs_count=<int>); both NA on failure, which
# is non-fatal — the pipeline continues with retrieved_at as a fallback
# version marker.
fetch_contentconstraint <- function(agency, dataflow) {
  url <- sprintf(
    "https://sdmx.oecd.org/public/rest/contentconstraint/%s/CR_A_%s/",
    agency, dataflow
  )
  
  doc <- tryCatch(xml2::read_xml(url), error = function(e) NULL)
  if (is.null(doc))
    return(list(valid_from = NA_character_, obs_count = NA_integer_))
  
  # validFrom sits on the ContentConstraint element itself
  vf_node    <- xml2::xml_find_first(doc, "//*[@validFrom]")
  valid_from <- if (!is.na(vf_node)) xml2::xml_attr(vf_node, "validFrom") else NA_character_
  
  # obs_count lives in <Annotation id="obs_count"><AnnotationTitle>...</...>
  oc_node <- xml2::xml_find_first(
    doc,
    "//*[local-name()='Annotation'][@id='obs_count']/*[local-name()='AnnotationTitle']"
  )
  obs_count <- if (!is.na(oc_node))
    suppressWarnings(as.integer(xml2::xml_text(oc_node))) else NA_integer_
  
  list(valid_from = valid_from, obs_count = obs_count)
}

# ==============================================================================
# MAIN LOOP
# ==============================================================================
cat("OECD extraction\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

parts   <- list()
labels  <- character(0)
skipped <- character(0)

for (ind in indicators) {
  dt <- fetch_oecd(
    dataflow    = ind$dataflow,
    output_name = ind$output_name,
    start_time  = ind$start_time
  )
  if (is.null(dt)) { skipped <- c(skipped, ind$output_name); next }
  parts[[length(parts) + 1L]] <- dt
  labels <- c(labels, ind$output_name)
  Sys.sleep(1.0)   # polite gap between SDMX calls
}

if (length(parts) == 0) {
  cat("\nNo data extracted.\n")
  quit(status = 1)
}

# ==============================================================================
# COMBINE + WRITE
# ==============================================================================
cat("\n", paste(rep("=", 70), collapse = ""), "\n")

dt <- rbindlist(parts, use.names = TRUE, fill = TRUE)

before <- nrow(dt)
dt <- unique(dt)
if (nrow(dt) < before)
  cat("Dedup: removed", format(before - nrow(dt), big.mark = ","),
      "rows\n")

setcolorder(dt, CORE_COLS)

pq <- file.path(output_dir, "oecd_all.parquet")
js <- file.path(output_dir, "oecd_all.json")
write_parquet(dt, pq)

# Per-dataset metadata for the JSON
per_dataset <- lapply(labels, function(nm) {
  sub <- dt[dataset_name == nm]
  list(
    dataset_name = nm,
    dataset_id   = sub$dataset_id[1],
    valid_from   = sub$valid_from[1],
    obs_count    = sub$obs_count[1],
    stored_rows  = nrow(sub),
    n_countries  = uniqueN(sub$iso3, na.rm = TRUE),
    year_min     = min(sub$year, na.rm = TRUE),
    year_max     = max(sub$year, na.rm = TRUE),
    measures     = sort(unique(stats::na.omit(sub$MEASURE)))
  )
})
names(per_dataset) <- labels

yr <- dt$year[!is.na(dt$year)]
meta <- list(
  source          = "OECD",
  source_url      = "https://data-explorer.oecd.org/",
  license         = "CC BY 4.0 (OECD, post July 2024)",
  citation        = "OECD Data Explorer (https://data-explorer.oecd.org/), CC BY 4.0",
  retrieved_at    = as.character(Sys.time()),
  valid_from_range = {
    vf <- unique(stats::na.omit(dt$valid_from))
    if (length(vf) == 0) NA_character_ else paste(range(vf), collapse = " to ")
  },
  parquet_file    = basename(pq),
  datasets        = labels,
  row_count       = nrow(dt),
  n_datasets      = length(labels),
  n_countries     = uniqueN(dt$iso3, na.rm = TRUE),
  n_indicators    = uniqueN(dt$indicator_code, na.rm = TRUE),
  indicator_codes = sort(unique(stats::na.omit(dt$indicator_code))),
  year_min        = min(yr),
  year_max        = max(yr),
  per_dataset     = per_dataset,
  columns         = names(dt)
)
write_json(meta, js, pretty = TRUE, auto_unbox = TRUE)

# ==============================================================================
# SUMMARY
# ==============================================================================
cat("\nOECD extraction complete\n\n")
cat("  Parquet:     ", basename(pq), " (",
    round(file.size(pq) / 1024^2, 2), " MB)\n", sep = "")
cat("  Rows:        ", format(nrow(dt), big.mark = ","), "\n", sep = "")
cat("  Datasets:    ", length(labels), "\n", sep = "")
cat("  Countries:   ", uniqueN(dt$iso3, na.rm = TRUE), "\n", sep = "")
cat("  Indicators:  ", uniqueN(dt$indicator_code, na.rm = TRUE), "\n", sep = "")
cat("  Years:       ", min(yr), " - ", max(yr), "\n", sep = "")

if (length(skipped) > 0)
  cat("\n  Skipped: ", paste(skipped, collapse = ", "), "\n", sep = "")

# ==============================================================================
# NOTES: Measure codes
# ==============================================================================
# DF_PAG / DF_DEC:
#   ELMEA  — Effective mean retirement age (years)
#   ELMEN  — Effective retirement age, men
#   ELMEW  — Effective retirement age, women
#   NRRG   — Net replacement rate, gross
#   BPENS  — Public pension spending (% GDP)
#   ... run unique(data$MEASURE) after fetching to see the full list
#
# DF_PRR:
#   GRPR   — Gross replacement rate
#   NEPR   — Net replacement rate
#
# DF_PW:
#   GRPW   — Gross pension wealth (multiples of annual earnings)
#   NEPW   — Net pension wealth
#
# DF_NET_GDP:
#   Single measure per row; NET_SOCX or similar — verify after pull.
#
# When building staging tables: keep MEASURE as a dimension column (like Eurostat
# keeps `indic_de`). Do NOT pivot wide at this stage — let analytics views filter
# to the specific measure needed for each chart.
# ==============================================================================