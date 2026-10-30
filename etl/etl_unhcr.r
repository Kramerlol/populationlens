# ==============================================================================
# UNHCR ETL Script - Production Version
# Source: UNHCR Refugee Statistics (via `refugees` R package)
# Output:
#   unhcr_stocks.parquet + unhcr_stocks.json   (year-end positions)
#   unhcr_flows.parquet  + unhcr_flows.json    (annual events)
# Schema: See CORE_COLS under SCHEMA
# Last Updated: 30.09.2026
# Notes: The `refugees` R package loads datasets as tibbles directly.
# Config: sourced from _common.R (path resolution via POPLENS_RAW env var)

#   Three datasets:
#     population — stocks (refugees, asylum seekers, IDPs, stateless), 1951–
#     asylum_decisions — recognition rates by coo/coa, 2000–
#     asylum_applications — global applications by coo/coa/procedure, 2006–

# EXPLORATION:
# help(package="refugees")
# df <- asylum_applications
# glimpse(df)
# df_dec <- asylum_decisions
# glimpse(df_dec)
# df_pop <- population
# glimpse(df_pop)
# df_flow <- flows
# glimpse(df_flow)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Clean environment
rm(list = ls(all.names = TRUE))
`%||%` <- function(a, b) if (is.null(a)) b else a

# Load packages
library(refugees)
library(arrow)
library(jsonlite)
library(data.table)

# Configuration:
# output_dir <- "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/data/raw_data/unhcr"
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
source("_common.R") # Shared config for all PopLens ETL scripts
output_dir <- out_dir_for("unhcr")

# ==============================================================================
# SCHEMA — identical across both output files
# ==============================================================================
CORE_COLS <- c(
  "year",
  "coo_iso3", "coo_name", "coo",
  "coa_iso3", "coa_name", "coa",
  "indicator_code", "value",
  "procedure_type", "dec_level", "app_type", "app_pc", "dec_pc",
  "dataset_name"
)

# ==============================================================================
# INDICATORS
#   grain  : "stocks" | "flows" — determines which Parquet the rows land in
#   prefix : grain-encoding prefix for indicator_code (defense in depth)
# ==============================================================================
indicators <- list(
  # --- STOCK: year-end positions ------------------------------------------
  list(
    dataset     = "population",
    output_name = "population_stocks",
    grain       = "stocks",
    prefix      = "UNHCR.STOCK",
    description = paste(
      "End-of-year stock of persons of concern by country of origin and",
      "country of asylum. Includes refugees, asylum seekers, IDPs,",
      "stateless persons, and other categories."
    ),
    value_cols  = c("refugees", "asylum_seekers", "returned_refugees",
                    "idps", "returned_idps", "stateless", "ooc",
                    "oip", "hst"),
    dim_cols    = character(0)
  ),
  
  # --- FLOW: displacement (annual new arrivals) ---------------------------
  list(
    dataset     = "flows",
    output_name = "displacement_flows",
    grain       = "flows",
    prefix      = "UNHCR.FLOW",
    description = paste(
      "Annual forced displacement flows — new arrivals during the year.",
      "Distinct from population_stocks: this records newly displaced",
      "persons in a given year, not year-end positions."
    ),
    value_cols  = c("refugees", "asylum_seekers", "refugee_like", "oip"),
    dim_cols    = character(0)
  ),
  
  # --- FLOW: asylum applications (annual administrative events) ----------
  list(
    dataset     = "asylum_applications",
    output_name = "asylum_applications",
    grain       = "flows",
    prefix      = "UNHCR.APP",
    description = paste(
      "Asylum applications lodged by country of origin and country of",
      "asylum. Global complement to eurostat_asylum_applications."
    ),
    value_cols  = c("applied"),
    dim_cols    = c("procedure_type", "app_type", "dec_level", "app_pc")
  ),
  
  # --- FLOW: asylum decisions (annual administrative events) --------------
  list(
    dataset     = "asylum_decisions",
    output_name = "asylum_decisions",
    grain       = "flows",
    prefix      = "UNHCR.DEC",
    description = paste(
      "Decisions on asylum claims by country of origin and country of",
      "asylum. Includes recognized (refugee status), complementary",
      "protection, rejected, and closed outcomes."
    ),
    value_cols  = c("dec_recognized", "dec_other", "dec_rejected",
                    "dec_closed", "dec_total"),
    dim_cols    = c("procedure_type", "dec_level", "dec_pc")
  )
)

# ==============================================================================
# MELT
# ==============================================================================
# UNHCR ships wide (one column per measure); we melt to long so every value
# becomes an (indicator_code, value) pair, matching the WDI/WPP/SIPRI/OECD
# pattern. Grain is preserved in the code prefix.
melt_unhcr <- function(dt, spec) {
  # Normalise ISO column names (package ships coo_iso / coa_iso)
  if ("coo_iso" %in% names(dt)) setnames(dt, "coo_iso", "coo_iso3")
  if ("coa_iso" %in% names(dt)) setnames(dt, "coa_iso", "coa_iso3")
  
  id_cols <- intersect(
    c("year", "coo_iso3", "coo_name", "coo",
      "coa_iso3", "coa_name", "coa",
      spec$dim_cols),
    names(dt)
  )
  present_values <- intersect(spec$value_cols, names(dt))
  if (length(present_values) == 0) return(NULL)
  
  long <- melt(dt, id.vars = id_cols, measure.vars = present_values,
               variable.name = "measure", value.name = "value",
               variable.factor = FALSE)
  long <- long[!is.na(value)]
  
  # Strip dec_/app_ verb prefixes so codes read cleanly:
  #   dec_recognized -> UNHCR.DEC.RECOGNIZED
  #   applied        -> UNHCR.APP.APPLIED
  clean <- toupper(gsub("^dec_|^app_", "", long$measure))
  long[, indicator_code := paste0(spec$prefix, ".", clean)]
  long[, dataset_name     := spec$output_name]
  
  # Guarantee every output column exists (fill missing with typed NA)
  for (c in CORE_COLS) if (!c %in% names(long)) long[, (c) := NA]
  long[, ..CORE_COLS]
}

# ==============================================================================
# EXTRACT
# ==============================================================================
cat("UNHCR extraction\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

parts   <- list(stocks = list(), flows = list())
labels  <- list(stocks = character(0), flows = character(0))
skipped <- character(0)

for (spec in indicators) {
  cat("\nProcessing:", spec$output_name, "(", spec$dataset, ")\n")
  
  raw <- tryCatch(
    getExportedValue("refugees", spec$dataset),
    error = function(e) {
      cat("  error:", conditionMessage(e), "\n"); NULL
    }
  )
  if (is.null(raw) || nrow(raw) == 0) {
    cat("  no data — skipping\n")
    skipped <- c(skipped, spec$output_name); next
  }
  
  long <- melt_unhcr(as.data.table(raw), spec)
  if (is.null(long) || nrow(long) == 0) {
    cat("  melt produced no rows — skipping\n")
    skipped <- c(skipped, spec$output_name); next
  }
  
  n_countries <- uniqueN(c(long$coo_iso3, long$coa_iso3), na.rm = TRUE)
  cat("  rows:", format(nrow(long), big.mark = ","),
      " indicators:", uniqueN(long$indicator_code),
      " countries:", n_countries, "\n")
  
  parts[[spec$grain]][[length(parts[[spec$grain]]) + 1L]] <- long
  labels[[spec$grain]] <- c(labels[[spec$grain]], spec$output_name)
}

if (all(vapply(parts, length, integer(1)) == 0)) {
  cat("\nNo data extracted.\n"); quit(status = 1)
}

# ==============================================================================
# COMBINE + WRITE  (one Parquet + one JSON per grain)
# ==============================================================================
cat("\n", paste(rep("=", 70), collapse = ""), "\n")

for (grp in names(parts)) {
  if (length(parts[[grp]]) == 0) next
  
  dt <- rbindlist(parts[[grp]], use.names = TRUE, fill = TRUE)
  
  before <- nrow(dt)
  dt <- unique(dt)
  if (nrow(dt) < before)
    cat(sprintf("[%s] Dedup: removed %s rows\n",
                grp, format(before - nrow(dt), big.mark = ",")))
  
  setcolorder(dt, CORE_COLS)
  
  pq <- file.path(output_dir, paste0("unhcr_", grp, ".parquet"))
  js <- file.path(output_dir, paste0("unhcr_", grp, ".json"))
  write_parquet(dt, pq)
  
  per_dataset <- lapply(labels[[grp]], function(nm) {
    sub <- dt[dataset_name == nm]
    if (nrow(sub) == 0) return(NULL)
    list(
      dataset_name = nm,
      row_count    = nrow(sub),
      year_min     = min(sub$year, na.rm = TRUE),
      year_max     = max(sub$year, na.rm = TRUE),
      indicators   = sort(unique(sub$indicator_code))
    )
  })
  names(per_dataset) <- labels[[grp]]
  per_dataset <- per_dataset[!vapply(per_dataset, is.null, logical(1))]
  
  yr <- dt$year[!is.na(dt$year)]
  meta <- list(
    source          = "UNHCR",
    source_url      = "https://www.unhcr.org/refugee-statistics/",
    license         = "CC BY 4.0",
    citation        = "UNHCR Refugee Statistics (https://www.unhcr.org/refugee-statistics/), UNHCR. Licensed under CC BY 4.0.",
    retrieved_at    = as.character(Sys.time()),
    parquet_file    = basename(pq),
    grain           = grp,      # "stocks" | "flows"
    datasets        = labels[[grp]],
    row_count       = nrow(dt),
    n_indicators    = uniqueN(dt$indicator_code),
    indicator_codes = sort(unique(dt$indicator_code)),
    year_min        = min(yr),
    year_max        = max(yr),
    per_dataset     = per_dataset,
    columns         = names(dt)
  )
  write_json(meta, js, pretty = TRUE, auto_unbox = TRUE)
}

# ==============================================================================
# SUMMARY
# ==============================================================================
cat("\nUNHCR extraction complete\n\n")

for (grp in names(parts)) {
  if (length(parts[[grp]]) == 0) next
  pq <- file.path(output_dir, paste0("unhcr_", grp, ".parquet"))
  if (!file.exists(pq)) next
  dt <- rbindlist(parts[[grp]], use.names = TRUE, fill = TRUE)
  cat(sprintf("Grain: %-8s  %.2f MB  rows=%s  indicators=%d\n",
              grp,
              file.size(pq) / 1024^2,
              format(nrow(dt), big.mark = ","),
              uniqueN(dt$indicator_code, na.rm = TRUE)))
  cat(sprintf("  datasets: %s\n", paste(labels[[grp]], collapse = ", ")))
}

if (length(skipped) > 0)
  cat("\n  Skipped: ", paste(skipped, collapse = ", "), "\n", sep = "")


# cat("\n--- Column reference ---\n")
# cat("Join pattern for all three datasets:\n")
# cat("  Host-country analysis : coa_iso3 = reference.country_codes.iso_alpha3\n")
# cat("  Origin-country analysis: coo_iso3 = reference.country_codes.iso_alpha3\n")
# cat("  Unknown origin rows   : coo_iso3 = 'UNK' — keep, filter in views as needed\n\n")
# cat("Ingestion note: add to data_ingestion.py as a new 'unhcr' source handler.\n")
# cat("  Key columns to filter in analytics views:\n")
# cat("  - dec_pc / app_pc = 'P' (persons) vs 'C' (cases)\n")
# cat("  - app_type = 'N' (new applications only)\n")
# cat("  - procedure_type = 'G' (government) for most EU country comparisons\n")