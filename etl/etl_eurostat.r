# ==============================================================================
# EUROSTAT ETL Script - Production Version
# Source: Eurostat (https://ec.europa.eu/eurostat) — CC BY 4.0
# Output: two Parquet + two JSON files, split by whether the grain includes
#         citizenship. Both files share the same schema; unused dimension
#         columns are NA. See SCHEMA for columns
# Schema: See columns under SCHEMA
# Last Updated: 30.09.2026
# Config: sourced from _common.R (path resolution via POPLENS_RAW env var)

## Why These 14 Indicators?
#   
# Coverage of PopLens use cases:
# 1. Population projections → Use WPP baseline + Eurostat recent data
# 2. Pension modeling → Need age structure, life expectancy, employment rates
# 3. Migration scenarios → Immigration, emigration, asylum data
# 4. Fertility policy → TFR, age-specific rates
# 5. Labor force forecasting → Employment by age, unemployment
# 
# Data quality:
# - All are well-maintained annual series
# - Good coverage of EU27
# - Long time horizons (1990s+)
# - Policy-relevant

# Diagnostics:
# help(package = "eurostat")
# get_eurostat_folder("migr_asyappctzm", env = .EurostatEnv)
# toc <- get_eurostat_toc()
# ==============================================================================

# Clean environment
rm(list = ls(all.names = TRUE))
`%||%` <- function(a, b) if (is.null(a)) b else a

# Load packages
library(eurostat)
library(arrow)
library(jsonlite)
library(data.table)

# Configuration
# output_dir <- "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/data/raw_data/eurostat"
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
source("_common.R") # Shared config for all PopLens ETL scripts
output_dir <- out_dir_for("eurostat") 

# ==============================================================================
# SCHEMA
# ==============================================================================
# Universal columns — present on every row
UNIVERSAL_COLS <- c(
  "geo", "year", "indicator_code", "value",
  "unit", "dataset_name", "dataset_id"
)

# Dimension columns — dataset-specific; NA on datasets that don't use them.
# Kept as first-class columns (rather than folded into indicator_code) so
# queries can filter on the dimension directly, e.g. WHERE sex = 'F'.
DIMENSION_COLS <- c(
  "sex", "age", "citizen",
  "isced11", "applicant", "agedef",
  "indic_de"
)

CORE_COLS <- c(UNIVERSAL_COLS, DIMENSION_COLS)

# ==============================================================================
# INDICATORS
#   group = "citizenship"  -> eurostat_by_citizenship.parquet
#           "demographics" -> eurostat_by_demographics.parquet
# The split is by output grain, not subject matter.
# ==============================================================================
indicators <- list(
  # ---- Demographics ------------------------------------------------
  list(dataset_id = "demo_pjan",       name = "population_by_age_sex",
       group = "demographics",
       description = "Population on 1 January by age and sex"),
  list(dataset_id = "demo_pjangroup",  name = "population_age_groups",
       group = "demographics",
       description = "Population by 5-year age groups"),
  list(dataset_id = "demo_find",       name = "fertility_indicators",
       group = "demographics",
       description = "Fertility indicators (TFR, mean age at childbirth)"),
  list(dataset_id = "demo_fasec",      name = "fertility_age_specific",
       group = "demographics",
       description = "Age-specific fertility rates"),
  list(dataset_id = "demo_mlexpec",    name = "life_expectancy",
       group = "demographics",
       description = "Life expectancy by age and sex"),
  list(dataset_id = "demo_magec",      name = "deaths_by_age",
       group = "demographics",
       description = "Deaths by age group and sex"),
  list(dataset_id = "demo_gind",       name = "demographic_balance",
       group = "demographics",
       description = "Crude rates (birth, death, natural change, net migration)"),
  list(dataset_id = "une_rt_a",        name = "unemployment_rate",
       group = "demographics",
       description = "Unemployment rate by age and sex"),
  list(dataset_id = "ilc_peps01",      name = "poverty_risk",
       group = "demographics",
       description = "At-risk-of-poverty rate"),
  list(dataset_id = "edat_lfse_03",    name = "educational_attainment",
       group = "demographics",
       description = "Educational attainment by age and sex"),
  
  # ---- Citizenship --------------------------------------------------
  list(dataset_id = "migr_imm1ctz",    name = "immigration_by_citizenship",
       group = "citizenship",
       description = "Immigration by citizenship, age and sex"),
  list(dataset_id = "migr_emi1ctz",    name = "emigration_by_citizenship",
       group = "citizenship",
       description = "Emigration by citizenship, age and sex"),
  # Updated to annual aggregations to prevent all other months from dropping
  # in the de-dupe step; full explanation:
  # migr_asyappctzm ends in m — monthly. Eurostat ships 12 rows per 
  # country-year-dimension combination. time_format = "num" returns fractional 
  #years (2023.083, 2023.167, …), and then as.integer(year) truncates all 12 to 
  # just 2023. The dedup step below then keeps one arbitrary month and throws 
  # the other 11 away.
  list(dataset_id = "migr_asyappctza", name = "asylum_applications",
       group = "citizenship", 
       description = "Asylum applicants by citizenship, age, sex, applicant type (annual)"),
  list(dataset_id = "lfsa_argan",      name = "employment_by_age_sex",
       group = "citizenship",
       description = "Employment by age, sex, citizenship")
)

# ==============================================================================
# HELPERS
# ==============================================================================
# Fetch with retry
fetch_eurostat <- function(dataset_id, attempts = 3) {
  d <- NULL
  for (i in seq_len(attempts)) {
    d <- tryCatch(
      get_eurostat(
        id               = dataset_id,
        time_format      = "num",
        type             = "code",
        cache            = TRUE,
        stringsAsFactors = FALSE
      ),
      error = function(e) { cat("    error:", conditionMessage(e), "\n"); NULL }
    )
    if (!is.null(d) && nrow(d) > 0) break
    if (i < attempts) {
      delay <- 3 * i
      cat("    retry", i, "of", attempts - 1, "- waiting", delay, "s...\n")
      Sys.sleep(delay)
    }
  }
  if (is.null(d) || nrow(d) == 0) return(NULL)
  as.data.table(d)
}

normalize_eurostat <- function(dt, dataset_id, dataset_name) {
  # --- rename time column (package has used 'time' and 'TIME_PERIOD') ------
  for (tc in c("TIME_PERIOD", "time", "Time", "TIME")) {
    if (tc %in% names(dt) && tc != "year") {
      setnames(dt, tc, "year")
      break
    }
  }
  
  # --- rename value column to 'value' -------------------------------------
  for (vc in c("values", "value", "OBS_VALUE")) {
    if (vc %in% names(dt) && vc != "value") {
      setnames(dt, vc, "value")
      break
    }
  }
  
  # --- drop scaffolding ---------------------------------------------------
  for (c in intersect("freq", names(dt))) dt[, (c) := NULL]
  
  # --- type coercion ------------------------------------------------------
  if ("year"  %in% names(dt)) dt[, year  := as.integer(year)]
  if ("value" %in% names(dt)) dt[, value := suppressWarnings(as.numeric(value))]
  
  # --- indicator_code -----------------------------------------------------
  # Base code embeds dataset name. indic_de appended only when it varies
  # (single-measure datasets get a clean, un-suffixed code).
  base <- toupper(paste0("EUROSTAT.", dataset_name))
  if ("indic_de" %in% names(dt) && uniqueN(dt$indic_de, na.rm = TRUE) > 1) {
    dt[, indicator_code := paste0(base, ".", indic_de)]
  } else {
    dt[, indicator_code := base]
    if (!"indic_de" %in% names(dt)) dt[, indic_de := NA_character_]
  }
  
  dt[, dataset_name := dataset_name]
  dt[, dataset_id   := dataset_id]
  
  # --- ensure every core column exists ------------------------------------
  for (c in CORE_COLS) if (!c %in% names(dt)) dt[, (c) := NA]
  
  dt <- dt[, ..CORE_COLS]
  
  # --- drop rows without value or year ------------------------------------
  before <- nrow(dt)
  dt <- dt[!is.na(value) & !is.na(year)]
  if (nrow(dt) < before)
    cat("    dropped", format(before - nrow(dt), big.mark = ","), "NA rows\n")
  
  dt
}

# ==============================================================================
# MAIN LOOP
# ==============================================================================
cat("Eurostat extraction\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

parts   <- list(citizenship = list(), demographics = list())
labels  <- list(citizenship = character(0), demographics = character(0))
skipped <- character(0)

for (ind in indicators) {
  cat("\nProcessing:", ind$name, "(", ind$dataset_id, ")\n")
  
  raw <- fetch_eurostat(ind$dataset_id)
  if (is.null(raw)) {
    cat("  no data — skipping\n")
    skipped <- c(skipped, ind$name)
    next
  }
  
  dt <- normalize_eurostat(raw, ind$dataset_id, ind$name)
  
  cat("  rows:", format(nrow(dt), big.mark = ","),
      " countries:", uniqueN(dt$geo, na.rm = TRUE),
      " indicators:", uniqueN(dt$indicator_code, na.rm = TRUE),
      "\n")
  
  parts[[ind$group]][[length(parts[[ind$group]]) + 1L]] <- dt
  labels[[ind$group]] <- c(labels[[ind$group]], ind$name)
  
  Sys.sleep(0.5)
}

if (all(vapply(parts, length, integer(1)) == 0)) {
  cat("\nNo data extracted.\n"); quit(status = 1)
}

# ==============================================================================
# COMBINE + WRITE  (one Parquet + one JSON per group)
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
  
  pq <- file.path(output_dir, paste0("eurostat_by_", grp, ".parquet"))
  js <- file.path(output_dir, paste0("eurostat_by_", grp, ".json"))
  write_parquet(dt, pq)
  
  per_dataset <- lapply(labels[[grp]], function(nm) {
    sub <- dt[dataset_name == nm]
    if (nrow(sub) == 0) return(NULL)
    list(
      dataset_id = sub$dataset_id[1],
      row_count  = nrow(sub),
      n_geo      = uniqueN(sub$geo, na.rm = TRUE),
      year_min   = min(sub$year, na.rm = TRUE),
      year_max   = max(sub$year, na.rm = TRUE),
      units      = sort(unique(stats::na.omit(sub$unit))),
      indicators = sort(unique(stats::na.omit(sub$indicator_code)))
    )
  })
  names(per_dataset) <- labels[[grp]]
  per_dataset <- per_dataset[!vapply(per_dataset, is.null, logical(1))]
  
  yr <- dt$year[!is.na(dt$year)]
  meta <- list(
    source          = "Eurostat",
    source_url      = "https://ec.europa.eu/eurostat/",
    license         = "CC BY 4.0",
    citation        = "Eurostat, https://ec.europa.eu/eurostat/, CC BY 4.0",
    retrieved_at    = as.character(Sys.time()),
    parquet_file    = basename(pq),
    group           = grp,
    datasets        = labels[[grp]],
    row_count       = nrow(dt),
    n_datasets      = length(labels[[grp]]),
    n_geo           = uniqueN(dt$geo, na.rm = TRUE),
    n_indicators    = uniqueN(dt$indicator_code, na.rm = TRUE),
    indicator_codes = sort(unique(stats::na.omit(dt$indicator_code))),
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
cat("\nEurostat extraction complete\n\n")

for (grp in names(parts)) {
  if (length(parts[[grp]]) == 0) next
  pq <- file.path(output_dir, paste0("eurostat_by_", grp, ".parquet"))
  if (!file.exists(pq)) next
  dt <- rbindlist(parts[[grp]], use.names = TRUE, fill = TRUE)
  cat(sprintf("Group: %-14s  %.2f MB  rows=%s  indicators=%d\n",
              grp,
              file.size(pq) / 1024^2,
              format(nrow(dt), big.mark = ","),
              uniqueN(dt$indicator_code, na.rm = TRUE)))
  cat(sprintf("  datasets: %s\n",
              paste(labels[[grp]], collapse = ", ")))
}

if (length(skipped) > 0)
  cat("\n  Skipped: ", paste(skipped, collapse = ", "), "\n", sep = "")
