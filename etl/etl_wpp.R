# ==============================================================================
# WPP 2024 ETL Script - Production Version
# Source:  UN World Population Prospects (via `wpp2024` R package)
# Output: wpp_all.parquet + wpp_all.json
# Schema: country_code, year, indicator_code, value, age, data_type
# Last Updated: 28.09.2026
# Config: sourced from _common.R (path resolution via POPLENS_RAW env var)

# Diagnostics:
# help(wpp2024)
# ?pop
# data("migprojAge1dt")
# colnames(e0proj1dt)
# wpp.indicator()
# ==============================================================================

# Clean environment
rm(list = ls(all.names = TRUE))
`%||%` <- function(a, b) if (is.null(a)) b else a

library(wpp2024)
library(arrow)
library(jsonlite)
library(data.table)

# Configuration
# output_dir <- "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/data/raw_data/wpp"
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
source("_common.R") # Shared config for all PopLens ETL scripts
output_dir <- out_dir_for("wpp")

# WPP aggregates (World, regions, sub-regions) are handled by
# reference.dim_aggregates + dim_aggregate_members, so they are excluded here.
exclude_codes <- 900:999

# ==============================================================================
# LOADER
# ==============================================================================
load_one <- function(code) {
  if (is.null(code) || !nzchar(code)) return(NULL)
  obj <- tryCatch(get(code, envir = globalenv(), inherits = TRUE),
                  error = function(e) NULL)
  if (is.data.frame(obj)) return(as.data.table(obj))
  
  e <- new.env()
  ok <- tryCatch({
    utils::data(list = code, package = "wpp2024", envir = e, verbose = FALSE)
    exists(code, envir = e, inherits = FALSE)
  }, error = function(e) FALSE)
  if (isTRUE(ok)) return(as.data.table(get(code, envir = e, inherits = FALSE)))
  
  obj <- tryCatch(get(code, envir = asNamespace("wpp2024"), inherits = FALSE),
                  error = function(e) NULL)
  if (is.data.frame(obj)) return(as.data.table(obj))
  NULL
}

load_first <- function(candidates) {
  if (is.null(candidates)) return(NULL)
  for (c in candidates) {
    if (is.null(c) || !nzchar(c)) next
    d <- load_one(c)
    if (!is.null(d)) return(d)
  }
  NULL
}

# ==============================================================================
# INDICATORS
# ==============================================================================
indicators <- list(
  list(
    estimate_code = "pop1dt", projection_code = "popproj1dt",
    output_name = "population_total",
    description = "Total population by sex",
    code_map = c(
      pop = "WPP.POP.TOTAL", popM = "WPP.POP.MALE", popF = "WPP.POP.FEMALE",
      pop_80l = "WPP.POP.TOTAL.80L", pop_80u = "WPP.POP.TOTAL.80U",
      pop_95l = "WPP.POP.TOTAL.95L", pop_95u = "WPP.POP.TOTAL.95U",
      pop_low = "WPP.POP.TOTAL.LOW", pop_high = "WPP.POP.TOTAL.HIGH"
    )
  ),
  list(
    estimate_code = "e01dt", projection_code = "e0proj1dt",
    output_name = "life_expectancy",
    description = "Life expectancy at birth by sex",
    code_map = c(
      e0M = "WPP.E0.MALE", e0F = "WPP.E0.FEMALE", e0B = "WPP.E0.BOTH",
      e0M_80l = "WPP.E0.MALE.80L",   e0M_80u = "WPP.E0.MALE.80U",
      e0M_95l = "WPP.E0.MALE.95L",   e0M_95u = "WPP.E0.MALE.95U",
      e0F_80l = "WPP.E0.FEMALE.80L", e0F_80u = "WPP.E0.FEMALE.80U",
      e0F_95l = "WPP.E0.FEMALE.95L", e0F_95u = "WPP.E0.FEMALE.95U",
      e0B_80l = "WPP.E0.BOTH.80L",   e0B_80u = "WPP.E0.BOTH.80U",
      e0B_95l = "WPP.E0.BOTH.95L",   e0B_95u = "WPP.E0.BOTH.95U"
    )
  ),
  list(
    estimate_code = "tfr1dt", projection_code = "tfrproj1dt",
    output_name = "fertility_tfr",
    description = "Total Fertility Rate",
    code_map = c(
      tfr = "WPP.TFR",
      tfr_80l = "WPP.TFR.80L", tfr_80u = "WPP.TFR.80U",
      tfr_95l = "WPP.TFR.95L", tfr_95u = "WPP.TFR.95U",
      tfr_low = "WPP.TFR.LOW", tfr_high = "WPP.TFR.HIGH"
    )
  ),
  list(
    estimate_code = "percentASFR1dt", projection_code = NULL,
    output_name = "fertility_age_specific",
    description = "Age-specific fertility distribution",
    code_map = c(pasfr = "WPP.PASFR")
  ),
  list(
    estimate_code = "migration1dt", projection_code = "migproj1dt",
    output_name = "migration_net",
    description = "Net migration (totals + bounds)",
    code_map = c(
      mig = "WPP.MIG.NET",
      mig_80l = "WPP.MIG.NET.80L", mig_80u = "WPP.MIG.NET.80U",
      mig_95l = "WPP.MIG.NET.95L", mig_95u = "WPP.MIG.NET.95U"
    )
  ),
  list(
    estimate_code = NULL, projection_code = "migprojAge1dt",
    output_name = "migration_age_specific",
    description = "Net migration by age (median)",
    drop_measures = c("mig"),
    code_map = c(
      migM = "WPP.MIG.AGE.MALE",
      migF = "WPP.MIG.AGE.FEMALE"
    )
  ),
  list(
    estimate_code = "misc1dt", projection_code = "miscproj1dt",
    output_name = "demographic_indicators",
    description = "Births, deaths, crude rates, growth rates",
    code_map = c(
      births = "WPP.BIRTHS", deaths = "WPP.DEATHS",
      cbr = "WPP.CBR", cdr = "WPP.CDR", cnmr = "WPP.CNMR",
      growthrate = "WPP.POP.GROWTH",
      PopChange = "WPP.POP.CHANGE",
      NatChangeRT = "WPP.NAT.CHANGE"
    )
  ),
  list(
    estimate_code = "sexRatio1dt", projection_code = NULL,
    output_name = "sex_ratio_birth",
    description = "Sex ratio at birth (male/female)",
    code_map = c(srb = "WPP.SEX.RATIO.MF")
  )
)

# ==============================================================================
# MELT
# ==============================================================================
DROP_COLS <- c("name", "location", "notes", "source", "source_version",
               "variant")
OUT_COLS  <- c("country_code", "year", "indicator_code", "value",
               "age", "data_type")

melt_wpp <- function(dt, spec) {
  cc <- intersect(c("country_code", "wpp_country_code"), names(dt))
  if (length(cc) == 0) return(NULL)
  if (cc[1] != "country_code") setnames(dt, cc[1], "country_code")
  
  for (c in intersect(DROP_COLS, names(dt))) dt[, (c) := NULL]
  
  pass_cols <- intersect("age", names(dt))
  
  drop_cols <- intersect(spec$drop_measures %||% character(0), names(dt))
  if (length(drop_cols) > 0) for (c in drop_cols) dt[, (c) := NULL]
  
  value_cols <- setdiff(
    names(dt)[vapply(dt, is.numeric, logical(1))],
    c("country_code", "year", pass_cols)
  )
  if (length(value_cols) == 0) return(NULL)
  
  code_map <- spec$code_map %||% character(0)
  unmapped <- setdiff(value_cols, names(code_map))
  
  id_vars <- intersect(c("country_code", "year", "data_type", pass_cols),
                       names(dt))
  
  long <- melt(dt, id.vars = id_vars, measure.vars = value_cols,
               variable.name = "measure", value.name = "value",
               variable.factor = FALSE)
  long <- long[!is.na(value)]
  
  fallback <- toupper(spec$output_name)
  long[, indicator_code := ifelse(
    measure %in% names(code_map),
    code_map[measure],
    paste0(fallback, ".", toupper(measure))
  )]
  
  if (!"age" %in% names(long)) long[, age := NA_integer_]
  long[, age := as.integer(age)]
  
  if (!"data_type" %in% names(long)) long[, data_type := NA_character_]
  
  list(
    values   = long[, ..OUT_COLS],
    unmapped = unmapped
  )
}

# ==============================================================================
# EXTRACT
# ==============================================================================
cat("WPP 2024 extraction (Option A: age as first-class column)\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

parts    <- list()
labels   <- character(0)
skipped  <- character(0)
fallback <- list()

for (spec in indicators) {
  est  <- load_first(spec$estimate_code)
  proj <- load_first(spec$projection_code)
  if (!is.null(est))  est[,  data_type := "estimate"]
  if (!is.null(proj)) proj[, data_type := "projection"]
  
  if (is.null(est) && is.null(proj)) {
    cat(sprintf("%-26s  SKIPPED (no dataset)\n", spec$output_name))
    skipped <- c(skipped, spec$output_name)
    next
  }
  
  combined <- rbindlist(list(est, proj), use.names = TRUE, fill = TRUE)
  
  if (length(exclude_codes) > 0)
    combined <- combined[!country_code %in% exclude_codes]
  
  if ("variant" %in% names(combined))
    combined <- combined[is.na(variant) | tolower(variant) == "median"]
  
  res <- melt_wpp(combined, spec)
  if (is.null(res) || nrow(res$values) == 0) {
    cat(sprintf("%-26s  SKIPPED (melt empty)\n", spec$output_name))
    skipped <- c(skipped, spec$output_name)
    next
  }
  
  parts[[length(parts) + 1L]] <- res$values
  labels <- c(labels, spec$output_name)
  if (length(res$unmapped) > 0)
    fallback[[spec$output_name]] <- res$unmapped
  
  n_age <- sum(!is.na(res$values$age))
  
  cat(sprintf("%-26s  rows=%-10s codes=%-3d age_rows=%s\n",
              spec$output_name,
              format(nrow(res$values), big.mark = ","),
              uniqueN(res$values$indicator_code),
              format(n_age, big.mark = ",")))
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
  cat("Dedup: removed", before - nrow(dt), "duplicate rows\n")

setcolorder(dt, OUT_COLS)

pq <- file.path(output_dir, "wpp_all.parquet")
js <- file.path(output_dir, "wpp_all.json")

write_parquet(dt, pq)

yr        <- dt$year[!is.na(dt$year)]
n_age     <- sum(!is.na(dt$age))
age_range <- if (n_age > 0) range(dt$age, na.rm = TRUE) else c(NA_integer_, NA_integer_)

meta <- list(
  source          = "UN World Population Prospects 2024",
  source_url      = "https://population.un.org/wpp/",
  retrieved_at    = as.character(Sys.time()),
  parquet_file    = basename(pq),
  indicators      = labels,
  row_count       = nrow(dt),
  n_countries     = uniqueN(dt$country_code),
  n_indicators    = uniqueN(dt$indicator_code),
  indicator_codes = sort(unique(dt$indicator_code)),
  year_min        = if (length(yr)) min(yr) else NA_integer_,
  year_max        = if (length(yr)) max(yr) else NA_integer_,
  has_age         = n_age > 0,
  n_age_rows      = n_age,
  age_min         = age_range[1],
  age_max         = age_range[2],
  data_types      = sort(unique(dt$data_type)),
  excluded_codes  = exclude_codes[1]:exclude_codes[length(exclude_codes)],
  columns         = names(dt)
)
write_json(meta, js, pretty = TRUE, auto_unbox = TRUE)

# ==============================================================================
# SUMMARY
# ==============================================================================
cat("\nWPP 2024 extraction complete\n\n")
cat("  Parquet:    ", basename(pq), " (",
    round(file.size(pq) / 1024^2, 2), " MB)\n", sep = "")
cat("  Rows:       ", format(nrow(dt), big.mark = ","), "\n", sep = "")
cat("  Countries:  ", uniqueN(dt$country_code), "\n", sep = "")
cat("  Indicators: ", uniqueN(dt$indicator_code), "\n", sep = "")
cat("  Years:      ", min(yr), " - ", max(yr), "\n", sep = "")
if (n_age > 0) {
  cat("  Aged rows:  ", format(n_age, big.mark = ","),
      sprintf("  (ages %d-%d)", age_range[1], age_range[2]), "\n", sep = "")
}

if (length(fallback) > 0) {
  cat("\n  ! Fallback naming detected (add these columns to code_map):\n")
  for (nm in names(fallback)) {
    cat("      ", nm, ": ", paste(fallback[[nm]], collapse = ", "), "\n",
        sep = "")
  }
}
if (length(skipped) > 0)
  cat("\n  Skipped: ", paste(skipped, collapse = ", "), "\n", sep = "")
