# ==============================================================================
# SIPRI Military Expenditure ETL Script - Production Version
# Source: SIPRI Military Expenditure Database 
#   - 4 indicators via milRex::sipri_get_data()
#   - constantUSD via local CSV (milRex hardcodes a retired sheet name)
# Output: sipri_milex.parquet + sipri_milex.json
# Schema: country_code, country_name, year, indicator_code, value, data_type
# Last Updated: 28.09.2026
# Config: sourced from _common.R (path resolution via POPLENS_RAW env var)

# Diagnostics:
# install.packages(
#   "milRex",
#   repos = c(
#     "https://community.r-multiverse.org",
#     getOption("repos")  # keeps CRAN as fallback
#   )
# )
# ??milRex
# indicator must be one of "regionalTotals", "currencyFY", "currencyCY",
# "constantUSD", "currentUSD", "shareOfGDP", "perCapita", "shareGovt", or "all"
# All indicators
# milex_shareOfGDP <- sipri_get_data(indicator = "shareOfGDP")
# milex_currentUSD
# milex_shareOfGDP
# 
# milex_currencyCY <- sipri_get_data(indicator = "currencyCY")
# # Local currency financial years - do I even need this? What does it mean?
# # If I should, what are these values actually telling me?
# # Similar question for Local currency calendar years
# milex_currencyFY %>% filter(country=="United States of America")

# One thing to verify: 
# constantUSD has 168 countries; the other four cover 174 between them. 
# That's a 6-country gap in your Power Query pivot.
# setdiff(unique(milex_dt$country_code),
#         unique(csv_dt$country_code))

# ==============================================================================

# Clean environment
rm(list = ls(all.names = TRUE))
`%||%` <- function(a, b) if (is.null(a)) b else a

library(milRex)
library(arrow)
library(jsonlite)
library(data.table)
library(countrycode)

# Configuration:
# output_dir <- "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/data/raw_data/sipri"
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
source("_common.R") # Shared config for all PopLens ETL scripts
output_dir <- out_dir_for("sipri")

# milRex ships a hard-coded sheet name "Constant (2022) US$" that SIPRI's
# 2026 workbook no longer contains. Pull the current sheet manually, pivot
# in Power Query, export as CSV. On Hetzner, update this path.
csv_constantUSD <- paste0(
  "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/etl/",
  "SIPRI-Milex-data-1949-2025_Constant (2024) US$.csv"
)

# ==============================================================================
# INDICATORS
#   regionalTotals omitted — see docs. Re-add later if SIPRI's official
#   regional groupings become analytically relevant.
# ==============================================================================
indicators <- list(
  list(id = "currentUSD", code = "SIPRI.MILEX.CURRENT_USD",
       label = "Military expenditure, current US$ (millions)"),
  list(id = "shareOfGDP", code = "SIPRI.MILEX.SHARE_GDP",
       label = "Military expenditure, share of GDP (%)"),
  list(id = "shareGovt",  code = "SIPRI.MILEX.SHARE_GOVT",
       label = "Military expenditure, share of government spending (%)"),
  list(id = "perCapita",  code = "SIPRI.MILEX.PER_CAPITA_CURRENT_USD",
       label = "Military expenditure per capita, current US$")
)

csv_spec <- list(
  csv_path = csv_constantUSD,
  code     = "SIPRI.MILEX.CONSTANT_2024_USD",
  label    = "Military expenditure, constant 2024 US$ (millions)"
)

# ==============================================================================
# PASS 1 — milRex indicators
# ==============================================================================
cat("SIPRI Military Expenditure extraction\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

parts   <- list()
labels  <- character(0)
skipped <- character(0)

for (spec in indicators) {
  cat(sprintf("Fetching %-15s ... ", spec$id))
  d <- tryCatch(
    milRex::sipri_get_data(indicator = spec$id),
    error = function(e) { cat("FAILED: ", conditionMessage(e), "\n"); NULL }
  )
  if (is.null(d) || nrow(d) == 0) {
    cat("no rows\n"); skipped <- c(skipped, spec$id); next
  }
  dt <- as.data.table(d)
  
  required <- c("iso3c", "country", "year", "value")
  missing_cols <- setdiff(required, names(dt))
  if (length(missing_cols) > 0) {
    cat("missing columns:", paste(missing_cols, collapse = ", "), "\n")
    skipped <- c(skipped, spec$id); next
  }
  
  out <- dt[, .(
    country_code = iso3c,
    country_name = country,
    year         = as.integer(year),
    value        = as.numeric(value)
  )]
  out[, indicator_code := spec$code]

  before <- nrow(out)
  out <- out[!is.na(value) & !is.na(year) & !is.na(country_code)]
  cat(format(nrow(out), big.mark = ","), "rows",
      if (nrow(out) < before) sprintf(" (%d dropped)",
                                      before - nrow(out)) else "",
      "\n", sep = "")
  
  parts[[length(parts) + 1L]] <- out
  labels <- c(labels, spec$id)
}

if (length(parts) == 0) {
  cat("\nNo milRex indicators loaded. Exiting.\n"); quit(status = 1)
}

milex_dt <- rbindlist(parts, use.names = TRUE, fill = TRUE)

# ==============================================================================
# BUILD NAME → ISO3C LOOKUP FROM milRex (no manual table needed)
# ==============================================================================
name_lookup <- unique(
  milex_dt[!is.na(country_code) & !is.na(country_name),
           .(country_name, country_code)]
)
setnames(name_lookup, c("country_name", "iso3c"))
cat("\nName→ISO3C lookup built from milRex:",
    format(nrow(name_lookup), big.mark = ","), "entries\n")

# ==============================================================================
# PASS 2 — constantUSD from local CSV
# ==============================================================================
cat("\nReading constantUSD from CSV:\n  ", basename(csv_spec$csv_path),
    "\n", sep = "")

if (!file.exists(csv_spec$csv_path)) {
  cat("  ! CSV not found — constantUSD will be omitted\n")
  skipped <- c(skipped, "constantUSD")
} else {
  csv_dt <- fread(csv_spec$csv_path, check.names = FALSE)
  setnames(csv_dt, tolower(trimws(names(csv_dt))))
  
  # Accept common shorthands for the country column
  if (!"country_name" %in% names(csv_dt)) {
    alt <- intersect(c("country", "name"), names(csv_dt))
    if (length(alt) > 0) setnames(csv_dt, alt[1], "country_name")
    else stop("CSV is missing a country column")
  }
  if (!"year"  %in% names(csv_dt)) stop("CSV is missing a year column")
  if (!"value" %in% names(csv_dt)) stop("CSV is missing a value column")
  
  csv_dt[, country_name := trimws(as.character(country_name))]
  csv_dt[, year         := as.integer(year)]
  csv_dt[, value        := as.numeric(value)]
  csv_dt <- csv_dt[!is.na(value) & !is.na(year) & !is.na(country_name)]
  csv_dt[, country_code := NA_character_]
  
  # Join to milRex-derived lookup
  csv_dt[name_lookup, on = "country_name", country_code := i.iso3c]
  
  # Fallback: countrycode() for anything not seen in milRex
  unresolved <- csv_dt[is.na(country_code), unique(country_name)]
  if (length(unresolved) > 0) {
    fb_iso <- suppressWarnings(
      countrycode(unresolved, origin = "country.name", destination = "iso3c")
    )
    fb <- data.table(country_name = unresolved, iso3c = fb_iso)
    csv_dt[fb, on = "country_name", country_code := i.iso3c]
    
    still_bad <- fb[is.na(iso3c), country_name]
    if (length(still_bad) > 0) {
      cat("  ! Unresolved country names (rows will be dropped):\n")
      for (nm in still_bad) cat("      -", nm, "\n")
    }
  }
  
  csv_dt <- csv_dt[!is.na(country_code)]
  csv_dt[, indicator_code := csv_spec$code]
  csv_dt <- csv_dt[, .(country_code, country_name, year,
                       indicator_code, value)]
  
  cat("  rows=", format(nrow(csv_dt), big.mark = ","),
      "  countries=", uniqueN(csv_dt$country_code),
      "  years=", min(csv_dt$year), "-", max(csv_dt$year), "\n", sep = "")
  
  parts[[length(parts) + 1L]] <- csv_dt
  labels <- c(labels, "constantUSD")
}

# ==============================================================================
# COMBINE + WRITE
# ==============================================================================
cat("\n", paste(rep("=", 70), collapse = ""), "\n")

dt <- rbindlist(parts, use.names = TRUE, fill = TRUE)

before <- nrow(dt)
dt <- unique(dt)
if (nrow(dt) < before) cat("Dedup: removed", before - nrow(dt), "rows\n")

setcolorder(dt, c("country_code", "country_name", "year",
                  "indicator_code", "value"))

pq <- file.path(output_dir, "sipri_milex.parquet")
js <- file.path(output_dir, "sipri_milex.json")

coverage_by_code <- dt[, .(n_countries = uniqueN(country_code)),
                       by = indicator_code][order(n_countries)]
cat("\nCountry coverage by indicator:\n")
print(coverage_by_code)

write_parquet(dt, pq)

n_units <- nrow(dt)

meta <- list(
  source             = "Stockholm International Peace Research Institute (SIPRI)",
  source_url         = "https://www.sipri.org/databases/milex",
  doi                = "10.55163/CQGC9685",
  citation           = "SIPRI Military Expenditure Database 2026, https://www.sipri.org/databases/milex",
  dataset_revision   = "2026-04-27T19:00:00+01:00",
  retrieved_at       = as.character(Sys.time()),
  parquet_file       = basename(pq),
  indicators         = labels,
  indicator_labels   = setNames(
    c(vapply(indicators, `[[`, character(1), "label"), csv_spec$label),
    c(vapply(indicators, `[[`, character(1), "code"),  csv_spec$code)
  ),
  row_count          = nrow(dt),
  n_countries        = uniqueN(dt$country_code),
  n_indicators       = uniqueN(dt$indicator_code),
  indicator_codes    = sort(unique(dt$indicator_code)),
  year_min           = min(dt$year, na.rm = TRUE),
  year_max           = max(dt$year, na.rm = TRUE),
  n_country_units    = n_units,
  sipri_10pct_units  = 3500,
  sipri_units_status = if (n_units > 3500) "above_threshold" else "below_threshold",
  csv_sources        = setNames(csv_spec$csv_path, csv_spec$code),
  skipped            = if (length(skipped) > 0) skipped else NULL,
  license_note       = paste(
    "Fair use for non-commercial research with attribution, and only if",
    "less than 10% of the dataset is reproduced (SIPRI terms).",
    "Commercial use requires prior written permission and typically a royalty."
  ),
  columns            = names(dt)
)
write_json(meta, js, pretty = TRUE, auto_unbox = TRUE)

# ==============================================================================
# SUMMARY
# ==============================================================================
cat("\nSIPRI extraction complete\n\n")
cat("  Parquet:      ", basename(pq), " (",
    round(file.size(pq) / 1024^2, 2), " MB)\n", sep = "")
cat("  Rows:         ", format(nrow(dt), big.mark = ","), "\n", sep = "")
cat("  Countries:    ", uniqueN(dt$country_code), "\n", sep = "")
cat("  Indicators:   ", uniqueN(dt$indicator_code), "\n", sep = "")
cat("  Years:        ", min(dt$year), " - ", max(dt$year), "\n", sep = "")
cat("\n  SIPRI 10% rule: ",
    format(n_units, big.mark = ","), " / 3,500 units -> ",
    toupper(meta$sipri_units_status), "\n", sep = "")

if (length(skipped) > 0)
  cat("\n  Skipped: ", paste(skipped, collapse = ", "), "\n", sep = "")


# Script below is efficient if/when the hard-coded sheet name is fixed
# # ==============================================================================
# # INDICATORS
# #   currencyFY and currencyCY are intentionally omitted (see docs).
# #   All indicators share the same shape: sheet, country, iso3c, year, value,
# #   missing — so no melt step is needed.
# # ==============================================================================
# indicators <- list(
#   list(id = "constantUSD", code = "SIPRI.MILEX.CONSTANT_2024_USD",
#        label = "Military expenditure, constant 2024 US$ (millions)",
#        group = "country"),
#   list(id = "currentUSD",  code = "SIPRI.MILEX.CURRENT_USD",
#        label = "Military expenditure, current US$ (millions)",
#        group = "country"),
#   list(id = "shareOfGDP",  code = "SIPRI.MILEX.SHARE_GDP",
#        label = "Military expenditure, share of GDP (%)",
#        group = "country"),
#   list(id = "shareGovt",   code = "SIPRI.MILEX.SHARE_GOVT",
#        label = "Military expenditure, share of government spending (%)",
#        group = "country"),
#   list(id = "perCapita",   code = "SIPRI.MILEX.PER_CAPITA_CURRENT_USD",
#        label = "Military expenditure per capita, current US$",
#        group = "country"),
#   list(id = "regionalTotals", code = "SIPRI.MILEX.REGIONAL",
#        label = "Regional & world totals (constant + current US$ billions)",
#        group = "region")
# )
# 
# # ==============================================================================
# # FETCHERS
# # ==============================================================================
# # milRex hardcodes the sheet name "Constant (2022) US$"; SIPRI now ships
# # "Constant (2024) US$". This fallback reads the correct sheet directly.
# fetch_constant_usd <- function() {
#   # (a) Try direct XLSX read. Confirm current filename at
#   #     https://www.sipri.org/databases/milex before relying on this.
#   url <- paste0(
#     "https://www.sipri.org/sites/default/files/",
#     "SIPRI-Milex-data-1949-2025_v1.2.xlsx"
#   )
#   dt <- tryCatch({
#     tmp <- tempfile(fileext = ".xlsx")
#     utils::download.file(url, tmp, mode = "wb", quiet = TRUE)
#     # Sheet layout: parse with readxl and reshape as needed. Stubbed for now.
#     NULL
#   }, error = function(e) NULL)
#   dt
# }
# 
# fetch_indicator <- function(id) {
#   if (id == "constantUSD") {
#     # Try the direct read first, then fall back to the broken package call
#     dt <- fetch_constant_usd()
#     if (!is.null(dt)) return(dt)
#     cat("  constantUSD: milRex sheet-name bug; direct read unavailable\n")
#     return(NULL)
#   }
#   tryCatch(milRex::sipri_get_data(indicator = id),
#            error = function(e) {
#              cat("  ", id, ": ", conditionMessage(e), "\n", sep = "")
#              NULL
#            })
# }
# 
# # ==============================================================================
# # EXTRACT
# # ==============================================================================
# cat("SIPRI Military Expenditure extraction\n")
# cat(paste(rep("=", 70), collapse = ""), "\n")
# 
# parts    <- list()
# labels   <- character(0)
# skipped  <- character(0)
# 
# for (spec in indicators) {
#   cat(sprintf("Fetching %-15s ... ", spec$id))
#   d <- fetch_indicator(spec$id)
#   if (is.null(d) || nrow(d) == 0) {
#     cat("no rows\n"); skipped <- c(skipped, spec$id); next
#   }
#   
#   dt <- as.data.table(d)
#   
#   # All indicators ship with: iso3c, country, year, value
#   required <- c("iso3c", "country", "year", "value")
#   missing_cols <- setdiff(required, names(dt))
#   if (length(missing_cols) > 0) {
#     cat("missing columns:", paste(missing_cols, collapse = ", "), "\n")
#     skipped <- c(skipped, spec$id); next
#   }
#   
#   out <- dt[, .(
#     country_code = iso3c,
#     country_name = country,
#     year         = as.integer(year),
#     value        = as.numeric(value)
#   )]
#   out[, indicator_code := spec$code]
#   out[, data_type := if (spec$group == "region") "regional_total" else "country"]
#   
#   # Drop NA values — `missing` column is dropped with them
#   before <- nrow(out)
#   out <- out[!is.na(value) & !is.na(year)]
#   cat(format(nrow(out), big.mark = ","), "rows",
#       if (nrow(out) < before) sprintf(" (%d NA dropped)", before - nrow(out)) else "",
#       "\n", sep = "")
#   
#   parts[[length(parts) + 1L]] <- out
#   labels <- c(labels, spec$id)
# }
# 
# if (length(parts) == 0) {
#   cat("\nNo data extracted.\n"); quit(status = 1)
# }
# 
# # ==============================================================================
# # COMBINE + WRITE
# # ==============================================================================
# cat("\n", paste(rep("=", 70), collapse = ""), "\n")
# 
# dt <- rbindlist(parts, use.names = TRUE, fill = TRUE)
# 
# # For regional totals the iso3c may be NA — derive a code from the name so
# # queries have a stable key.
# dt[data_type == "regional_total" & is.na(country_code),
#    country_code := gsub("[^A-Z0-9]+", "_", toupper(country_name))]
# 
# before <- nrow(dt)
# dt <- unique(dt)
# if (nrow(dt) < before) cat("Dedup: removed", before - nrow(dt), "rows\n")
# 
# setcolorder(dt, c("country_code", "country_name", "year",
#                   "indicator_code", "value", "data_type"))
# 
# pq <- file.path(output_dir, "sipri_milex.parquet")
# js <- file.path(output_dir, "sipri_milex.json")
# write_parquet(dt, pq)
# 
# n_units <- dt[data_type == "country", .N]
# 
# meta <- list(
#   source             = "Stockholm International Peace Research Institute (SIPRI)",
#   source_url         = "https://www.sipri.org/databases/milex",
#   doi                = "10.55163/CQGC9685",
#   citation           = "SIPRI Military Expenditure Database 2026, https://www.sipri.org/databases/milex",
#   dataset_revision   = "2026-04-27T19:00:00+01:00",
#   retrieved_at       = as.character(Sys.time()),
#   parquet_file       = basename(pq),
#   indicators         = labels,
#   indicator_labels   = setNames(
#     vapply(indicators, `[[`, character(1), "label"),
#     vapply(indicators, `[[`, character(1), "code")
#   ),
#   row_count          = nrow(dt),
#   n_countries        = dt[data_type == "country",
#                           uniqueN(country_code, na.rm = TRUE)],
#   n_regions          = dt[data_type == "regional_total",
#                           uniqueN(country_code)],
#   n_indicators       = uniqueN(dt$indicator_code),
#   indicator_codes    = sort(unique(dt$indicator_code)),
#   year_min           = min(dt$year, na.rm = TRUE),
#   year_max           = max(dt$year, na.rm = TRUE),
#   data_types         = sort(unique(dt$data_type)),
#   n_country_units    = n_units,
#   sipri_10pct_units  = 3500,
#   sipri_units_status = if (n_units > 3500) "above_threshold" else "below_threshold",
#   partial_failure    = if ("constantUSD" %in% skipped) "constantUSD" else NA_character_,
#   license_note       = paste(
#     "Fair use for non-commercial research with attribution, and only if",
#     "less than 10% of the dataset is reproduced (SIPRI terms).",
#     "Commercial use requires prior written permission and typically a royalty."
#   ),
#   columns            = names(dt)
# )
# write_json(meta, js, pretty = TRUE, auto_unbox = TRUE)
# 
# # ==============================================================================
# # SUMMARY
# # ==============================================================================
# cat("\nSIPRI extraction complete\n\n")
# cat("  Parquet:     ", basename(pq), " (",
#     round(file.size(pq) / 1024^2, 2), " MB)\n", sep = "")
# cat("  Rows:        ", format(nrow(dt), big.mark = ","), "\n", sep = "")
# cat("  Countries:   ", meta$n_countries, "\n", sep = "")
# cat("  Regions:     ", meta$n_regions, "\n", sep = "")
# cat("  Indicators:  ", meta$n_indicators, "\n", sep = "")
# cat("  Years:       ", meta$year_min, " - ", meta$year_max, "\n", sep = "")
# cat("\n  SIPRI 10% rule: ",
#     format(n_units, big.mark = ","), " / 3,500 units -> ",
#     toupper(meta$sipri_units_status), "\n", sep = "")
# 
# if (length(skipped) > 0)
#   cat("\n  Skipped: ", paste(skipped, collapse = ", "), "\n", sep = "")
# if (!is.na(meta$partial_failure))
#   cat("  ! Partial failure:", meta$partial_failure,
#       "- see notes in sipri_milex.json\n", sep = "")


# Kept in case I want to try to pull in notes later - VERY MESSY
# rm(list = ls(all.names = TRUE))
# `%||%` <- function(a, b) if (is.null(a)) b else a
# 
# library(milRex)
# library(arrow)
# library(jsonlite)
# library(data.table)
# library(countrycode)
# 
# output_dir <- "C:/Users/krame/Documents/PopLens/raw_data/sipri"
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
# 
# # ==============================================================================
# # INDICATOR MAP
# #   id    = argument passed to sipri_get_data()
# #   code  = indicator_code written to Parquet  (base; suffixes for multi-col)
# #   group = "country" | "region" (routes data_type downstream)
# # ==============================================================================
# indicators <- list(
#   list(id = "constantUSD", code = "SIPRI.MILEX.CONSTANT_2024_USD",
#        label = "Military expenditure, constant 2024 US$ (millions)",
#        group = "country"),
#   list(id = "currentUSD",  code = "SIPRI.MILEX.CURRENT_USD",
#        label = "Military expenditure, current US$ (millions)",
#        group = "country"),
#   list(id = "shareOfGDP",  code = "SIPRI.MILEX.SHARE_GDP",
#        label = "Military expenditure, share of GDP (%)",
#        group = "country"),
#   list(id = "shareGovt",   code = "SIPRI.MILEX.SHARE_GOVT",
#        label = "Military expenditure, share of government spending (%)",
#        group = "country"),
#   list(id = "perCapita",   code = "SIPRI.MILEX.PER_CAPITA_CURRENT_USD",
#        label = "Military expenditure per capita, current US$",
#        group = "country"),
#   list(id = "currencyFY",  code = "SIPRI.MILEX.LOCAL_CURRENCY_FY",
#        label = "Military expenditure, local currency, financial year",
#        group = "country"),
#   list(id = "currencyCY",  code = "SIPRI.MILEX.LOCAL_CURRENCY_CY",
#        label = "Military expenditure, local currency, calendar year",
#        group = "country"),
#   list(id = "regionalTotals", code = "SIPRI.MILEX.REGIONAL",
#        label = "Regional & world totals (constant + current US$ billions)",
#        group = "region")
# )
# 
# # ==============================================================================
# # COUNTRY-NAME MAPPING
# # ==============================================================================
# # Manual overrides for names countrycode() misses or mismaps in SIPRI.
# name_overrides <- c(
#   "United States of America"       = "USA",
#   "Iran"                           = "IRN",
#   "Iran (Islamic Republic of)"     = "IRN",
#   "South Korea"                    = "KOR",
#   "Korea, Republic of"             = "KOR",
#   "North Korea"                    = "PRK",
#   "Korea, Dem. People's Rep."      = "PRK",
#   "Vietnam"                        = "VNM",
#   "Viet Nam"                       = "VNM",
#   "Laos"                           = "LAO",
#   "Syria"                          = "SYR",
#   "Syrian Arab Republic"           = "SYR",
#   "Moldova"                        = "MDA",
#   "Republic of Moldova"            = "MDA",
#   "Bolivia"                        = "BOL",
#   "Tanzania"                       = "TZA",
#   "United Republic of Tanzania"    = "TZA",
#   "Venezuela"                      = "VEN",
#   "Taiwan"                         = "TWN",
#   "Czech Republic"                 = "CZE",
#   "Czechia"                        = "CZE",
#   "Turkey"                         = "TUR",
#   "Türkiye"                        = "TUR",
#   "Ivory Coast"                    = "CIV",
#   "Côte d'Ivoire"                  = "CIV",
#   "Russia"                         = "RUS",
#   "Russian Federation"             = "RUS",
#   "Egypt"                          = "EGY",
#   "Swaziland"                      = "SWZ",
#   "Eswatini"                       = "SWZ",
#   "Cape Verde"                     = "CPV",
#   "Cabo Verde"                     = "CPV",
#   "East Timor"                     = "TLS",
#   "Timor-Leste"                    = "TLS",
#   "Congo, Dem. Rep."               = "COD",
#   "Democratic Republic of the Congo" = "COD",
#   "Congo, Rep."                    = "COG",
#   "Republic of the Congo"          = "COG",
#   "Burma"                          = "MMR",
#   "Myanmar"                        = "MMR",
#   "Yugoslavia (former)"            = NA_character_,   # no modern ISO3
#   "Yugoslavia, SFR"                = NA_character_,
#   "USSR (former)"                  = NA_character_,
#   "Czechoslovakia (former)"        = NA_character_
# )
# 
# name_to_iso3 <- function(x) {
#   iso <- countrycode(
#     x, origin = "country.name", destination = "iso3c", warn = FALSE
#   )
#   hit <- is.na(iso) & x %in% names(name_overrides)
#   if (any(hit)) iso[hit] <- unname(name_overrides[x[hit]])
#   iso
# }
# 
# region_to_code <- function(x) gsub("[^A-Z0-9]+", "_", toupper(x))
# 
# # ==============================================================================
# # EXTRACT
# # ==============================================================================
# cat("SIPRI Military Expenditure extraction\n")
# cat(paste(rep("=", 70), collapse = ""), "\n")
# 
# parts   <- list()
# labels  <- character(0)
# skipped <- character(0)
# 
# for (spec in indicators) {
#   cat(sprintf("Fetching %-15s ... ", spec$id))
#   d <- tryCatch(
#     milRex::sipri_get_data(indicator = spec$id),
#     error = function(e) {
#       cat("FAILED:", conditionMessage(e), "\n")
#       NULL
#     }
#   )
#   if (is.null(d) || nrow(d) == 0) {
#     cat("no rows\n"); skipped <- c(skipped, spec$id); next
#   }
#   dt <- as.data.table(d)
#   cat(format(nrow(dt), big.mark = ","), "rows\n")
#   
#   nms <- names(dt)
#   
#   country_col <- intersect(c("country", "Country", "country_name",
#                              "name", "region", "Region"), nms)[1]
#   year_col    <- intersect(c("year", "Year"), nms)[1]
#   if (is.na(country_col) || is.na(year_col)) {
#     cat("  missing country/year column - skipping\n")
#     skipped <- c(skipped, spec$id); next
#   }
#   
#   footnote_cols <- grep("footnote|^note", nms, ignore.case = TRUE, value = TRUE)
#   value_cols <- setdiff(
#     nms[vapply(dt, is.numeric, logical(1))],
#     c(country_col, year_col)
#   )
#   if (length(value_cols) == 0) {
#     cat("  no numeric value columns - skipping\n")
#     skipped <- c(skipped, spec$id); next
#   }
#   
#   setnames(dt, country_col, "country_name", skip_absent = TRUE)
#   setnames(dt, year_col,    "year",         skip_absent = TRUE)
#   dt[, year := as.integer(year)]
#   dt <- dt[!is.na(year)]
#   
#   fn_col  <- if (length(footnote_cols) > 0) footnote_cols[1] else NULL
#   id_cols <- intersect(c("country_name", "year", fn_col), names(dt))
#   
#   long <- melt(
#     dt, id.vars = id_cols, measure.vars = value_cols,
#     variable.name = "measure", value.name = "value",
#     variable.factor = FALSE
#   )
#   long[, value := as.numeric(value)]
#   long <- long[!is.na(value)]
#   
#   # indicator_code — one per value column, suffixed when multiple
#   if (length(value_cols) == 1) {
#     long[, indicator_code := spec$code]
#   } else {
#     long[, indicator_code := paste0(spec$code, ".", toupper(measure))]
#   }
#   
#   if (spec$group == "region") {
#     long[, country_code := region_to_code(country_name)]
#     long[, data_type    := "regional_total"]
#   } else {
#     long[, country_code := name_to_iso3(country_name)]
#     long[, data_type    := "country"]
#   }
#   
#   if (!is.null(fn_col) && fn_col %in% names(long)) {
#     setnames(long, fn_col, "footnote", skip_absent = TRUE)
#   } else {
#     long[, footnote := NA_character_]
#   }
#   
#   keep <- c("country_code", "country_name", "year", "indicator_code",
#             "value", "footnote", "data_type")
#   for (c in setdiff(keep, names(long))) long[, (c) := NA]
#   long <- long[, ..keep]
#   
#   parts[[length(parts) + 1L]] <- long
#   labels <- c(labels, spec$id)
# }
# 
# if (length(parts) == 0) {
#   cat("\nNo data extracted.\n"); quit(status = 1)
# }
# 
# # ==============================================================================
# # COMBINE + WRITE
# # ==============================================================================
# cat("\n", paste(rep("=", 70), collapse = ""), "\n")
# 
# dt <- rbindlist(parts, use.names = TRUE, fill = TRUE)
# 
# # Report unresolved country names (informational, not fatal)
# unresolved <- dt[data_type == "country" & is.na(country_code),
#                  sort(unique(country_name))]
# if (length(unresolved) > 0) {
#   cat("Unresolved country names (", length(unresolved), "):\n", sep = "")
#   for (nm in unresolved) cat("    -", nm, "\n")
# }
# 
# dt[, footnote := ifelse(is.na(footnote) | footnote == "", NA_character_,
#                         footnote)]
# 
# before <- nrow(dt)
# dt <- unique(dt)
# if (nrow(dt) < before) cat("Dedup: removed", before - nrow(dt), "rows\n")
# 
# setcolorder(dt, c("country_code", "country_name", "year", "indicator_code",
#                   "value", "footnote", "data_type"))
# 
# pq <- file.path(output_dir, "sipri_milex.parquet")
# js <- file.path(output_dir, "sipri_milex.json")
# write_parquet(dt, pq)
# 
# n_units <- dt[data_type == "country", .N]
# 
# meta <- list(
#   source              = "Stockholm International Peace Research Institute (SIPRI)",
#   source_url          = "https://www.sipri.org/databases/milex",
#   doi                 = "10.55163/CQGC9685",
#   citation            = "SIPRI Military Expenditure Database 2026, https://www.sipri.org/databases/milex",
#   license_note        = paste(
#     "Fair use for non-commercial research with attribution, and only if",
#     "less than 10% of the dataset is reproduced (see SIPRI terms).",
#     "Commercial use requires prior written permission and typically a royalty."
#   ),
#   dataset_revision    = "2026-04-27T19:00:00+01:00",
#   retrieved_at        = as.character(Sys.time()),
#   parquet_file        = basename(pq),
#   indicators          = labels,
#   indicator_labels    = setNames(
#     vapply(indicators, `[[`, character(1), "label"),
#     vapply(indicators, `[[`, character(1), "code")
#   ),
#   row_count           = nrow(dt),
#   n_countries         = dt[data_type == "country", uniqueN(country_code, na.rm = TRUE)],
#   n_regions           = dt[data_type == "regional_total", uniqueN(country_code)],
#   n_indicators        = uniqueN(dt$indicator_code),
#   indicator_codes     = sort(unique(dt$indicator_code)),
#   year_min            = min(dt$year, na.rm = TRUE),
#   year_max            = max(dt$year, na.rm = TRUE),
#   data_types          = sort(unique(dt$data_type)),
#   n_footnoted         = sum(!is.na(dt$footnote)),
#   n_country_units     = n_units,
#   sipri_10pct_units   = 3500,
#   sipri_units_status  = if (n_units > 3500) "above_threshold" else "below_threshold",
#   columns             = names(dt)
# )
# write_json(meta, js, pretty = TRUE, auto_unbox = TRUE)
# 
# # ==============================================================================
# # SUMMARY
# # ==============================================================================
# cat("\nSIPRI Military Expenditure extraction complete\n\n")
# cat("  Parquet:      ", basename(pq), " (",
#     round(file.size(pq) / 1024^2, 2), " MB)\n", sep = "")
# cat("  Rows:         ", format(nrow(dt), big.mark = ","), "\n", sep = "")
# cat("  Countries:    ", meta$n_countries, "\n", sep = "")
# cat("  Regions:      ", meta$n_regions, "\n", sep = "")
# cat("  Indicators:   ", meta$n_indicators, "\n", sep = "")
# cat("  Years:        ", meta$year_min, " - ", meta$year_max, "\n", sep = "")
# cat("  Footnoted:    ", format(meta$n_footnoted, big.mark = ","), "\n", sep = "")
# cat("\n  SIPRI 10% rule check:\n")
# cat("    Threshold: 3,500 units (country-year-value)\n")
# cat("    This extract: ", format(n_units, big.mark = ","), " units\n", sep = "")
# cat("    Status: ", toupper(meta$sipri_units_status), "\n", sep = "")
# if (meta$sipri_units_status == "above_threshold") {
#   cat("    -> Commercial use requires written permission from SIPRI.\n")
# }
# 
# if (length(skipped) > 0)
#   cat("\n  Skipped: ", paste(skipped, collapse = ", "), "\n", sep = "")

