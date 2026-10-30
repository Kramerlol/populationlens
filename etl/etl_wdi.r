# ==============================================================================
# WDI ETL Script - Production Version
# Source: World Bank World Development Indicators 
# Output: wdi_all.parquet + wdi_all.json
# Schema: "iso2c", "iso3c", "year", "indicator_code", "value"
# Last Updated: 29.09.2026
# Config: sourced from _common.R (path resolution via POPLENS_RAW env var)

#help("WDI")
#WDIsearch("milit")
# ==============================================================================

# Clean environment
rm(list = ls(all.names = TRUE))

# Load packages
library(WDI)
library(arrow)
library(dplyr)
library(jsonlite)
library(data.table)

# The World Bank API is occasionally slow to serve large indicators. The
# default R timeout (60 s) is too short for multi-page pulls like
# MS.MIL.XPND.CD, which is what caused the SE.TER.ENRR failure. 300 s gives
# the API breathing room without hanging indefinitely.
options(timeout = 300)   # 5 minutes instead of default 60s

# Configuration
# output_dir <- "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/data/raw_data/wdi"
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
source("_common.R") # Shared config for all PopLens ETL scripts
output_dir <- out_dir_for("wdi")

# Output file names
parquet_file <- file.path(output_dir, "wdi_all.parquet")
json_file    <- file.path(output_dir, "wdi_all.json")

# ==============================================================================
# DEFINE INDICATORS TO EXTRACT
# ==============================================================================
indicators <- list(
  # TIER 1: CORE DEMOGRAPHIC
  list(code = "SP.POP.TOTL",     name = "population_total",        description = "Total population based on de facto definition"),
  list(code = "SP.POP.1564.TO",  name = "population_working_age",  description = "Population ages 15-64 (working age)"),
  list(code = "SP.POP.65UP.TO",  name = "population_elderly",      description = "Population ages 65 and above"),
  list(code = "SP.POP.0014.TO",  name = "population_youth",        description = "Population ages 0-14"),
  list(code = "SP.DYN.LE00.IN",  name = "life_expectancy",         description = "Life expectancy at birth, total (years)"),
  list(code = "SP.POP.DPND",     name = "dependency_ratio_total",  description = "Age dependency ratio (% of working-age population)"),
  list(code = "SP.POP.DPND.OL",  name = "dependency_ratio_old",    description = "Old-age dependency ratio (% of working-age population)"),
  list(code = "SP.POP.DPND.YG",  name = "dependency_ratio_young",  description = "Youth dependency ratio (% of working-age population)"),
  
  # TIER 2: LABOR MARKET
  list(code = "SL.TLF.CACT.ZS",  name = "labor_force_participation", description = "Labor force participation rate, total (% of total population ages 15+)"),
  list(code = "SL.UEM.TOTL.ZS",  name = "unemployment_rate",         description = "Unemployment, total (% of total labor force)"),
  
  # TIER 2: HEALTH SYSTEM CAPACITY
  list(code = "SH.XPD.CHEX.GD.ZS", name = "health_expenditure_gdp",  description = "Current health expenditure (% of GDP)"),
  list(code = "SH.MED.PHYS.ZS",    name = "physicians_per_1000",     description = "Physicians (per 1,000 people)"),
  list(code = "SH.MED.NUMW.P3",    name = "nurses_per_1000",         description = "Nurses and midwives (per 1,000 people)"),
  list(code = "SH.STA.BRTC.ZS",    name = "births_skilled_staff",    description = "Births attended by skilled health staff (% of total)"),
  
  # TIER 2: ECONOMIC INDICATORS
  list(code = "NY.GDP.PCAP.CD",    name = "gdp_per_capita_current",  description = "GDP per capita (current US$)"),
  list(code = "NY.GDP.PCAP.KD",    name = "gdp_per_capita_constant", description = "GDP per capita (constant 2015 US$) - better for time series"),
  
  # TIER 3: POVERTY & INEQUALITY
  list(code = "SI.POV.DDAY",       name = "poverty_190",             description = "Poverty headcount ratio at $2.15/day (2017 PPP, % of population)"),
  list(code = "SI.POV.LMIC",       name = "poverty_320",             description = "Poverty headcount ratio at $3.65/day (2017 PPP, % of population)"),
  list(code = "SI.POV.UMIC",       name = "poverty_550",             description = "Poverty headcount ratio at $6.85/day (2017 PPP, % of population)"),
  list(code = "SI.POV.GINI",       name = "gini_index",              description = "Gini index (0 = perfect equality, 100 = perfect inequality)"),
  
  # TIER 3: EDUCATION
  list(code = "SE.PRM.ENRR",       name = "enrollment_primary",      description = "School enrollment, primary (% gross)"),
  list(code = "SE.SEC.ENRR",       name = "enrollment_secondary",    description = "School enrollment, secondary (% gross)"),
  list(code = "SE.TER.ENRR",       name = "enrollment_tertiary",     description = "School enrollment, tertiary (% gross)"),
  list(code = "SE.ADT.LITR.ZS",    name = "literacy_adult",          description = "Literacy rate, adult total (% of people ages 15 and above)"),
  
  # TIER 3: INFRASTRUCTURE & SERVICES
  list(code = "SH.STA.SMSS.ZS",    name = "sanitation_access",       description = "People using safely managed sanitation services (% of population)"),
  list(code = "EG.ELC.ACCS.ZS",    name = "electricity_access",      description = "Access to electricity (% of population)"),
  list(code = "IT.NET.USER.ZS",    name = "internet_users",          description = "Individuals using the Internet (% of population)"),
  
  # TIER 3: MILITARY EXPENDITURE (SIPRI-sourced via WDI redistribution)
  list(
    code        = "MS.MIL.XPND.CD",name        = "military_expenditure_current_usd",
    description = paste(
      "Total military expenditure in current US$. Source: SIPRI Military",
      "Expenditure Database, redistributed by World Bank WDI. Nominal;",
      "for real-terms trends use SIPRI constant-USD series (not available in WDI).",
      "Commercial use: verify SIPRI licensing if redistributing."
    )
  ),
  list(
    code        = "MS.MIL.XPND.GD.ZS",
    name        = "military_expenditure_pct_gdp",
    description = paste(
      "Military expenditure as a share of GDP (%).",
      "Source: SIPRI Military Expenditure Database, redistributed by World Bank WDI."
    )
  ),
  list(
    code        = "MS.MIL.XPND.ZS",
    name        = "military_expenditure_pct_govt",
    description = paste(
      "Military expenditure as a share of general government expenditure (%).",
      "Measures defense as a budget priority. Source: SIPRI Military",
      "Expenditure Database, redistributed by World Bank WDI. Shorter time",
      "coverage than current USD and % of GDP — typically from 1988 onward."
    )
  )
  # # TIER 3: PEACEKEEPING CONTRIBUTIONS (UN DPO, via WDI)
  # # Retired
  # list(
  #   code        = "VC.PKP.TOTL.UN",
  #   name        = "un_peacekeepers_contributed",
  #   description = paste(
  #     "Number of troops, police, and military observers contributed to UN",
  #     "peacekeeping missions. Source: UN Department of Peace Operations,",
  #     "redistributed by World Bank WDI. Distinct from expenditure — measures",
  #     "personnel commitment, not spending."
  #   )
  # )
  
)

# ==============================================================================
# VALIDATE INDICATOR CODES
# ==============================================================================
cat("Validating WDI indicator codes...\n")
all_indicators <- WDIsearch("", cache = NULL)
valid_codes <- all_indicators[, 1]

indicators_valid <- Filter(function(x) x$code %in% valid_codes, indicators)
indicators_invalid <- Filter(function(x) !(x$code %in% valid_codes), indicators)

cat("  Valid indicators:", length(indicators_valid), "\n")
if (length(indicators_invalid) > 0) {
  cat("  Invalid indicators:", length(indicators_invalid), "\n")
  for (ind in indicators_invalid) {
    cat("    -", ind$code, "(", ind$name, ")\n")
  }
}

# ------------------------------------------------------------------------------
# fetch_wdi(): RETRY WRAPPER AROUND WDI()
# ------------------------------------------------------------------------------
# The World Bank API occasionally times out on large indicators (a single
# indicator returns one row per country-year, ~17k rows, and the API fetches
# these in pages). A transient failure on one page means the entire call
# returns NULL. This wrapper retries up to `attempts` times with increasing
# delay between tries — 5 s, 10 s, 15 s — which is enough to outlast most
# server-side hiccups without hammering the API.
#
# Returns the data.frame on success, or NULL if every attempt fails.
fetch_wdi <- function(code, attempts = 3) {
  for (i in seq_len(attempts)) {
    d <- tryCatch(
      WDI(
        country   = "all",
        indicator = code,
        start     = 1960,
        end       = NULL,
        extra     = FALSE,
        language  = "en"
      ),
      error   = function(e) NULL,
      warning = function(w) NULL
    )
    
    if (!is.null(d) && nrow(d) > 0) return(d)
    
    if (i < attempts) {
      delay <- 5 * i
      cat("    retry", i, "of", attempts - 1,
          "- waiting", delay, "s...\n")
      Sys.sleep(delay)
    }
  }
  NULL
}

# ==============================================================================
# FETCH AND ACCUMULATE DATA
# ==============================================================================
# Pull each validated indicator from the World Bank API one at a time, trim
# the response down to the columns the fact table needs, and stack them into
# a single list. The loop is deliberately serial (not parallel) so the WB
# API sees a polite, throttled client — parallel requests tend to trigger
# rate-limiting.

cat("\nStarting WDI data extraction...\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

# Accumulator: one data.table per indicator, keyed by indicator code.
# We collect here and rbindlist() once at the end — much faster than growing
# a single data.frame row-by-row inside the loop.
data_list <- list()

for (ind in indicators_valid) {
  cat("\nProcessing:", ind$name, "(", ind$code, ")\n")
  
  # Fetch the indicator from the WB API with retries. `extra = FALSE` tells
  # the WB API to omit the region / income / lending / capital / lat-lon
  # columns — those belong in reference.dim_countries, not in the fact table,
  # and pulling them here only bloats the response.
  #
  # fetch_wdi() handles the retry-with-backoff logic. If it returns NULL
  # after all attempts, we treat the indicator as unavailable for this run
  # and move on; the loop doesn't fail, which lets the remaining 30+ pulls
  # complete successfully even if one indicator's server-side data is
  # temporarily unreachable.
  data <- fetch_wdi(ind$code, attempts = 3)
  
  # Guard: skip if the fetch failed outright (network, timeout, invalid
  # indicator code) or returned an empty frame (indicator retired by the
  # World Bank). Both cases are non-fatal here — missing indicators show up
  # in the run summary at the end.
  if (is.null(data) || nrow(data) == 0) {
    cat("  No data returned - skipping\n")
    next
  }
  
  # Convert tibble -> data.table so downstream column operations use
  # by-reference semantics (setnames, `:=`, `..` selection). Tibbles work
  # with dplyr syntax, but the rest of this script is data.table-flavored.
  dt <- as.data.table(data)
  
  # WDI() returns each indicator's value under a column named after the
  # indicator code itself (e.g. SP.POP.TOTL). Rename that column to `value`
  # so every indicator shares the same target schema and the resulting
  # Parquet is self-consistent. If the expected column is missing, something
  # is wrong with the response shape — skip rather than crash.
  if (ind$code %in% names(dt)) {
    setnames(dt, old = ind$code, new = "value")
  } else {
    cat("  Indicator column not found - skipping\n")
    next
  }
  
  # Enforce a canonical column set. `iso2c` and `iso3c` are kept as-is so
  # the ingester can join on either; `year` for dim_time; `value` for the
  # fact measurement. Anything else (country name, region metadata, etc.)
  # is dropped because it's redundant with the dimension tables.
  keep_cols <- c("iso2c", "iso3c", "year", "value")
  if (!all(keep_cols %in% names(dt))) {
    cat("  Missing expected columns - skipping\n")
    next
  }
  
  dt <- dt[, ..keep_cols]
  
  # Tag every row with its indicator code so the combined output can be
  # filtered/grouped by indicator downstream. The original wide-format
  # response loses this information once the code-named column is renamed.
  dt[, indicator_code := ind$code]
  
  # Store for the final rbindlist(). Keyed by indicator code so any
  # duplicate attempt (not expected here, but defensive) would overwrite
  # rather than append.
  data_list[[ind$code]] <- dt
  cat("  Retrieved:", nrow(dt), "rows\n")
  
  # Short pause between indicator requests. The WB API doesn't publish a
  # hard rate limit, but a 300 ms gap keeps us well below any throttling
  # threshold and avoids the "same WDI package version worked yesterday!"
  # class of failures that usually means the server is having a bad day.
  Sys.sleep(0.3)  # polite delay
}

# ==============================================================================
# COMBINE AND SAVE
# ==============================================================================
if (length(data_list) == 0) {
  cat("\nNo data fetched. Exiting.\n")
  quit(status = 1)
}

cat("\nCombining all indicators...\n")
all_data <- rbindlist(data_list, use.names = TRUE, fill = TRUE)
setcolorder(all_data, c("iso2c", "iso3c", "year", "indicator_code", "value"))

# Write Parquet
cat("Writing Parquet:", basename(parquet_file), "\n")
write_parquet(all_data, parquet_file)

# Create metadata JSON (one row per Parquet file)
metadata <- list(
  source              = "World Bank - World Development Indicators",
  source_url          = "https://data.worldbank.org/",
  retrieved_at        = as.character(Sys.time()),
  parquet_file        = basename(parquet_file),
  row_count           = nrow(all_data),
  n_indicators        = length(data_list),
  indicator_codes     = names(data_list),
  n_countries         = uniqueN(all_data$iso2c),
  year_range          = paste(range(all_data$year, na.rm = TRUE), collapse = " - "),
  non_missing_values  = sum(!is.na(all_data$value)),
  coverage_pct        = sprintf("%.1f%%", 100 * sum(!is.na(all_data$value)) / nrow(all_data)),
  columns             = names(all_data)
)

write_json(metadata, json_file, pretty = TRUE, auto_unbox = TRUE)

# ==============================================================================
# SUMMARY
# ==============================================================================
cat("\n", paste(rep("=", 70), collapse = ""), "\n")
cat("WDI extraction complete!\n\n")
cat("Total rows:", nrow(all_data), "\n")
cat("Indicators:", length(data_list), "\n")
cat("Countries:", uniqueN(all_data$iso2c), "\n")
cat("Year range:", paste(range(all_data$year, na.rm = TRUE), collapse = " - "), "\n")
cat("Parquet file size:", round(file.size(parquet_file) / 1024^2, 2), "MB\n")
cat("Metadata saved to:", basename(json_file), "\n")
