
# Shared config for all PopLens ETL scripts
raw_root    <- Sys.getenv("POPLENS_RAW",
  unset = "C:/Users/krame/Documents/PopLens/PostgreSQL_Migration/data/raw_data")
log_root    <- Sys.getenv("POPLENS_LOG", unset = "/var/log/poplens")
out_dir_for <- function(src) {
  d <- file.path(raw_root, src)
  if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  d
}

# Then each script starts with source("_common.R"); output_dir <- out_dir_for("eurostat"). 
# One place to change paths, forever. But if you're in a hurry, the three-line inline fix is fine.
