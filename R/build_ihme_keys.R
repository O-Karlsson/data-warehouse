# R/build_ihme_keys.R
#
# Build small IHME GBD key tables (locations, causes, REIs) from the IHME workbook.
# Run from the warehouse root:
#   source("R/build_ihme_keys.R")

VINTAGE <- "2025-10-23"

RAW_DIR <- file.path("data", "raw", "ihme_keys")
OUT_DIR <- file.path("data", "cleaned", "ihme_keys", VINTAGE)
dir.create(RAW_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

XLSX <- file.path(RAW_DIR, "IHME_GBD_2023_HIERARCHIES_Y2025M10D23_Fixed.XLSX")
if (!(file.exists(XLSX) && file.info(XLSX)$size > 0)) {
  stop(
    "Missing raw file:\n  ", XLSX, "\n\n",
    "Place the IHME hierarchies workbook in:\n  ", RAW_DIR, "\n",
    call. = FALSE
  )
}

# Read sheets
loc <- readxl::read_xlsx(XLSX, sheet = "All Location Hierarchies")
cau <- readxl::read_xlsx(XLSX, sheet = "Cause Hierarchy")
rei <- readxl::read_xlsx(XLSX, sheet = "REI Hierarchy")

# Clean column names (lowercase, underscores, unique; Stata-friendly)
clean_names <- function(nm) {
  nm <- tolower(nm)
  nm <- gsub("[^a-z0-9_]", "_", nm)
  nm <- gsub("^([0-9])", "_\\1", nm)
  make.unique(nm, sep = "_")
}
names(loc) <- clean_names(names(loc))
names(cau) <- clean_names(names(cau))
names(rei) <- clean_names(names(rei))

# Coerce key ID columns to integer (if present)
to_int <- function(df, cols) {
  for (cc in cols) if (cc %in% names(df)) df[[cc]] <- as.integer(df[[cc]])
  df
}
loc <- to_int(loc, c("location_set_version_id","location_id","parent_id","level","sort_order"))
cau <- to_int(cau, c("cause_id","parent_id","level","sort_order"))
rei <- to_int(rei, c("rei_id","parent_id","level","sort_order","rei_type_id"))

# Write outputs (parquet + dta + csv)
arrow::write_parquet(loc, file.path(OUT_DIR, "ihme_location_hierarchy.parquet"))
haven::write_dta(loc,     file.path(OUT_DIR, "ihme_location_hierarchy.dta"))
readr::write_csv(loc,     file.path(OUT_DIR, "ihme_location_hierarchy.csv"))

arrow::write_parquet(cau, file.path(OUT_DIR, "ihme_cause_hierarchy.parquet"))
haven::write_dta(cau,     file.path(OUT_DIR, "ihme_cause_hierarchy.dta"))
readr::write_csv(cau,     file.path(OUT_DIR, "ihme_cause_hierarchy.csv"))

arrow::write_parquet(rei, file.path(OUT_DIR, "ihme_rei_hierarchy.parquet"))
haven::write_dta(rei,     file.path(OUT_DIR, "ihme_rei_hierarchy.dta"))
readr::write_csv(rei,     file.path(OUT_DIR, "ihme_rei_hierarchy.csv"))

message("Done: wrote IHME key tables to ", OUT_DIR)
