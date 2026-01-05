# R/build_ihme_under15_qx_subregions_japan_usa_uk_italy.R
#
# Cleans IHME GBD Results export (Probability of death, all-cause, under 15)
# Includes overlapping age intervals
# from a manually downloaded ZIP (login required).

source("R/utils.R")

# ---- Paths (edit ZIP_PATH if needed) ----
ZIP_PATH <- "data/raw/GBD/Probability of death all cause under 15 subregions and overall Japan USA UK Italy/IHME-GBD_2023_DATA-1f385c0a-1.zip"

OUT_DIR <- file.path("data", "cleaned", "GBD",
  "Probability of death all cause under 15 subregions and overall Japan USA UK Italy")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

if (!(file.exists(ZIP_PATH) && file.info(ZIP_PATH)$size > 0)) {
  stop(
    "Missing raw ZIP:\n  ", ZIP_PATH, "\n\n",
    "Download manually from IHME Viz Hub (login required) and place it at this path.",
    call. = FALSE
  )
}

# ---- Find the CSV inside the ZIP ----
zlist <- utils::unzip(ZIP_PATH, list = TRUE)
csvs  <- zlist$Name[grepl("\\.csv$", zlist$Name, ignore.case = TRUE)]

if (length(csvs) != 1) {
  stop(
    "Expected exactly 1 CSV inside the ZIP, found ", length(csvs), ":\n  ",
    paste(csvs, collapse = "\n  "),
    call. = FALSE
  )
}

# ---- Read CSV directly from ZIP (no extraction needed) ----
# readr can read from connections; unz() provides a connection to a file inside a ZIP.
con <- unz(ZIP_PATH, csvs[[1]], open = "rb")
on.exit(try(close(con), silent = TRUE), add = TRUE)

df <- readr::read_csv(con, show_col_types = FALSE)

# ---- Keep + rename columns ----
keep <- c("location_id", "location_name", "sex_id", "age_name", "year", "val", "upper", "lower")
missing <- setdiff(keep, names(df))
if (length(missing) > 0) {
  stop("Missing expected columns in CSV:\n  ", paste(missing, collapse = "\n  "), call. = FALSE)
}

out <- df[, keep]
names(out)[names(out) == "sex_id"] <- "sex"
names(out)[names(out) == "val"]    <- "qx"
names(out)[names(out) == "upper"]  <- "qx_u"
names(out)[names(out) == "lower"]  <- "qx_l"

# ensure integer typing where appropriate
out$location_id <- as.integer(out$location_id)
out$sex         <- as.integer(out$sex)
out$year        <- as.integer(out$year)

# ------------------------------------------------------------------------------
# 3) Write outputs (parquet + dta + csv)
# ------------------------------------------------------------------------------

# *****************************************************************************
# OLD simple write (no archiving): uncomment to use (and remove code blocks below)
# arrow::write_parquet(out, file.path(OUT_DIR, "data.parquet"))
# haven::write_dta(out,     file.path(OUT_DIR, "data.dta"))
# readr::write_csv(out,     file.path(OUT_DIR, "data.csv"))
# *****************************************************************************

# *****************************************************************************
# Build script output + archiving pattern

# Note: only checks if parquet changed, since dta may have  
#       metadata differences even if data is identical.
#       Makes use of utility function archive_and_promote() from R/utils.R

# Archive folder (for old versions, if files have changed)
arch_dir <- file.path(OUT_DIR, "_archive")

# Exisiting data file (for comparison)
out_parquet_final <- file.path(OUT_DIR, paste0("data", ".parquet"))

# 1) Write the NEW parquet to a temp file first (never overwrite directly)
out_parquet_tmp <- tempfile(fileext = ".parquet")
arrow::write_parquet(out, out_parquet_tmp)

# 2) Archive-and-promote:
#    - If parquet exists and differs from the new one, copy old to archive
#    - Then move tmp -> final
res <- archive_and_promote(
  final_path  = out_parquet_final,
  tmp_path    = out_parquet_tmp,
  archive_dir = arch_dir
)

# 3) If parquet changed (res.changed==TRUE) archive old .dta and .csv too
if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  
  file.copy(file.path(OUT_DIR, "data.dta"),
            file.path(arch_dir, paste0("data_", tag, ".dta")),
            overwrite = FALSE)
  
  file.copy(file.path(OUT_DIR, "data.csv"),
            file.path(arch_dir, paste0("data_", tag, ".csv")),
            overwrite = FALSE)
}

# then overwrite exports
haven::write_dta(out, file.path(OUT_DIR, "data.dta"))
readr::write_csv(out, file.path(OUT_DIR, "data.csv"))
# *****************************************************************************

# ------------------------------------------------------------
# Dataset-level notes (general documentation)
# ------------------------------------------------------------
# These notes describe scope, sources, and caveats for the
# dataset as a whole (not individual variables).
# Edit the content below as needed.
# ------------------------------------------------------------

dataset_notes <- c(
  "This dataset contains all cause mortality probabilities for Japan, USA, UK 
  and Italy at subnational levels for children under age 15.",
  "Data were  manually obtained from the Institute for Health Metrics and 
  Evaluation (IHME) Global Burden of Disease (GBD) Results tool, version GBD 2023
  https://vizhub.healthdata.org/gbd-results/ (requires login).",
  "Age interval lengths vary and some overlap.",
  "Includes decades 1950-2010 and 2019-2023 (all years are available online)."
  )

notes_path <- file.path(OUT_DIR, "DATASET_NOTES.md")

notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste("- ", dataset_notes)
)

# Write notes file
writeLines(notes_lines, notes_path)
message("Wrote dataset notes: ", notes_path)

# ------------------------------------------------------------
# ------------------------------------------------------------
# ------------------------------------------------------------
# Variable dictionary (edit var_label / notes here)
# ------------------------------------------------------------
# ------------------------------------------------------------

cat(paste(names(out), collapse = "\n"), "\n")

var_dict <- data.frame(
  var = c("location_id", "location_name", "sex", "age_name","year","qx","qx_u","qx_l"),
  var_label = c(
    "IHME ID for location",
    "Name of location",
    "Sex",
    "Age interval",
    "Calendar year",
    "Probability of death",
    "Upper bound of 95% uncertainty interval for probability of death",
    "Lower bound of 95% uncertainty interval for probability of death"
  ),
  notes = c(
    "See IHME Keys file for location codes",
    "",
    "1=male, 2=female, 3=both",
    "Age intervals vary in length and may overalp",
    "",
    "","",""
  ),
  stringsAsFactors = FALSE
)

# ------------------------------------------------------------
# Write variable dictionary
# ------------------------------------------------------------

vars_path <- file.path(OUT_DIR, "variables_info.csv")

write.csv(
  var_dict,
  vars_path,
  row.names = FALSE,
  na = ""
)

message("Wrote variable dictionary: ", vars_path)

# ------------------------------------------------------------
# Emit documentation warnings at the very end
# ------------------------------------------------------------
# Collect documentation warnings (function from R/utils.R)

doc_warnings <-doc_warnings_for_var_dict(out, 
  var_dict, dict_name = "variables_info.csv", df_name = "out")

if (length(doc_warnings) > 0) {
  warning(
    paste(doc_warnings, collapse = "\n"),
    call. = FALSE
  )
}

message("Done.")
