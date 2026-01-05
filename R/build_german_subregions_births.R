# R/build_german_subregions_births.R
#
# Download GENESIS table 12612-0100 via restatis, then clean to:
#   region_name, sex (1/2/3), year, births
source("R/utils.R")

TABLE   <- "12612-0100"
RAW_DIR <- file.path("data", "raw", "National Statistics", "Germany", "births")
OUT_DIR <- file.path("data", "cleaned", "National Statistics", "Germany", "births")
dir.create(RAW_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ---- 1) Download via restatis ----
# (Assumes you ran once: restatis::gen_auth_save("genesis", use_token = FALSE)
# ---- 1) Download via restatis (only if raw data missing) ----
# If there is an issue with the API, the file can also be manually downloaded from here:
# https://www-genesis.destatis.de/datenbank/online/statistic/12612/table/12612-0100/search/s/YmlydGhz

raw_csv <- file.path(RAW_DIR, paste0(TABLE, ".csv"))

if (!file.exists(raw_csv) || file.info(raw_csv)$size == 0) {
  
  message("Raw data not found. Downloading from GENESIS API…")
  options(restatis.use_cache = FALSE)
    # (Assumes you ran once: restatis::gen_auth_save("genesis", use_token = FALSE)
  tab <- restatis::gen_table(
    TABLE,
    database = "genesis",
    area = "all",
    language = "en",
    transpose = TRUE,
    compress = TRUE,
    all_character = TRUE
  )
  
  # Save raw snapshot
  readr::write_csv(tab, raw_csv)
  
} else {
  
  message("Raw data already exists. Using cached file: ", raw_csv)
  tab <- readr::read_csv(raw_csv, show_col_types = FALSE)
  
}
# ---- 2) Clean names (same as your other scripts) ----
clean_names <- function(nm) {
  nm <- tolower(nm)
  nm <- gsub("[^a-z0-9_]", "_", nm)
  nm <- gsub("^([0-9])", "_\\1", nm)
  make.unique(nm, sep = "_")
}
names(tab) <- clean_names(names(tab))

trim <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x
}
parse_num <- function(x) {
  x <- trim(x)
  x[x %in% c(":", "-", "…")] <- NA_character_
  x <- gsub("\\.", "", x)
  x <- gsub(",", ".", x)
  suppressWarnings(as.numeric(x))
}

# ---- 3) Identify region/sex slots (_1, _2, ...) ----
nm <- names(tab)
var_label_cols <- grep("^_[0-9]+_variable_label$", nm, value = TRUE)
if (length(var_label_cols) == 0) {
  stop("No '_k_variable_label' columns found. Columns:\n  ", paste(nm, collapse = "\n  "), call. = FALSE)
}

slot_of <- function(keyword_patterns) {
  for (col in var_label_cols) {
    lab <- unique(na.omit(trim(tab[[col]])))
    lab <- if (length(lab) == 0) "" else tolower(lab[1])
    if (any(sapply(keyword_patterns, function(p) grepl(p, lab)))) {
      return(sub("_variable_label$", "", col)) # e.g., "_1"
    }
  }
  NA_character_
}

region_slot <- slot_of(c("l[äa]nder", "land", "region", "state"))
sex_slot    <- slot_of(c("^sex$", "gender", "geschlecht"))

if (is.na(region_slot) || is.na(sex_slot)) {
  stop(
    "Could not identify region/sex slots.\n\n",
    "Variable labels seen:\n  ",
    paste(sapply(var_label_cols, function(cn) {
      lab <- unique(na.omit(trim(tab[[cn]])))
      paste0(cn, " = ", if (length(lab)) lab[1] else "<empty>")
    }), collapse = "\n  "),
    call. = FALSE
  )
}

region_name_col <- paste0(region_slot, "_variable_attribute_label")
sexlab_col      <- paste0(sex_slot,    "_variable_attribute_label")

if (!(region_name_col %in% nm)) stop("Missing column: ", region_name_col, call. = FALSE)
if (!(sexlab_col %in% nm))      stop("Missing column: ", sexlab_col, call. = FALSE)
if (!("time" %in% nm))          stop("Missing column: time", call. = FALSE)
if (!("value" %in% nm))         stop("Missing column: value", call. = FALSE)

# ---- 4) Build output ----
region_name <- trim(tab[[region_name_col]])
sexlab      <- tolower(trim(tab[[sexlab_col]]))

sex <- rep(NA_integer_, length(sexlab))
sex[sexlab %in% c("male", "males")]     <- 1L
sex[sexlab %in% c("female", "females")] <- 2L
sex[sexlab %in% c("total")]             <- 3L


if (any(is.na(sex))) {
  bad <- unique(tab[[sexlab_col]][is.na(sex)])
  stop("Unrecognized sex labels:\n  ", paste(bad, collapse = "\n  "), call. = FALSE)
}

out <- data.frame(
  region_name = region_name,
  sex         = sex,
  year        = as.integer(tab$time),
  births      = parse_num(tab$value),
  stringsAsFactors = FALSE
)

out <- out[!is.na(out$region_name) & !is.na(out$year) & !is.na(out$births), , drop = FALSE]

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
  "This dataset contains annual counts of live births by federal state (Bundesland), year, and sex for Germany.",
  "Data are sourced from the German Federal Statistical Office (Destatis) via the GENESIS-Online database, table 12612-0100.",
  "Birth counts are reported as absolute numbers.",
  "See: https://www-genesis.destatis.de/datenbank/online/statistic/12612/table/12612-0100/search/s/YmlydGhz"
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
  var = c("region_name", "sex", "year", "births"),
  var_label = c(
    "Name of location",
    "Sex",
    "Calendar year",
    "Number of live births"
  ),
  notes = c(
    "",
    "1=male, 2=female, 3=both",
    "",
    ""
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
