# R/build_owid_academic_performance.R
#
# PURPOSE
# -------
# Download Our World in Data grapher files for academic performance
# (PISA mathematics, science, and reading; both/girls/boys),
# store raw CSV + metadata JSON files, merge them into one dataset,
# and write standard warehouse outputs plus lightweight documentation.
#
# Raw storage:
#   data/raw/OWID/academic-performance/
#
# Cleaned storage:
#   data/cleaned/OWID/academic-performance/
#
# IDs used to merge:
#   entity, code, year

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------
source("R/utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(arrow)
  library(haven)
})

BASE_CSV_URL <- paste0(
  "https://ourworldindata.org/grapher/academic-performance.csv",
  "?v=1&csvType=full&useColumnShortNames=true",
  "&subject=%s&sex=%s"
)

BASE_META_URL <- paste0(
  "https://ourworldindata.org/grapher/academic-performance.metadata.json",
  "?v=1&csvType=full&useColumnShortNames=true",
  "&subject=%s&sex=%s"
)

SUBJECTS <- c("mathematics", "science", "reading")
SEXES <- c("both", "girls", "boys")
ID_COLS <- c("entity", "code", "year")

VALUE_NAME_MAP <- c(
  mathematics = "math",
  science     = "science",
  reading     = "reading"
)

SEX_NAME_MAP <- c(
  both  = "both",
  girls = "girls",
  boys  = "boys"
)

manifest <- expand.grid(
  subject = SUBJECTS,
  sex     = SEXES,
  stringsAsFactors = FALSE
) %>%
  arrange(subject, sex) %>%
  mutate(
    csv_url = sprintf(BASE_CSV_URL, subject, sex),
    meta_url = sprintf(BASE_META_URL, subject, sex),
    csv_file = sprintf("academic-performance_subject-%s_sex-%s.csv", subject, sex),
    meta_file = sprintf("academic-performance_subject-%s_sex-%s.metadata.json", subject, sex),
    value_var = sprintf(
      "score_%s_%s",
      unname(VALUE_NAME_MAP[subject]),
      unname(SEX_NAME_MAP[sex])
    )
  )

dir_raw <- file.path("data", "raw", "OWID", "academic-performance")
dir_clean <- file.path("data", "cleaned", "OWID", "academic-performance")

dir.create(dir_raw, recursive = TRUE, showWarnings = FALSE)
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
download_if_needed <- function(url, dest) {
  if (file.exists(dest) && isTRUE(file.info(dest)$size > 0)) {
    return(invisible(TRUE))
  }

  ok <- tryCatch({
    suppressWarnings(utils::download.file(url, dest, mode = "wb", quiet = TRUE))
    file.exists(dest) && isTRUE(file.info(dest)$size > 0)
  }, error = function(e) FALSE)

  if (!ok) {
    if (file.exists(dest)) unlink(dest)
    stop(sprintf("Download failed: %s -> %s", url, dest), call. = FALSE)
  }

  invisible(TRUE)
}

read_one_csv <- function(path, value_var) {
  df <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE)

  missing_ids <- setdiff(ID_COLS, names(df))
  if (length(missing_ids) > 0) {
    stop(
      sprintf(
        "Missing expected id columns in %s: %s",
        path,
        paste(missing_ids, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  value_cols <- setdiff(names(df), ID_COLS)
  if (length(value_cols) != 1) {
    stop(
      sprintf(
        "Expected exactly one non-id value column in %s, found: %s",
        path,
        paste(value_cols, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  df %>%
    select(all_of(ID_COLS), all_of(value_cols)) %>%
    rename(!!value_var := all_of(value_cols))
}

# ------------------------------------------------------------------------------
# 1) Download raw CSV + metadata JSON files
# ------------------------------------------------------------------------------
for (i in seq_len(nrow(manifest))) {
  csv_path <- file.path(dir_raw, manifest$csv_file[i])
  meta_path <- file.path(dir_raw, manifest$meta_file[i])

  download_if_needed(manifest$csv_url[i], csv_path)
  download_if_needed(manifest$meta_url[i], meta_path)
}

# ------------------------------------------------------------------------------
# 2) Read and merge the 9 source CSVs
# ------------------------------------------------------------------------------
dfs <- lapply(seq_len(nrow(manifest)), function(i) {
  read_one_csv(
    path = file.path(dir_raw, manifest$csv_file[i]),
    value_var = manifest$value_var[i]
  )
})

df_clean <- Reduce(function(x, y) full_join(x, y, by = ID_COLS), dfs) %>%
  arrange(entity, code, year)

# ------------------------------------------------------------------------------
# 3) Write outputs (archive + promote pattern)
# ------------------------------------------------------------------------------
arch_dir <- file.path(dir_clean, "_archive")
dir.create(arch_dir, recursive = TRUE, showWarnings = FALSE)

out_parquet_final <- file.path(dir_clean, "data.parquet")
out_parquet_tmp <- tempfile(fileext = ".parquet")
arrow::write_parquet(df_clean, out_parquet_tmp)

res <- archive_and_promote(
  final_path  = out_parquet_final,
  tmp_path    = out_parquet_tmp,
  archive_dir = arch_dir
)

if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")

  if (file.exists(file.path(dir_clean, "data.dta"))) {
    file.copy(
      file.path(dir_clean, "data.dta"),
      file.path(arch_dir, paste0("data_", tag, ".dta")),
      overwrite = FALSE
    )
  }

  if (file.exists(file.path(dir_clean, "data.csv"))) {
    file.copy(
      file.path(dir_clean, "data.csv"),
      file.path(arch_dir, paste0("data_", tag, ".csv")),
      overwrite = FALSE
    )
  }
}

haven::write_dta(df_clean, file.path(dir_clean, "data.dta"))
readr::write_csv(df_clean, file.path(dir_clean, "data.csv"))

# ------------------------------------------------------------------------------
# 4) Documentation
# ------------------------------------------------------------------------------
sources_md <- file.path(dir_raw, "SOURCES.md")
source_lines <- c(
  "Source: Our World in Data grapher dataset `academic-performance`.",
  "Underlying topic: PISA academic performance by subject and sex.",
  "Website reference: https://ourworldindata.org/grapher/academic-performance?subject=mathematics&sex=both",
  "",
  "Downloaded CSV files:"
)

csv_lines <- paste0(
  "- ",
  manifest$csv_url
)

meta_header <- c(
  "",
  "Downloaded metadata JSON files:"
)

meta_lines <- paste0(
  "- ",
  manifest$meta_url
)

timestamp_line <- paste0(
  "",
  "This file was created on: ",
  format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
)

writeLines(
  c(source_lines, csv_lines, meta_header, meta_lines, timestamp_line),
  sources_md
)

notes_path <- file.path(dir_clean, "DATASET_NOTES.md")
notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  "- This dataset combines 9 Our World in Data grapher extracts for academic performance.",
  "- Subjects included: mathematics, science, and reading.",
  "- Sex breakdowns included: both, girls, and boys.",
  "- Raw CSV and metadata JSON files are stored in `data/raw/OWID/academic-performance/`.",
  "- The cleaned dataset is created by full-joining all source CSV files on `entity`, `code`, and `year`.",
  "- Value columns are renamed to standardized warehouse-style names such as `score_math_both` and `score_reading_girls`.",
  "- Source URLs are parameterized OWID grapher download links supplied directly in the build script."
)
writeLines(notes_lines, notes_path)

var_dict <- data.frame(
  var = c(
    "entity",
    "code",
    "year",
    manifest$value_var
  ),
  var_label = c(
    "Entity name",
    "Entity code",
    "Calendar year",
    paste(
      "Average academic performance score in",
      manifest$subject,
      "(",
      manifest$sex,
      ")"
    )
  ),
  notes = c(
    "Entity label as provided by the OWID grapher export.",
    "Code as provided by the OWID grapher export; often ISO3 for countries but may include non-ISO codes for aggregates.",
    "Calendar year.",
    paste(
      "Downloaded from OWID grapher dataset `academic-performance` using subject =",
      manifest$subject,
      "and sex =",
      manifest$sex,
      "."
    )
  ),
  stringsAsFactors = FALSE
)

vars_path <- file.path(dir_clean, "variables_info.csv")
write.csv(var_dict, vars_path, row.names = FALSE, na = "")

doc_warnings <- doc_warnings_for_var_dict(
  df_clean,
  var_dict,
  dict_name = "variables_info.csv",
  df_name   = "df_clean"
)

if (length(doc_warnings) > 0) {
  warning(paste(doc_warnings, collapse = "\n"), call. = FALSE)
}

message("Done: OWID academic performance raw/cleaned build complete.")
