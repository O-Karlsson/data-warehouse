# R/build_ncdrisc_height_child_adolescent_country.R
#
# PURPOSE
# -------
# Download NCD-RisC child/adolescent country height dataset (Lancet 2020),
# store the source ZIP in raw, extract to cleaned, and write standardized outputs.

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

source('R/utils.R')

suppressPackageStartupMessages({
  library(readr)
  library(arrow)
  library(haven)
})

URL <- 'https://www.ncdrisc.org/downloads/bmi-height-2020/height/all_countries/NCD_RisC_Lancet_2020_height_child_adolescent_country.zip'

dir_raw <- file.path('data', 'raw', 'NCDRisc', 'height')
dir_clean <- file.path('data', 'cleaned', 'NCDRisc', 'height')

dir.create(dir_raw, recursive = TRUE, showWarnings = FALSE)
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

zip_name <- basename(URL)
zip_path <- file.path(dir_raw, zip_name)

# ------------------------------------------------------------------------------
# 1) Download raw ZIP
# ------------------------------------------------------------------------------

if (!file.exists(zip_path) || !isTRUE(file.info(zip_path)$size > 0)) {
  utils::download.file(URL, zip_path, mode = 'wb', quiet = TRUE)
}

# ------------------------------------------------------------------------------
# 2) Extract ZIP to cleaned (unzip-only)
# ------------------------------------------------------------------------------

utils::unzip(zipfile = zip_path, exdir = dir_clean, overwrite = TRUE)

members <- utils::unzip(zip_path, list = TRUE)$Name
members <- members[!grepl('/$', members) & nzchar(members)]

csv_members <- members[grepl('\\.csv$', members, ignore.case = TRUE)]
if (length(csv_members) == 0) {
  stop('No CSV files found in ZIP.', call. = FALSE)
}

if (length(csv_members) > 1) {
  message('Multiple CSV files found; using first member: ', csv_members[1])
}

source_csv <- file.path(dir_clean, csv_members[1])
if (!file.exists(source_csv)) {
  stop('Expected extracted CSV not found: ', source_csv, call. = FALSE)
}

# ------------------------------------------------------------------------------
# 3) Read extracted CSV and write standard outputs
# ------------------------------------------------------------------------------

df <- readr::read_csv(source_csv, show_col_types = FALSE, progress = FALSE)

# Stata variable names must be <=32 chars, start with a letter/underscore,
# and contain only letters, numbers, and underscores.
make_stata_names <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9_]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  x <- ifelse(grepl("^[a-z_]", x), x, paste0("v_", x))
  x <- substr(x, 1, 32)
  make.unique(x, sep = "_")
}

original_names <- names(df)
stata_names <- make_stata_names(original_names)
names(df) <- stata_names

if ('sex' %in% names(df)) {
  sex_raw <- as.character(df$sex)
  sex_norm <- tolower(trimws(sex_raw))
  df$sex <- ifelse(
    sex_norm == 'boys',
    1L,
    ifelse(sex_norm == 'girls', 2L, NA_integer_)
  )

  unmapped <- sort(unique(sex_raw[!is.na(sex_raw) & !(sex_norm %in% c('boys', 'girls'))]))
  if (length(unmapped) > 0) {
    warning(
      paste0(
        'Unmapped values in sex (set to NA): ',
        paste(unmapped, collapse = ', ')
      ),
      call. = FALSE
    )
  }
}

arch_dir <- file.path(dir_clean, '_archive')
dir.create(arch_dir, recursive = TRUE, showWarnings = FALSE)

out_parquet_final <- file.path(dir_clean, 'data.parquet')
out_parquet_tmp <- tempfile(fileext = '.parquet')
arrow::write_parquet(df, out_parquet_tmp)

res <- archive_and_promote(
  final_path = out_parquet_final,
  tmp_path = out_parquet_tmp,
  archive_dir = arch_dir
)

if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), '%Y-%m-%d_%H%M%S')

  if (file.exists(file.path(dir_clean, 'data.dta'))) {
    file.copy(
      file.path(dir_clean, 'data.dta'),
      file.path(arch_dir, paste0('data_', tag, '.dta')),
      overwrite = FALSE
    )
  }

  if (file.exists(file.path(dir_clean, 'data.csv'))) {
    file.copy(
      file.path(dir_clean, 'data.csv'),
      file.path(arch_dir, paste0('data_', tag, '.csv')),
      overwrite = FALSE
    )
  }
}

haven::write_dta(df, file.path(dir_clean, 'data.dta'))
readr::write_csv(df, file.path(dir_clean, 'data.csv'))

# ------------------------------------------------------------------------------
# 4) Documentation
# ------------------------------------------------------------------------------

sources_md <- file.path(dir_raw, 'SOURCES.md')
if (!file.exists(sources_md)) {
  writeLines(
    c(
      'NCD-RisC child/adolescent country height dataset (Lancet 2020 release).',
      paste0('Downloaded from: ', URL),
      paste0('Saved ZIP as: ', zip_path),
      '',
      'This dataset is already analysis-ready tabular output and requires no transformation at raw stage.',
      paste0('Downloaded on: ', format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z'))
    ),
    sources_md
  )
}

notes_md <- file.path(dir_clean, 'DATASET_NOTES.md')
writeLines(
  c(
    '# NCD-RisC Height Dataset Notes',
    '',
    paste0('Generated on: ', format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')),
    '',
    '- Source ZIP is stored in `data/raw/NCDRisc/height/`.',
    '- Cleaned folder contains files extracted directly from the source ZIP.',
    '- No row-level or column-level transformations were applied.',
    '- Column names are standardized to valid Stata variable names.',
    '- `sex` is recoded to numeric: Boys = 1, Girls = 2.',
    '- Standardized warehouse outputs are also written: `data.csv`, `data.parquet`, `data.dta`.',
    '',
    'Extracted files:',
    paste0('- ', members)
  ),
  notes_md
)

message('Done: NCD-RisC height raw/cleaned build complete.')
