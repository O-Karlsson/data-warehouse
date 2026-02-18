# R/build_hld_life_tables_ITA_GBR_DEU.R
#
# PURPOSE
# -------
# Download and clean Human Lifetable Database (HLD) life tables for:
#   - United Kingdom (GBR)
#   - Italy (ITA)
#   - Germany (DEU)
#
# Raw zip files are stored in:
#   data/raw/HLD/life tables/<country>/
#
# Cleaned outputs are written (parquet + dta + csv) to the same folder:
#   data/raw/HLD/life tables/<country>/
#
# Before saving, the data are joined with:
#   data/manual/HLD/codes/region_codes_ITA_GBR_DEU.csv
# using keys:
#   - Country (DEU, GBR, ITA)  -> cleaned to `country`
#   - Region  (e.g., 10, 20, 30, ENG0, FRG0, ...) -> cleaned to `region`
#
# IMPORTANT: Region is NOT guaranteed to be numeric in HLD files, so we treat it
# as a string key and join on (country, region) as character.
#
# Column labels are cleaned to remove characters like "(", ")", and "-".
#
# Sources:
#   https://www.lifetable.de/File/GetDocument/data/GBR/GBR.zip
#   https://www.lifetable.de/File/GetDocument/data/ITA/ITA.zip
#   https://www.lifetable.de/File/GetDocument/data/DEU/DEU.zip

# R/build_hld_life_tables_ITA_GBR_DEU.R

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------
source("R/utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(arrow)
  library(haven)
  library(tibble)
})

CODES_PATH <- file.path("data", "manual", "HLD", "codes", "region_codes_ITA_GBR_DEU.csv")

countries <- tibble::tribble(
  ~country_name,      ~iso3, ~url,
  "United Kingdom",   "GBR", "https://www.lifetable.de/File/GetDocument/data/GBR/GBR.zip",
  "Italy",            "ITA", "https://www.lifetable.de/File/GetDocument/data/ITA/ITA.zip",
  "Germany",          "DEU", "https://www.lifetable.de/File/GetDocument/data/DEU/DEU.zip"
)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

download_if_missing <- function(url, dest_path) {
  if (file.exists(dest_path) && file.info(dest_path)$size > 0) return(invisible(TRUE))
  
  ok <- tryCatch({
    suppressWarnings(utils::download.file(url, dest_path, mode = "wb", quiet = TRUE))
    file.exists(dest_path) && file.info(dest_path)$size > 0
  }, error = function(e) FALSE)
  
  if (!ok) {
    if (file.exists(dest_path)) unlink(dest_path)
    stop(sprintf("Download failed: %s -> %s", url, dest_path), call. = FALSE)
  }
  
  invisible(TRUE)
}

clean_colnames_hld <- function(nm) {
  nm <- tolower(nm)
  nm <- gsub("[()\\-]", "_", nm)
  nm <- gsub("[^a-z0-9_]", "_", nm)
  nm <- gsub("_+", "_", nm)
  nm <- gsub("^_|_$", "", nm)
  nm <- gsub("^([0-9])", "_\\1", nm)
  make.unique(nm, sep = "_")
}

pick_csv_inside_zip <- function(zip_path) {
  z <- utils::unzip(zip_path, list = TRUE)$Name
  csvs <- z[grepl("\\.csv$", z, ignore.case = TRUE)]
  if (length(csvs) == 0) {
    stop("No CSV found inside zip: ", zip_path, call. = FALSE)
  }
  csvs[1]
}

read_csv_all_chr_logged <- function(path_or_con, problems_path = NULL) {
  dat <- withCallingHandlers(
    readr::read_csv(
      path_or_con,
      show_col_types = FALSE,
      progress = FALSE,
      col_types = readr::cols(.default = readr::col_character())
    ),
    warning = function(w) {
      if (grepl("One or more parsing issues", conditionMessage(w), fixed = TRUE)) {
        invokeRestart("muffleWarning")
      }
    }
  )
  
  probs <- readr::problems(dat)
  if (nrow(probs) > 0 && !is.null(problems_path)) {
    dir.create(dirname(problems_path), recursive = TRUE, showWarnings = FALSE)
    readr::write_csv(probs, problems_path)
  }
  
  dat
}

safe_join_region_codes <- function(df, codes) {
  df <- df %>%
    mutate(
      country = as.character(country),
      region  = str_trim(as.character(region))
    )
  
  codes <- codes %>%
    mutate(
      country = as.character(country),
      region  = str_trim(as.character(region))
    )
  
  codes_dedup <- codes %>%
    group_by(country, region) %>%
    summarise(across(everything(), ~ first(na.omit(.x))), .groups = "drop")
  
  left_join(df, codes_dedup, by = c("country", "region"), relationship = "many-to-one")
}

write_outputs_with_archive <- function(df, clean_dir) {
  
  dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)
  
  arch_dir <- file.path(clean_dir, "_archive")
  out_parquet_final <- file.path(clean_dir, "data.parquet")
  out_parquet_tmp   <- tempfile(fileext = ".parquet")
  
  arrow::write_parquet(df, out_parquet_tmp)
  
  res <- archive_and_promote(
    final_path  = out_parquet_final,
    tmp_path    = out_parquet_tmp,
    archive_dir = arch_dir
  )
  
  if (isTRUE(res$changed)) {
    tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
    
    if (file.exists(file.path(clean_dir, "data.dta"))) {
      file.copy(
        file.path(clean_dir, "data.dta"),
        file.path(arch_dir, paste0("data_", tag, ".dta")),
        overwrite = FALSE
      )
    }
    if (file.exists(file.path(clean_dir, "data.csv"))) {
      file.copy(
        file.path(clean_dir, "data.csv"),
        file.path(arch_dir, paste0("data_", tag, ".csv")),
        overwrite = FALSE
      )
    }
  }
  
  haven::write_dta(df, file.path(clean_dir, "data.dta"))
  readr::write_csv(df, file.path(clean_dir, "data.csv"))
  
  invisible(TRUE)
}

# ------------------------------------------------------------------------------
# Load region codes
# ------------------------------------------------------------------------------

region_codes <- read_csv_all_chr_logged(CODES_PATH)
names(region_codes) <- clean_colnames_hld(names(region_codes))

# ------------------------------------------------------------------------------
# Build per-country
# ------------------------------------------------------------------------------

for (i in seq_len(nrow(countries))) {
  
  country_name <- countries$country_name[i]
  iso3         <- countries$iso3[i]
  url          <- countries$url[i]
  
  raw_dir   <- file.path("data", "raw", "HLD", "life tables", country_name)
  clean_dir <- file.path("data", "cleaned", "HLD", "life tables", country_name)
  
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
  
  zip_path <- file.path(raw_dir, paste0(iso3, ".zip"))
  
  # 1) Download ZIP -> RAW
  download_if_missing(url, zip_path)
  
  # 2) Read CSV from ZIP
  csv_rel <- pick_csv_inside_zip(zip_path)
  
  df <- read_csv_all_chr_logged(
    unz(zip_path, csv_rel),
    problems_path = file.path(raw_dir, "_logs", paste0("problems_", iso3, ".csv"))
  )
  
  names(df) <- clean_colnames_hld(names(df))
  
  # 3) Join region names
  df <- safe_join_region_codes(df, region_codes)
  
  df <- df %>% mutate(country_name = country_name, .before = 1)
  
  # 4) Write CLEANED outputs
  write_outputs_with_archive(df, clean_dir)
  
  message("Done: ", country_name, " (", iso3, ")")
}
