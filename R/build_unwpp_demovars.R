# R/build_hld_life_tables_ITA_GBR_DEU.R

source("R/utils.R")

library(dplyr)
library(readr)
library(arrow)
library(haven)
library(purrr)

COUNTRIES <- c("GBR", "ITA", "DEU")

RAW_BASE <- file.path("data", "raw", "HLD", "life tables")
OUT_BASE <- file.path("data", "cleaned", "HLD", "life tables")

rename_hld_cols <- function(df) {
  names(df) <- names(df) |>
    gsub("m\\(x\\)", "mx", x = _) |>
    gsub("q\\(x\\)", "qx", x = _) |>
    gsub("l\\(x\\)", "lx", x = _) |>
    gsub("d\\(x\\)", "dx", x = _) |>
    gsub("L\\(x\\)", "lx_1", x = _) |>
    gsub("T\\(x\\)", "tx", x = _) |>
    gsub("e\\(x\\)Orig", "exOrig", x = _) |>
    gsub("e\\(x\\)", "ex", x = _)
  
  df
}

for (ctry in COUNTRIES) {
  
  message("Processing ", ctry)
  
  raw_dir <- file.path(RAW_BASE, ctry)
  out_dir <- file.path(OUT_BASE, ctry)
  
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  zip_url <- paste0("https://www.lifetable.de/File/GetDocument/data/", ctry, "/", ctry, ".zip")
  zip_path <- file.path(raw_dir, paste0(ctry, ".zip"))
  
  if (!file.exists(zip_path)) {
    download.file(zip_url, zip_path, mode = "wb")
    unzip(zip_path, exdir = raw_dir)
  }
  
  data_file <- list.files(raw_dir, pattern = "\\.(txt|csv|dat)$",
                          full.names = TRUE)[1]
  
  df <- read_delim(
    data_file,
    delim = ";",
    locale = locale(decimal_mark = "."),
    show_col_types = FALSE
  )
  
  df <- df |>
    rename_hld_cols() |>
    mutate(across(where(is.character), ~ type.convert(.x, as.is = TRUE))) |>
    mutate(country = ctry, .before = 1)
  
  # Make Stata-safe names
  names(df) <- make.names(names(df))
  names(df) <- substr(names(df), 1, 32)
  
  write_parquet(df, file.path(out_dir, "data.parquet"))
  write_csv(df, file.path(out_dir, "data.csv"))
  write_dta(df, file.path(out_dir, "data.dta"))
}
