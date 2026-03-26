# R/build_nhanes3_public.R
#
# PURPOSE
# -------
# Mirror publicly linked NHANES III assets from the official CDC data-files page.
#
# This legacy builder is intentionally different from build_nhanes_public.R:
# NHANES III exposes fixed-width DAT files, SAS import scripts, PDFs, and other
# supporting assets rather than the modern one-table-per-XPT manifest.
#
# Current behavior:
#   - scrape the official NHANES III data-files page
#   - inventory downloadable assets and selected documentation pages
#   - download direct public file assets into data/raw/NHANES/NHANES Prior to 1999/NHANES III/
#   - write raw and cleaned metadata inventories
#   - convert directly readable XPT/CSV assets into same-stem cleaned outputs
#
# It does NOT yet:
#   - parse DAT files into cleaned tables
#   - execute SAS import scripts
#   - harmonize files across releases

source("R/utils.R")

suppressPackageStartupMessages({
  library(xml2)
  library(rvest)
  library(haven)
  library(arrow)
  library(readr)
  library(dplyr)
  library(stringr)
  library(purrr)
  library(tibble)
})

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

SOURCE_URL <- "https://wwwn.cdc.gov/nchs/nhanes/nhanes3/datafiles.aspx"

RAW_ROOT   <- file.path("data", "raw", "NHANES", "NHANES Prior to 1999", "NHANES III")
CLEAN_ROOT <- file.path("data", "cleaned", "NHANES", "NHANES Prior to 1999", "NHANES III")

DOWNLOAD_RAW <- !identical(tolower(Sys.getenv("NHANES3_DOWNLOAD_RAW", "true")), "false")
MAX_FILES <- suppressWarnings(as.integer(Sys.getenv("NHANES3_MAX_FILES", "")))
if (is.na(MAX_FILES)) MAX_FILES <- Inf

dir.create(RAW_ROOT, recursive = TRUE, showWarnings = FALSE)
dir.create(CLEAN_ROOT, recursive = TRUE, showWarnings = FALSE)

raw_meta_path <- file.path(RAW_ROOT, "metadata_raw.csv")
clean_meta_path <- file.path(CLEAN_ROOT, "metadata.csv")
notes_path <- file.path(CLEAN_ROOT, "DATASET_NOTES.md")
sources_path <- file.path(RAW_ROOT, "SOURCES.md")

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

safe_name <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("[/\\:*?\"<>|]+", "_", x)
  x <- gsub("\\s+", " ", x)
  x
}

download_if_needed <- function(url, dest) {
  if (file.exists(dest) && isTRUE(file.info(dest)$size > 0)) return(invisible(TRUE))

  tmp <- paste0(dest, ".tmp")
  if (file.exists(tmp)) unlink(tmp, force = TRUE)
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)

  ok <- tryCatch({
    utils::download.file(url, destfile = tmp, mode = "wb", quiet = TRUE)
    file.exists(tmp) && isTRUE(file.info(tmp)$size > 0)
  }, error = function(e) FALSE)

  if (!ok) {
    if (file.exists(tmp)) unlink(tmp, force = TRUE)
    stop(sprintf("Download failed: %s -> %s", url, dest), call. = FALSE)
  }

  moved <- file.rename(tmp, dest)
  if (!moved) {
    file.copy(tmp, dest, overwrite = TRUE)
    unlink(tmp, force = TRUE)
  }

  invisible(TRUE)
}

write_named_sidecars <- function(df, out_dir, file_stem, write_csv = TRUE) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(df, file.path(out_dir, paste0(file_stem, ".parquet")))
  haven::write_dta(df, file.path(out_dir, paste0(file_stem, ".dta")))

  if (isTRUE(write_csv)) {
    readr::write_csv(df, file.path(out_dir, paste0(file_stem, ".csv")))
  }
}

convert_cleanable_asset <- function(asset_path, clean_dir, write_csv = TRUE) {
  ext <- tolower(tools::file_ext(asset_path))
  stem <- tools::file_path_sans_ext(basename(asset_path))

  if (ext == "xpt") {
    df <- haven::read_xpt(asset_path)
    write_named_sidecars(df, clean_dir, stem, write_csv = write_csv)
    return("converted")
  }

  if (ext == "csv") {
    df <- readr::read_csv(asset_path, show_col_types = FALSE, progress = FALSE)
    write_named_sidecars(df, clean_dir, stem, write_csv = write_csv)
    return("converted")
  }

  "unsupported"
}

absolute_url <- function(base_url, href) {
  if (length(href) == 0 || is.na(href) || !nzchar(href)) return(NA_character_)

  if (startsWith(href, "http://") || startsWith(href, "https://")) return(href)
  if (startsWith(href, "//")) return(paste0("https:", href))
  if (startsWith(href, "/")) return(paste0("https://wwwn.cdc.gov", href))

  xml2::url_absolute(href, base_url)
}

classify_asset_type <- function(url) {
  u <- tolower(url)

  dplyr::case_when(
    str_detect(u, "\\.xpt($|\\?)") ~ "data_xpt",
    str_detect(u, "\\.dat($|\\?)") ~ "data_dat",
    str_detect(u, "\\.sas($|\\?)") ~ "code_sas",
    str_detect(u, "\\.pdf($|\\?)") ~ "doc_pdf",
    str_detect(u, "\\.txt($|\\?)") ~ "doc_txt",
    str_detect(u, "\\.zip($|\\?)") ~ "data_zip",
    str_detect(u, "\\.exe($|\\?)") ~ "binary_exe",
    str_detect(u, "\\.aspx($|\\?)|\\.htm(l)?($|\\?)") ~ "doc_page",
    TRUE ~ "other"
  )
}

release_from_url <- function(url) {
  rel <- stringr::str_match(url, "/nchs/data/nhanes3/([^/]+)/")[, 2]
  ifelse(is.na(rel), "docs", rel)
}

file_name_from_url <- function(url) {
  out <- basename(gsub("\\?.*$", "", url))
  out[out == ""] <- NA_character_
  out
}

read_local_nhanes3_inventory <- function(path = raw_meta_path) {
  if (!file.exists(path)) {
    stop("Local NHANES III metadata_raw.csv fallback not found.", call. = FALSE)
  }

  out <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE) %>%
    mutate(across(everything(), as.character))

  required <- c("release", "asset_url", "asset_type")
  missing <- setdiff(required, names(out))
  if (length(missing) > 0) {
    stop(
      "Local NHANES III metadata_raw.csv is missing required columns: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  out %>%
    mutate(
      is_downloadable = tolower(coalesce(.data$is_downloadable, "false")) == "true",
      local_exists = if ("local_exists" %in% names(.)) {
        tolower(coalesce(.data$local_exists, "false")) == "true"
      } else {
        FALSE
      }
    )
}

nhanes3_inventory <- function() {
  out <- tryCatch({
    page <- xml2::read_html(SOURCE_URL)
    anchors <- rvest::html_elements(page, "a")

    tibble::tibble(
      link_text = trimws(rvest::html_text2(anchors)),
      href = rvest::html_attr(anchors, "href")
    ) %>%
      filter(!is.na(.data$href), nzchar(.data$href)) %>%
      mutate(
        asset_url = vapply(.data$href, absolute_url, FUN.VALUE = character(1), base_url = SOURCE_URL),
        asset_type = classify_asset_type(.data$asset_url),
        release = release_from_url(.data$asset_url),
        file_name = file_name_from_url(.data$asset_url),
        is_downloadable = str_detect(.data$asset_url, "^https://wwwn\\.cdc\\.gov/nchs/data/nhanes3/"),
        keep = .data$is_downloadable |
          str_detect(.data$asset_url, "^https://wwwn\\.cdc\\.gov/nchs/nhanes/nhanes3/") |
          .data$link_text %in% c(
            "Reference Manuals and Report",
            "Anthropometric Procedure Videos",
            "Survey Methods and Analytic Guidelines",
            "NHANES III Public Use Data Files Errata",
            "Serum Latex Allergy (IgE) Data Analysis Issues",
            "Data Files",
            "Usage Notes"
          )
      ) %>%
      filter(.data$keep) %>%
      distinct(.data$asset_url, .keep_all = TRUE) %>%
      mutate(
        release = safe_name(.data$release),
        file_name = ifelse(is.na(.data$file_name) | .data$file_name == "", NA_character_, .data$file_name),
        local_path = ifelse(
          .data$is_downloadable & !is.na(.data$file_name),
          file.path(RAW_ROOT, .data$release, .data$file_name),
          NA_character_
        ),
        local_exists = ifelse(!is.na(.data$local_path), file.exists(.data$local_path), FALSE),
        discovered_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      ) %>%
      select(
        "release",
        "link_text",
        "asset_type",
        "asset_url",
        "file_name",
        "is_downloadable",
        "local_path",
        "local_exists",
        "discovered_at"
      ) %>%
      arrange(.data$release, .data$asset_type, .data$file_name, .data$link_text)
  }, error = function(e) {
    message("Live NHANES III inventory discovery failed: ", conditionMessage(e))
    message("Falling back to existing local metadata_raw.csv inventory.")
    read_local_nhanes3_inventory()
  })

  out
}

# ------------------------------------------------------------------------------
# BUILD
# ------------------------------------------------------------------------------

message("Scraping NHANES III data-files page...")
inventory <- nhanes3_inventory()

download_rows <- inventory %>%
  filter(.data$is_downloadable, !is.na(.data$local_path), nzchar(.data$local_path))

if (is.finite(MAX_FILES)) {
  download_rows <- download_rows %>% slice_head(n = MAX_FILES)
}

if (DOWNLOAD_RAW && nrow(download_rows) > 0) {
  message("Downloading NHANES III raw assets...")

  for (i in seq_len(nrow(download_rows))) {
    r <- download_rows[i, , drop = FALSE]
    message(sprintf("[%d/%d] %s", i, nrow(download_rows), r$file_name[[1]]))
    download_if_needed(r$asset_url[[1]], r$local_path[[1]])
  }
}

inventory <- inventory %>%
  mutate(
    local_exists = ifelse(!is.na(.data$local_path), file.exists(.data$local_path), FALSE),
    local_size = ifelse(
      .data$local_exists & !is.na(.data$local_path),
      as.numeric(file.info(.data$local_path)$size),
      NA_real_
    )
  )

cleanable_rows <- which(
  inventory$asset_type %in% c("data_xpt", "data_csv") &
    inventory$local_exists &
    !is.na(inventory$local_path) &
    nzchar(inventory$local_path)
)

if (length(cleanable_rows) > 0) {
  clean_parquet_path <- rep(NA_character_, nrow(inventory))
  clean_dta_path <- rep(NA_character_, nrow(inventory))
  clean_csv_path <- rep(NA_character_, nrow(inventory))
  clean_conversion_status <- rep(NA_character_, nrow(inventory))

  for (idx in cleanable_rows) {
    asset_path <- inventory$local_path[[idx]]
    rel_dir <- dirname(asset_path)
    rel_dir <- sub(paste0("^", gsub("\\\\", "\\\\\\\\", RAW_ROOT)), "", rel_dir)
    rel_dir <- sub("^[/\\\\]+", "", rel_dir)
    target_dir <- if (nzchar(rel_dir)) file.path(CLEAN_ROOT, rel_dir) else CLEAN_ROOT
    stem <- tools::file_path_sans_ext(basename(asset_path))

    status <- tryCatch(
      convert_cleanable_asset(asset_path, target_dir, write_csv = TRUE),
      error = function(e) paste0("failed: ", conditionMessage(e))
    )

    clean_parquet_path[[idx]] <- file.path(target_dir, paste0(stem, ".parquet"))
    clean_dta_path[[idx]] <- file.path(target_dir, paste0(stem, ".dta"))
    clean_csv_path[[idx]] <- file.path(target_dir, paste0(stem, ".csv"))
    clean_conversion_status[[idx]] <- status
  }

  inventory$clean_parquet_path <- clean_parquet_path
  inventory$clean_dta_path <- clean_dta_path
  inventory$clean_csv_path <- clean_csv_path
  inventory$clean_conversion_status <- clean_conversion_status
}

write.csv(inventory, raw_meta_path, row.names = FALSE)
write.csv(inventory, clean_meta_path, row.names = FALSE)

writeLines(
  c(
    "NHANES III public-use assets mirrored from the official CDC/NCHS NHANES III data files page.",
    paste0("Source page: ", SOURCE_URL),
    "This legacy survey distributes DAT files, SAS import scripts, PDFs, TXT files, and related documentation rather than the modern XPT manifest.",
    paste0("Generated on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
  ),
  sources_path
)

writeLines(
  c(
    "# NHANES III dataset notes",
    "",
    sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "- This builder mirrors publicly linked NHANES III assets from the official CDC data-files page.",
    "- Raw assets are stored under `data/raw/NHANES/NHANES Prior to 1999/NHANES III/` using the CDC release-folder structure where available.",
    "- `metadata_raw.csv` and `metadata.csv` currently act as asset inventories, not parsed table-level metadata.",
    "- Where an NHANES III asset is directly readable as `XPT` or `CSV`, the cleaned tree stores same-stem conversions such as `VID_NH3.parquet` and `VID_NH3.dta`.",
    "- Direct file assets may include `DAT`, `SAS`, `PDF`, `TXT`, `ZIP`, and related legacy formats.",
    "- DAT parsing and fixed-width table conversion are not yet implemented in this script.",
    paste0("- Raw downloading enabled for this run: ", if (DOWNLOAD_RAW) "TRUE" else "FALSE"),
    paste0("- Max files setting for this run: ", if (is.finite(MAX_FILES)) as.character(MAX_FILES) else "Inf")
  ),
  notes_path
)

message("Wrote: ", raw_meta_path)
message("Wrote: ", clean_meta_path)
message("Wrote: ", sources_path)
message("Wrote: ", notes_path)
message("Done.")
