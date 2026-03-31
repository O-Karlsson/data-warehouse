# PURPOSE
# -------
# Build NHANES directly from `data/NHANES links All.csv`.
#
# This script:
#   1. Reads the manually curated NHANES links CSV
#   2. Expands each source row into downloadable assets:
#      - data
#      - sas code
#      - formatted sas
#   3. Downloads assets into `data/raw/NHANES/<wave>/`
#   4. Mirrors that structure in `data/cleaned/NHANES/<wave>/`
#   5. Converts readable raw files into `.parquet`, `.dta`, and optional `.csv`
#   6. Uses row-linked SAS/setup files to parse fixed-width TXT/DAT datasets
#   7. Writes inventories, helper manifests, rename maps, and notes

source("R/utils.R")

suppressPackageStartupMessages({
  library(haven)
  library(arrow)
  library(readr)
  library(dplyr)
  library(stringr)
  library(tibble)
  library(purrr)
})

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

RAW_ROOT <- file.path("data", "raw", "NHANES")
CLEAN_ROOT <- file.path("data", "cleaned", "NHANES")
LINKS_PATH <- file.path("data", "NHANES links All.csv")

WRITE_CSV <- !identical(tolower(Sys.getenv("NHANES_WRITE_CSV", "true")), "false")
DOWNLOAD_TIMEOUT <- suppressWarnings(as.integer(Sys.getenv("NHANES_DOWNLOAD_TIMEOUT_SECONDS", "7200")))
if (is.na(DOWNLOAD_TIMEOUT) || DOWNLOAD_TIMEOUT < 60L) DOWNLOAD_TIMEOUT <- 7200L

RAW_LINKS_COPY <- file.path(RAW_ROOT, "NHANES links All.csv")
CLEAN_LINKS_COPY <- file.path(CLEAN_ROOT, "NHANES links All.csv")
RAW_ASSET_MANIFEST <- file.path(RAW_ROOT, "nhanes_asset_manifest.csv")
CLEAN_ASSET_MANIFEST <- file.path(CLEAN_ROOT, "nhanes_asset_manifest.csv")
RAW_ROW_MANIFEST <- file.path(RAW_ROOT, "nhanes_row_manifest.csv")
CLEAN_ROW_MANIFEST <- file.path(CLEAN_ROOT, "nhanes_row_manifest.csv")
RAW_RENAME_MAP <- file.path(RAW_ROOT, "filename_conflicts.csv")
CLEAN_RENAME_MAP <- file.path(CLEAN_ROOT, "filename_conflicts.csv")

DOWNLOAD_INVENTORY_RAW <- file.path(RAW_ROOT, "download_inventory.csv")
DOWNLOAD_FAILURES_RAW <- file.path(RAW_ROOT, "download_failures.csv")
CONVERSION_INVENTORY_RAW <- file.path(RAW_ROOT, "conversion_inventory.csv")
CONVERSION_FAILURES_RAW <- file.path(RAW_ROOT, "conversion_failures.csv")

DOWNLOAD_INVENTORY_CLEAN <- file.path(CLEAN_ROOT, "download_inventory.csv")
DOWNLOAD_FAILURES_CLEAN <- file.path(CLEAN_ROOT, "download_failures.csv")
CONVERSION_INVENTORY_CLEAN <- file.path(CLEAN_ROOT, "conversion_inventory.csv")
CONVERSION_FAILURES_CLEAN <- file.path(CLEAN_ROOT, "conversion_failures.csv")

RAW_SOURCES_MD <- file.path(RAW_ROOT, "SOURCES.md")
CLEAN_NOTES_MD <- file.path(CLEAN_ROOT, "NOTES.md")

dir.create(RAW_ROOT, recursive = TRUE, showWarnings = FALSE)
dir.create(CLEAN_ROOT, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

dir_ok <- function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE)

is_blank <- function(x) {
  is.na(x) | trimws(as.character(x)) == ""
}

normalize_rel_path <- function(x) {
  x <- as.character(x)
  x <- gsub("/", .Platform$file.sep, x, fixed = TRUE)
  x <- gsub("\\\\+", "\\\\", x)
  x
}

safe_name <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("[/\\:*?\"<>|]+", "_", x)
  x <- gsub("\\s+", " ", x)
  x
}

raw_folder_to_clean_folder <- function(raw_folder) {
  raw_folder <- normalize_rel_path(raw_folder)
  raw_root_norm <- normalize_rel_path(RAW_ROOT)
  clean_root_norm <- normalize_rel_path(CLEAN_ROOT)

  rel <- sub(
    paste0("^", gsub("\\\\", "\\\\\\\\", raw_root_norm), "[/\\\\]?"),
    "",
    raw_folder
  )
  rel <- sub("^[/\\\\]+", "", rel)

  if (nzchar(rel)) file.path(clean_root_norm, rel) else clean_root_norm
}

url_path_basename <- function(url) {
  url <- trimws(as.character(url))
  if (is_blank(url)) return(NA_character_)
  path <- sub("^.*://[^/]+", "", url)
  path <- sub("\\?.*$", "", path)
  path <- sub("/+$", "", path)
  nm <- basename(path)
  if (!nzchar(nm)) NA_character_ else nm
}

classify_source_url <- function(url, source_column) {
  url <- trimws(as.character(url))
  source_column <- trimws(as.character(source_column))
  url_l <- tolower(url)

  if (startsWith(url_l, "ftp://")) return("ftp_folder")
  if (grepl("\\.xpt($|\\?)", url_l)) return("data_xpt")
  if (grepl("\\.csv($|\\?)", url_l)) return("data_csv")
  if (grepl("\\.zip($|\\?)", url_l)) return("data_zip")
  if (grepl("\\.txt($|\\?)", url_l) && source_column != "data") return("code_txt")
  if (grepl("\\.txt($|\\?)", url_l)) return("data_txt")
  if (grepl("\\.dat($|\\?)", url_l)) return("data_dat")
  if (grepl("\\.sas($|\\?)", url_l)) return("code_sas")
  if (grepl("\\.pdf($|\\?)", url_l)) return("doc_pdf")
  if (!grepl("\\.[A-Za-z0-9]+($|\\?)", basename(sub("\\?.*$", "", url)))) return("ftp_folder")
  "other"
}

asset_role_from_column <- function(source_column) {
  dplyr::case_when(
    source_column == "data" ~ "data",
    source_column == "sas code" ~ "sas_code",
    source_column == "formatted sas" ~ "formatted_sas",
    TRUE ~ "other"
  )
}

with_download_timeout <- function(expr, timeout_seconds = DOWNLOAD_TIMEOUT) {
  old_timeout <- getOption("timeout")
  on.exit(options(timeout = old_timeout), add = TRUE)
  options(timeout = max(old_timeout, timeout_seconds))
  force(expr)
}

download_if_needed <- function(url, dest) {
  if (file.exists(dest) && isTRUE(file.info(dest)$size > 0)) {
    return(list(status = "exists", error = NA_character_))
  }

  tmp <- paste0(dest, ".tmp")
  if (file.exists(tmp)) unlink(tmp, force = TRUE)
  dir_ok(dirname(dest))

  ok <- tryCatch({
    with_download_timeout(
      utils::download.file(url, destfile = tmp, mode = "wb", quiet = TRUE, method = "libcurl")
    )
    file.exists(tmp) && isTRUE(file.info(tmp)$size > 0)
  }, error = function(e) {
    structure(FALSE, error_message = conditionMessage(e))
  })

  if (!isTRUE(ok)) {
    err <- attr(ok, "error_message")
    if (file.exists(tmp)) unlink(tmp, force = TRUE)
    return(list(
      status = "failed",
      error = ifelse(is.null(err) || !nzchar(err), "Download failed.", err)
    ))
  }

  moved <- file.rename(tmp, dest)
  if (!moved) {
    copied <- file.copy(tmp, dest, overwrite = TRUE)
    unlink(tmp, force = TRUE)
    if (!copied) {
      return(list(status = "failed", error = "Could not move downloaded file into place."))
    }
  }

  list(status = "downloaded", error = NA_character_)
}

resolve_asset_file_names <- function(asset_manifest) {
  if (nrow(asset_manifest) == 0) {
    return(asset_manifest %>% mutate(original_file_name = character(), renamed_due_to_conflict = logical()))
  }

  asset_manifest <- asset_manifest %>%
    mutate(
      original_file_name = .data$file_name,
      file_name = as.character(.data$file_name),
      source_suffix = dplyr::case_when(
        .data$source_column == "data" ~ "data",
        .data$source_column == "sas code" ~ "sas",
        .data$source_column == "formatted sas" ~ "formatted_sas",
        TRUE ~ "asset"
      )
    ) %>%
    group_by(.data$local_folder, .data$file_name) %>%
    mutate(conflict_n = dplyr::n()) %>%
    ungroup()

  take <- asset_manifest$conflict_n > 1
  if (any(take)) {
    ext <- tools::file_ext(asset_manifest$file_name)
    stem <- tools::file_path_sans_ext(asset_manifest$file_name)
    asset_manifest$file_name[take] <- ifelse(
      nzchar(ext[take]),
      paste0(stem[take], "_", asset_manifest$source_suffix[take], "_row", asset_manifest$source_row_id[take], ".", ext[take]),
      paste0(stem[take], "_", asset_manifest$source_suffix[take], "_row", asset_manifest$source_row_id[take])
    )
  }

  asset_manifest %>%
    mutate(
      renamed_due_to_conflict = .data$file_name != .data$original_file_name
    ) %>%
    select(-all_of(c("source_suffix", "conflict_n")))
}

write_named_sidecars <- function(df, out_dir, file_stem, write_csv = TRUE) {
  dir_ok(out_dir)

  parquet_path <- file.path(out_dir, paste0(file_stem, ".parquet"))
  dta_path <- file.path(out_dir, paste0(file_stem, ".dta"))
  csv_path <- file.path(out_dir, paste0(file_stem, ".csv"))

  arrow::write_parquet(df, parquet_path)
  haven::write_dta(df, dta_path)

  if (isTRUE(write_csv)) {
    readr::write_csv(df, csv_path)
  }

  list(
    parquet_path = parquet_path,
    dta_path = dta_path,
    csv_path = if (isTRUE(write_csv)) csv_path else NA_character_
  )
}

convert_cleanable_asset <- function(asset_path, clean_dir, write_csv = TRUE) {
  ext <- tolower(tools::file_ext(asset_path))
  stem <- tools::file_path_sans_ext(basename(asset_path))

  if (ext == "xpt") {
    df <- haven::read_xpt(asset_path)
    out <- write_named_sidecars(df, clean_dir, stem, write_csv = write_csv)
    return(c(list(status = "converted", rows = nrow(df), cols = ncol(df)), out))
  }

  if (ext == "csv") {
    df <- readr::read_csv(asset_path, show_col_types = FALSE, progress = FALSE)
    out <- write_named_sidecars(df, clean_dir, stem, write_csv = write_csv)
    return(c(list(status = "converted", rows = nrow(df), cols = ncol(df)), out))
  }

  list(
    status = "unsupported",
    rows = NA_integer_,
    cols = NA_integer_,
    parquet_path = NA_character_,
    dta_path = NA_character_,
    csv_path = NA_character_
  )
}

parse_sas_length_map <- function(lines) {
  start_idx <- which(grepl("^\\s*length\\b", lines, ignore.case = TRUE))[1]
  if (is.na(start_idx)) return(tibble::tibble(var = character(), is_char = logical(), width = integer()))

  block <- character(0)
  for (i in seq.int(start_idx, length(lines))) {
    line <- lines[[i]]
    block <- c(block, line)
    if (grepl(";", line, fixed = TRUE)) break
  }

  text <- paste(block, collapse = " ")
  text <- sub("^\\s*length\\b", "", text, ignore.case = TRUE)
  text <- sub(";.*$", "", text)
  hits <- stringr::str_match_all(text, "([A-Za-z_][A-Za-z0-9_]*)\\s+(\\$?)(\\d+)")[[1]]
  if (nrow(hits) == 0) return(tibble::tibble(var = character(), is_char = logical(), width = integer()))

  tibble::tibble(
    var = hits[, 2],
    is_char = hits[, 3] == "$",
    width = as.integer(hits[, 4])
  ) %>%
    distinct(.data$var, .keep_all = TRUE)
}

parse_sas_input_layout <- function(lines, length_map = NULL) {
  start_idx <- which(grepl("^\\s*input\\b", lines, ignore.case = TRUE))[1]
  if (is.na(start_idx)) {
    return(tibble::tibble(var = character(), start = integer(), end = integer(), is_char = logical()))
  }

  block <- character(0)
  first <- TRUE
  for (i in seq.int(start_idx, length(lines))) {
    line <- lines[[i]]
    if (first) {
      line <- sub("^\\s*input\\b", "", line, ignore.case = TRUE)
      first <- FALSE
    }
    block <- c(block, line)
    if (grepl(";", line, fixed = TRUE)) break
  }

  text <- paste(block, collapse = "\n")
  text <- sub(";\\s*$", "", text)

  at_hits <- stringr::str_match_all(
    text,
    "@\\s*(\\d+)\\s+([A-Za-z_][A-Za-z0-9_]*)\\s+(\\$?[A-Za-z]+)?(\\d+)\\."
  )[[1]]

  if (nrow(at_hits) > 0) {
    out <- tibble::tibble(
      start = as.integer(at_hits[, 2]),
      var = at_hits[, 3],
      width = as.integer(at_hits[, 5]),
      is_char = grepl("^\\$", ifelse(is.na(at_hits[, 4]), "", at_hits[, 4]))
    ) %>%
      mutate(end = .data$start + .data$width - 1L) %>%
      select("var", "start", "end", "is_char")

    if (!is.null(length_map) && nrow(length_map) > 0) {
      out <- out %>%
        left_join(length_map %>% select("var", len_is_char = "is_char"), by = "var") %>%
        mutate(is_char = dplyr::coalesce(.data$len_is_char, .data$is_char)) %>%
        select("var", "start", "end", "is_char")
    }

    return(out %>% distinct(.data$var, .keep_all = TRUE))
  }

  line_hits <- stringr::str_match_all(
    text,
    "(?m)^\\s*([A-Za-z_][A-Za-z0-9_]*)\\s+(\\d+)(?:-(\\d+))?\\s*$"
  )[[1]]

  if (nrow(line_hits) == 0) {
    return(tibble::tibble(var = character(), start = integer(), end = integer(), is_char = logical()))
  }

  out <- tibble::tibble(
    var = line_hits[, 2],
    start = as.integer(line_hits[, 3]),
    end = as.integer(ifelse(is.na(line_hits[, 4]) | line_hits[, 4] == "", line_hits[, 3], line_hits[, 4]))
  )

  if (!is.null(length_map) && nrow(length_map) > 0) {
    out <- out %>% left_join(length_map %>% select("var", "is_char"), by = "var")
  } else {
    out$is_char <- FALSE
  }

  out %>% distinct(.data$var, .keep_all = TRUE)
}

parse_sas_labels <- function(lines) {
  start_idx <- which(grepl("^\\s*label\\b", lines, ignore.case = TRUE))[1]
  if (is.na(start_idx)) return(tibble::tibble(var = character(), label = character()))

  block <- character(0)
  first <- TRUE
  for (i in seq.int(start_idx, length(lines))) {
    line <- lines[[i]]
    if (first) {
      line <- sub("^\\s*label\\b", "", line, ignore.case = TRUE)
      first <- FALSE
    }
    block <- c(block, line)
    if (grepl(";", line, fixed = TRUE)) break
  }

  text <- paste(block, collapse = "\n")
  text <- sub(";\\s*$", "", text)
  hits <- stringr::str_match_all(text, "([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*'([^']*)'")[[1]]
  if (nrow(hits) == 0) return(tibble::tibble(var = character(), label = character()))

  tibble::tibble(
    var = hits[, 2],
    label = hits[, 3]
  ) %>%
    distinct(.data$var, .keep_all = TRUE)
}

read_fixed_width_from_sas <- function(txt_path, sas_path) {
  lines <- readLines(sas_path, warn = FALSE, encoding = "UTF-8")
  length_map <- parse_sas_length_map(lines)
  layout <- parse_sas_input_layout(lines, length_map)
  if (nrow(layout) == 0) stop("Could not parse INPUT layout from SAS script.")

  fwf <- readr::fwf_positions(
    start = layout$start,
    end = layout$end,
    col_names = layout$var
  )

  df <- suppressMessages(
    readr::read_fwf(
      txt_path,
      fwf,
      col_types = readr::cols(.default = readr::col_character()),
      trim_ws = FALSE,
      progress = FALSE,
      show_col_types = FALSE,
      na = c("")
    )
  )

  layout <- layout %>% mutate(is_char = dplyr::coalesce(.data$is_char, FALSE))

  numeric_vars <- layout %>%
    filter(!.data$is_char, .data$var %in% names(df)) %>%
    pull(.data$var)

  if (length(numeric_vars) > 0) {
    for (nm in numeric_vars) {
      x <- trimws(df[[nm]])
      x[x %in% c("", ".")] <- NA_character_
      df[[nm]] <- suppressWarnings(readr::parse_double(x, na = c("", ".")))
    }
  }

  labels <- parse_sas_labels(lines)
  if (nrow(labels) > 0) {
    for (i in seq_len(nrow(labels))) {
      nm <- labels$var[[i]]
      if (nm %in% names(df)) attr(df[[nm]], "label") <- labels$label[[i]]
    }
  }

  list(data = df, labels = labels)
}

build_asset_manifest <- function(link_rows) {
  asset_cols <- c("data", "sas code", "formatted sas")

  expanded <- bind_rows(lapply(asset_cols, function(col_nm) {
    tibble::tibble(
      source_row_id = link_rows$source_row_id,
      type = link_rows$type,
      wave = link_rows$wave,
      descriptor = link_rows$descriptor,
      final_url_before_data = link_rows$final_url_before_data,
      first_column_nhanes3 = link_rows$first_column_nhanes3,
      source_column = col_nm,
      source_url = link_rows[[col_nm]]
    )
  })) %>%
    mutate(
      source_url = trimws(as.character(.data$source_url)),
      source_column = as.character(.data$source_column),
      asset_role = asset_role_from_column(.data$source_column),
      link_type = mapply(classify_source_url, .data$source_url, .data$source_column),
      file_name = vapply(.data$source_url, url_path_basename, character(1)),
      wave_folder = safe_name(.data$wave),
      local_folder = file.path(RAW_ROOT, .data$wave_folder)
    ) %>%
    filter(!is_blank(.data$source_url)) %>%
    filter(!is.na(.data$file_name), nzchar(.data$file_name)) %>%
    filter(.data$link_type != "ftp_folder")

  expanded %>%
    arrange(.data$wave, .data$source_row_id, .data$source_column, .data$source_url) %>%
    group_by(.data$wave, .data$source_column, .data$source_url) %>%
    slice(1) %>%
    ungroup() %>%
    resolve_asset_file_names() %>%
    mutate(
      local_folder = normalize_rel_path(.data$local_folder),
      raw_dest_path = file.path(.data$local_folder, .data$file_name),
      clean_folder = vapply(.data$local_folder, raw_folder_to_clean_folder, character(1))
    )
}

build_row_manifest <- function(link_rows, asset_manifest) {
  data_lookup <- asset_manifest %>%
    filter(.data$source_column == "data") %>%
    select(.data$wave, data_source_url = .data$source_url, data_file_name = .data$file_name, data_dest_path = .data$raw_dest_path)

  sas_lookup <- asset_manifest %>%
    filter(.data$source_column == "sas code") %>%
    select(.data$wave, sas_source_url = .data$source_url, sas_file_name = .data$file_name, sas_dest_path = .data$raw_dest_path)

  formatted_lookup <- asset_manifest %>%
    filter(.data$source_column == "formatted sas") %>%
    select(.data$wave, formatted_sas_source_url = .data$source_url, formatted_sas_file_name = .data$file_name, formatted_sas_dest_path = .data$raw_dest_path)

  link_rows %>%
    left_join(data_lookup, by = c("wave", "data_source_url")) %>%
    left_join(sas_lookup, by = c("wave", "sas_source_url")) %>%
    left_join(formatted_lookup, by = c("wave", "formatted_sas_source_url")) %>%
    mutate(
      wave_folder = safe_name(.data$wave),
      raw_folder = normalize_rel_path(file.path(RAW_ROOT, .data$wave_folder)),
      clean_folder = vapply(.data$raw_folder, raw_folder_to_clean_folder, character(1)),
      preferred_script_path = dplyr::coalesce(.data$sas_dest_path, .data$formatted_sas_dest_path),
      preferred_script_kind = dplyr::case_when(
        !is.na(.data$sas_dest_path) ~ "sas code",
        !is.na(.data$formatted_sas_dest_path) ~ "formatted sas",
        TRUE ~ NA_character_
      )
    )
}

write_root_notes <- function(link_rows, asset_manifest, download_inventory, conversion_inventory, rename_map) {
  downloaded_n <- sum(download_inventory$download_status %in% c("downloaded", "exists"), na.rm = TRUE)
  failed_n <- sum(download_inventory$download_status == "failed", na.rm = TRUE)
  converted_n <- sum(conversion_inventory$conversion_status == "converted", na.rm = TRUE)
  conversion_fail_n <- sum(conversion_inventory$conversion_status == "failed", na.rm = TRUE)
  renamed_n <- nrow(rename_map)

  writeLines(
    c(
      "NHANES files downloaded from `data/NHANES links All.csv`.",
      paste0("Links source: ", LINKS_PATH),
      paste0("Generated on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
      paste0("Source CSV rows: ", nrow(link_rows)),
      paste0("Expanded downloadable assets: ", nrow(asset_manifest)),
      paste0("Downloaded or already present: ", downloaded_n),
      paste0("Download failures: ", failed_n),
      paste0("Filename conflict renames: ", renamed_n),
      paste0("Root asset manifest: ", RAW_ASSET_MANIFEST),
      paste0("Root row manifest: ", RAW_ROW_MANIFEST),
      paste0("Rename map: ", RAW_RENAME_MAP)
    ),
    RAW_SOURCES_MD
  )

  writeLines(
    c(
      "# NHANES Notes",
      "",
      sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
      "",
      "- The build uses `data/NHANES links All.csv` as the source of truth.",
      "- Raw assets are downloaded into `data/raw/NHANES/<wave>`.",
      "- Cleaned outputs mirror the same relative structure under `data/cleaned/NHANES/<wave>`.",
      "- The script downloads data files, SAS/setup files, and formatted SAS files, excluding FTP-style folder entries.",
      "- Readable raw `XPT` and `CSV` files are converted directly to `.parquet`, `.dta`, and optional `.csv` sidecars.",
      "- Fixed-width `TXT` and `DAT` files are converted using the SAS/setup file from the same source row whenever available.",
      paste0("- Source CSV rows: ", nrow(link_rows), "."),
      paste0("- Downloadable assets: ", nrow(asset_manifest), "."),
      paste0("- Converted files: ", converted_n, "."),
      paste0("- Conversion failures: ", conversion_fail_n, "."),
      paste0("- Download inventory: `", DOWNLOAD_INVENTORY_CLEAN, "`."),
      paste0("- Conversion inventory: `", CONVERSION_INVENTORY_CLEAN, "`."),
      paste0("- Download failures: `", DOWNLOAD_FAILURES_CLEAN, "`."),
      paste0("- Conversion failures: `", CONVERSION_FAILURES_CLEAN, "`."),
      paste0("- Filename conflicts: `", CLEAN_RENAME_MAP, "`."),
      paste0("- Asset manifest: `", CLEAN_ASSET_MANIFEST, "`."),
      paste0("- Row manifest: `", CLEAN_ROW_MANIFEST, "`.")
    ),
    CLEAN_NOTES_MD
  )
}

# ------------------------------------------------------------------------------
# LINKS INPUT
# ------------------------------------------------------------------------------

if (!file.exists(LINKS_PATH)) {
  stop(sprintf("Links file not found: %s", LINKS_PATH), call. = FALSE)
}

message("Reading NHANES links CSV...")
link_rows <- readr::read_csv(LINKS_PATH, show_col_types = FALSE, progress = FALSE) %>%
  mutate(across(where(is.character), as.character))

required_cols <- c(
  "type",
  "wave",
  "descriptor",
  "data",
  "sas code",
  "formatted sas",
  "final url before data",
  "first column (NHANES III)"
)

missing_cols <- setdiff(required_cols, names(link_rows))
if (length(missing_cols) > 0) {
  stop(
    sprintf("Links file is missing required columns: %s", paste(missing_cols, collapse = ", ")),
    call. = FALSE
  )
}

link_rows <- link_rows %>%
  transmute(
    source_row_id = dplyr::row_number(),
    type = trimws(as.character(.data$type)),
    wave = trimws(as.character(.data$wave)),
    descriptor = trimws(as.character(.data$descriptor)),
    data_source_url = trimws(as.character(.data$data)),
    sas_source_url = trimws(as.character(.data$`sas code`)),
    formatted_sas_source_url = trimws(as.character(.data$`formatted sas`)),
    final_url_before_data = trimws(as.character(.data$`final url before data`)),
    first_column_nhanes3 = trimws(as.character(.data$`first column (NHANES III)`)),
    data = .data$data_source_url,
    `sas code` = .data$sas_source_url,
    `formatted sas` = .data$formatted_sas_source_url
  )

asset_manifest <- build_asset_manifest(link_rows)
row_manifest <- build_row_manifest(link_rows, asset_manifest)

rename_map <- asset_manifest %>%
  filter(.data$renamed_due_to_conflict) %>%
  select(
    .data$source_row_id,
    .data$wave,
    .data$source_column,
    .data$source_url,
    .data$original_file_name,
    .data$file_name,
    .data$local_folder,
    .data$raw_dest_path,
    .data$descriptor
  )

links_copy <- readr::read_csv(LINKS_PATH, show_col_types = FALSE, progress = FALSE)
readr::write_csv(links_copy, RAW_LINKS_COPY)
readr::write_csv(links_copy, CLEAN_LINKS_COPY)
readr::write_csv(asset_manifest, RAW_ASSET_MANIFEST)
readr::write_csv(asset_manifest, CLEAN_ASSET_MANIFEST)
readr::write_csv(row_manifest, RAW_ROW_MANIFEST)
readr::write_csv(row_manifest, CLEAN_ROW_MANIFEST)
readr::write_csv(rename_map, RAW_RENAME_MAP)
readr::write_csv(rename_map, CLEAN_RENAME_MAP)

# ------------------------------------------------------------------------------
# DOWNLOAD
# ------------------------------------------------------------------------------

message("Downloading NHANES assets...")

download_rows <- vector("list", nrow(asset_manifest))

for (i in seq_len(nrow(asset_manifest))) {
  r <- asset_manifest[i, , drop = FALSE]
  dest <- r$raw_dest_path[[1]]
  dir_ok(dirname(dest))

  message(sprintf("Download [%d/%d] %s", i, nrow(asset_manifest), basename(dest)))
  dl <- download_if_needed(r$source_url[[1]], dest)

  download_rows[[i]] <- tibble::tibble(
    manifest_row_id = i,
    source_row_id = r$source_row_id[[1]],
    asset_role = r$asset_role[[1]],
    source_column = r$source_column[[1]],
    type = r$type[[1]],
    wave = r$wave[[1]],
    descriptor = r$descriptor[[1]],
    source_url = r$source_url[[1]],
    final_url_before_data = r$final_url_before_data[[1]],
    first_column_nhanes3 = r$first_column_nhanes3[[1]],
    local_folder = r$local_folder[[1]],
    clean_folder = r$clean_folder[[1]],
    file_name = r$file_name[[1]],
    raw_dest_path = dest,
    link_type = r$link_type[[1]],
    download_status = dl$status,
    local_exists = file.exists(dest),
    local_size = if (file.exists(dest)) as.numeric(file.info(dest)$size) else NA_real_,
    error_message = dl$error,
    downloaded_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
}

download_inventory <- bind_rows(download_rows)
download_failures <- download_inventory %>% filter(.data$download_status == "failed")

readr::write_csv(download_inventory, DOWNLOAD_INVENTORY_RAW)
readr::write_csv(download_inventory, DOWNLOAD_INVENTORY_CLEAN)
readr::write_csv(download_failures, DOWNLOAD_FAILURES_RAW)
readr::write_csv(download_failures, DOWNLOAD_FAILURES_CLEAN)

# ------------------------------------------------------------------------------
# CONVERSION
# ------------------------------------------------------------------------------

message("Converting NHANES data files...")

row_manifest <- row_manifest %>%
  mutate(
    data_exists = !is.na(.data$data_dest_path) & file.exists(.data$data_dest_path),
    preferred_script_exists = !is.na(.data$preferred_script_path) & file.exists(.data$preferred_script_path),
    clean_folder = vapply(.data$raw_folder, raw_folder_to_clean_folder, character(1))
  )

direct_candidates <- row_manifest %>%
  filter(.data$data_exists) %>%
  mutate(data_ext = tolower(tools::file_ext(.data$data_dest_path))) %>%
  filter(.data$data_ext %in% c("xpt", "csv"))

direct_conversion_rows <- vector("list", nrow(direct_candidates))

if (nrow(direct_candidates) > 0) {
  for (i in seq_len(nrow(direct_candidates))) {
    r <- direct_candidates[i, , drop = FALSE]
    message(sprintf("Direct conversion [%d/%d] %s", i, nrow(direct_candidates), basename(r$data_dest_path[[1]])))

    conv <- tryCatch(
      convert_cleanable_asset(r$data_dest_path[[1]], r$clean_folder[[1]], write_csv = WRITE_CSV),
      error = function(e) {
        list(
          status = "failed",
          rows = NA_integer_,
          cols = NA_integer_,
          parquet_path = NA_character_,
          dta_path = NA_character_,
          csv_path = NA_character_,
          error = conditionMessage(e)
        )
      }
    )

    direct_conversion_rows[[i]] <- tibble::tibble(
      source_row_id = r$source_row_id[[1]],
      wave = r$wave[[1]],
      descriptor = r$descriptor[[1]],
      data_path = r$data_dest_path[[1]],
      script_path = NA_character_,
      clean_dir = r$clean_folder[[1]],
      parquet_path = conv$parquet_path,
      dta_path = conv$dta_path,
      csv_path = conv$csv_path,
      variables_path = NA_character_,
      conversion_type = "direct",
      conversion_status = conv$status,
      rows = conv$rows,
      cols = conv$cols,
      error_message = if ("error" %in% names(conv)) conv$error else NA_character_
    )
  }
}

direct_conversion_inventory <- bind_rows(direct_conversion_rows)

fixed_width_candidates <- row_manifest %>%
  filter(.data$data_exists, .data$preferred_script_exists) %>%
  mutate(data_ext = tolower(tools::file_ext(.data$data_dest_path))) %>%
  filter(.data$data_ext %in% c("txt", "dat"))

fixed_width_rows <- vector("list", nrow(fixed_width_candidates))

if (nrow(fixed_width_candidates) > 0) {
  for (i in seq_len(nrow(fixed_width_candidates))) {
    r <- fixed_width_candidates[i, , drop = FALSE]
    message(sprintf("Fixed-width conversion [%d/%d] %s", i, nrow(fixed_width_candidates), basename(r$data_dest_path[[1]])))

    conv <- tryCatch({
      parsed <- read_fixed_width_from_sas(r$data_dest_path[[1]], r$preferred_script_path[[1]])
      out_paths <- write_named_sidecars(parsed$data, r$clean_folder[[1]], tools::file_path_sans_ext(basename(r$data_dest_path[[1]])), write_csv = WRITE_CSV)
      variables_path <- NA_character_
      if (nrow(parsed$labels) > 0) {
        variables_path <- file.path(r$clean_folder[[1]], paste0(tools::file_path_sans_ext(basename(r$data_dest_path[[1]])), "_variables.csv"))
        readr::write_csv(parsed$labels, variables_path)
      }
      list(
        status = "converted",
        rows = nrow(parsed$data),
        cols = ncol(parsed$data),
        parquet_path = out_paths$parquet_path,
        dta_path = out_paths$dta_path,
        csv_path = out_paths$csv_path,
        variables_path = variables_path,
        error = NA_character_
      )
    }, error = function(e) {
      list(
        status = "failed",
        rows = NA_integer_,
        cols = NA_integer_,
        parquet_path = NA_character_,
        dta_path = NA_character_,
        csv_path = NA_character_,
        variables_path = NA_character_,
        error = conditionMessage(e)
      )
    })

    fixed_width_rows[[i]] <- tibble::tibble(
      source_row_id = r$source_row_id[[1]],
      wave = r$wave[[1]],
      descriptor = r$descriptor[[1]],
      data_path = r$data_dest_path[[1]],
      script_path = r$preferred_script_path[[1]],
      clean_dir = r$clean_folder[[1]],
      parquet_path = conv$parquet_path,
      dta_path = conv$dta_path,
      csv_path = conv$csv_path,
      variables_path = conv$variables_path,
      conversion_type = "fixed_width",
      conversion_status = conv$status,
      rows = conv$rows,
      cols = conv$cols,
      error_message = conv$error
    )
  }
}

fixed_width_inventory <- bind_rows(fixed_width_rows)

conversion_inventory <- bind_rows(direct_conversion_inventory, fixed_width_inventory) %>%
  mutate(across(where(is.character), as.character))

conversion_failures <- conversion_inventory %>%
  filter(.data$conversion_status == "failed")

readr::write_csv(conversion_inventory, CONVERSION_INVENTORY_RAW)
readr::write_csv(conversion_inventory, CONVERSION_INVENTORY_CLEAN)
readr::write_csv(conversion_failures, CONVERSION_FAILURES_RAW)
readr::write_csv(conversion_failures, CONVERSION_FAILURES_CLEAN)

# ------------------------------------------------------------------------------
# NOTES
# ------------------------------------------------------------------------------

write_root_notes(link_rows, asset_manifest, download_inventory, conversion_inventory, rename_map)

message("Wrote: ", RAW_LINKS_COPY)
message("Wrote: ", CLEAN_LINKS_COPY)
message("Wrote: ", RAW_ASSET_MANIFEST)
message("Wrote: ", CLEAN_ASSET_MANIFEST)
message("Wrote: ", RAW_ROW_MANIFEST)
message("Wrote: ", CLEAN_ROW_MANIFEST)
message("Wrote: ", RAW_RENAME_MAP)
message("Wrote: ", CLEAN_RENAME_MAP)
message("Wrote: ", DOWNLOAD_INVENTORY_RAW)
message("Wrote: ", DOWNLOAD_FAILURES_RAW)
message("Wrote: ", CONVERSION_INVENTORY_CLEAN)
message("Wrote: ", CONVERSION_FAILURES_CLEAN)
message("Wrote: ", RAW_SOURCES_MD)
message("Wrote: ", CLEAN_NOTES_MD)
message("Done.")
