# R/build_nhanes.R
#
# PURPOSE
# -------
# Build NHANES from a reviewed download plan.
#
# This script:
#   1. Reads `data/cleaned/NHANES/nhanes_download_plan.csv`
#   2. Downloads files into the raw folders listed in that plan
#   3. Converts readable files into `.parquet`, `.dta`, and optional `.csv`
#      inside the mirrored cleaned folder structure
#   4. Writes inventories for downloads, conversions, and failures
#   5. Writes lightweight notes files at the NHANES root
#
# The plan CSV is treated as the source of truth for local folder structure.

source("R/utils.R")

suppressPackageStartupMessages({
  library(haven)
  library(arrow)
  library(readr)
  library(dplyr)
  library(stringr)
  library(tibble)
})

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

RAW_ROOT <- file.path("data", "raw", "NHANES")
CLEAN_ROOT <- file.path("data", "cleaned", "NHANES")
PLAN_PATH <- file.path(CLEAN_ROOT, "nhanes_download_plan.csv")

WRITE_CSV <- !identical(tolower(Sys.getenv("NHANES_WRITE_CSV", "true")), "false")
DOWNLOAD_TIMEOUT <- suppressWarnings(as.integer(Sys.getenv("NHANES_DOWNLOAD_TIMEOUT_SECONDS", "7200")))
if (is.na(DOWNLOAD_TIMEOUT) || DOWNLOAD_TIMEOUT < 60L) DOWNLOAD_TIMEOUT <- 7200L

RAW_PLAN_COPY <- file.path(RAW_ROOT, "nhanes_download_plan.csv")
CLEAN_PLAN_COPY <- file.path(CLEAN_ROOT, "nhanes_download_plan_used.csv")

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

prefer_source_url <- function(x) {
  x <- trimws(as.character(x))
  dplyr::case_when(
    startsWith(tolower(x), "https://") ~ 1L,
    startsWith(tolower(x), "http://") ~ 2L,
    TRUE ~ 3L
  )
}

with_download_timeout <- function(expr, timeout_seconds = DOWNLOAD_TIMEOUT) {
  old_timeout <- getOption("timeout")
  on.exit(options(timeout = old_timeout), add = TRUE)
  options(timeout = max(old_timeout, timeout_seconds))
  force(expr)
}

make_unique_file_names <- function(df) {
  if (nrow(df) == 0) return(df)

  df <- df %>%
    group_by(.data$local_folder, .data$file_name) %>%
    mutate(dup_n = dplyr::n()) %>%
    ungroup()

  if (!any(df$dup_n > 1)) {
    return(df %>% select(-.data$dup_n))
  }

  ext <- tools::file_ext(df$file_name)
  stem <- tools::file_path_sans_ext(df$file_name)
  suffix <- ifelse(
    is_blank(df$component_or_release),
    NA_character_,
    safe_name(gsub("[/\\\\]+", "_", df$component_or_release))
  )

  needs_suffix <- df$dup_n > 1 & !is.na(suffix) & nzchar(suffix)
  df$file_name[needs_suffix] <- ifelse(
    nzchar(ext[needs_suffix]),
    paste0(stem[needs_suffix], "_", suffix[needs_suffix], ".", ext[needs_suffix]),
    paste0(stem[needs_suffix], "_", suffix[needs_suffix])
  )

  df <- df %>%
    group_by(.data$local_folder, .data$file_name) %>%
    mutate(dup_n = dplyr::n()) %>%
    ungroup()

  if (any(df$dup_n > 1)) {
    df <- df %>%
      group_by(.data$local_folder, .data$file_name) %>%
      mutate(
        dup_index = dplyr::row_number(),
        dup_n = dplyr::n()
      ) %>%
      ungroup()

    ext <- tools::file_ext(df$file_name)
    stem <- tools::file_path_sans_ext(df$file_name)
    needs_index <- df$dup_n > 1
    df$file_name[needs_index] <- ifelse(
      nzchar(ext[needs_index]),
      paste0(stem[needs_index], "_dup", df$dup_index[needs_index], ".", ext[needs_index]),
      paste0(stem[needs_index], "_dup", df$dup_index[needs_index])
    )
  }

  df %>% select(-dplyr::any_of(c("dup_n", "dup_index")))
}

resolve_plan_duplicates <- function(plan) {
  if (nrow(plan) == 0) return(plan)

  plan <- plan %>%
    mutate(
      local_folder_key = tolower(.data$local_folder),
      file_name_key = tolower(.data$file_name),
      source_rank = prefer_source_url(.data$source_url)
    ) %>%
    arrange(.data$local_folder_key, .data$file_name_key, .data$source_rank, .data$source_url) %>%
    group_by(.data$local_folder_key, .data$file_name_key) %>%
    mutate(
      normalized_url = tolower(gsub("^http://", "https://", .data$source_url)),
      normalized_url = gsub("%20", " ", .data$normalized_url, fixed = TRUE)
    ) %>%
    mutate(is_same_target_variant = dplyr::n_distinct(.data$normalized_url) == 1L) %>%
    filter(!(dplyr::row_number() > 1L & .data$is_same_target_variant)) %>%
    ungroup() %>%
    select(-all_of(c("local_folder_key", "file_name_key", "source_rank", "normalized_url", "is_same_target_variant")))

  make_unique_file_names(plan)
}

is_known_bad_source_url <- function(url, link_type = NA_character_) {
  url <- trimws(as.character(url))
  link_type <- trimws(as.character(link_type))

  startsWith(tolower(url), "ftp://") &&
    identical(tolower(link_type), "other")
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
    distinct(var, .keep_all = TRUE)
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

    return(out %>% distinct(var, .keep_all = TRUE))
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

  out %>% distinct(var, .keep_all = TRUE)
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
    distinct(var, .keep_all = TRUE)
}

infer_sas_infile_target <- function(lines) {
  infile_hit <- stringr::str_match(
    paste(lines, collapse = "\n"),
    "(?i)infile\\s+['\"]([^'\"]+\\.(txt|dat))['\"]"
  )
  if (!is.na(infile_hit[1, 2]) && nzchar(infile_hit[1, 2])) {
    return(basename(infile_hit[1, 2]))
  }

  filename_hit <- stringr::str_match(
    paste(lines, collapse = "\n"),
    "(?i)filename\\s+(?:\\w+\\s+)?in\\s+['\"]([^'\"]+\\.(txt|dat))['\"]"
  )
  if (!is.na(filename_hit[1, 2]) && nzchar(filename_hit[1, 2])) {
    return(basename(filename_hit[1, 2]))
  }

  NA_character_
}

nhefs_script_txt_map <- function(script_name) {
  mapping <- c(
    "vitl.inputs.labels.txt" = "n92vitl.txt",
    "mort.inputs.labels.txt" = "N92mort.txt",
    "intv92.inputs.labels.txt" = "N92int.txt",
    "intv87.inputs.labels.txt" = "n87int.txt",
    "intv86.inputs.labels.txt" = "n86int.txt",
    "intv82.inputs.labels.txt" = "N82int.txt",
    "hcfs92.inputs.labels.txt" = "n92hcfs.txt",
    "hcfs87.inputs.labels.txt" = "n87hcfs.txt",
    "hcfs86.inputs.labels.txt" = "n86hcfs.txt",
    "hcfs82.inputs.labels.txt" = "n82hcfs.txt",
    "hcfssup.inputs.labels.txt" = "n92hcfsp.txt"
  )

  unname(mapping[[tolower(basename(script_name))]])
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

convert_fixed_width_scripts <- function(raw_dir, clean_dir, survey_label, write_csv = TRUE) {
  files <- list.files(raw_dir, recursive = TRUE, full.names = TRUE)
  files <- files[file.info(files)$isdir %in% FALSE]
  scripts <- files[grepl("\\.sas$|\\.inputs\\.labels\\.txt$", files, ignore.case = TRUE)]

  if (length(scripts) == 0) {
    return(tibble::tibble())
  }

  out <- vector("list", length(scripts))

  for (i in seq_along(scripts)) {
    script_path <- scripts[[i]]
    lines <- tryCatch(readLines(script_path, warn = FALSE, encoding = "UTF-8"), error = function(e) character())
    target_name <- infer_sas_infile_target(lines)

    if ((is.na(target_name) || !nzchar(target_name)) && identical(tolower(survey_label), "nhefs")) {
      target_name <- nhefs_script_txt_map(script_path)
    }

    result <- list(
      survey = survey_label,
      raw_dir = raw_dir,
      script_path = script_path,
      raw_data_path = NA_character_,
      clean_dir = clean_dir,
      parquet_path = NA_character_,
      dta_path = NA_character_,
      csv_path = NA_character_,
      variables_path = NA_character_,
      conversion_status = "skipped",
      rows = NA_integer_,
      cols = NA_integer_,
      error_message = NA_character_
    )

    if (is.na(target_name) || !nzchar(target_name)) {
      result$error_message <- "Could not infer target data file from script."
      out[[i]] <- tibble::as_tibble(result)
      next
    }

    candidates <- files[tolower(basename(files)) == tolower(target_name)]
    if (length(candidates) == 0) {
      result$error_message <- paste0("Target data file not found: ", target_name)
      out[[i]] <- tibble::as_tibble(result)
      next
    }

    txt_path <- candidates[[1]]
    stem <- tools::file_path_sans_ext(basename(txt_path))
    result$raw_data_path <- txt_path

    conv <- tryCatch({
      parsed <- read_fixed_width_from_sas(txt_path, script_path)
      out_paths <- write_named_sidecars(parsed$data, clean_dir, stem, write_csv = write_csv)
      variables_path <- NA_character_
      if (nrow(parsed$labels) > 0) {
        variables_path <- file.path(clean_dir, paste0(stem, "_variables.csv"))
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

    result$parquet_path <- conv$parquet_path
    result$dta_path <- conv$dta_path
    result$csv_path <- conv$csv_path
    result$variables_path <- conv$variables_path
    result$conversion_status <- conv$status
    result$rows <- conv$rows
    result$cols <- conv$cols
    result$error_message <- conv$error

    out[[i]] <- tibble::as_tibble(result)
  }

  bind_rows(out)
}

write_root_notes <- function(plan, download_inventory, conversion_inventory) {
  downloaded_n <- sum(download_inventory$download_status %in% c("downloaded", "exists"), na.rm = TRUE)
  failed_n <- sum(download_inventory$download_status == "failed", na.rm = TRUE)
  converted_n <- sum(conversion_inventory$conversion_status == "converted", na.rm = TRUE)
  conversion_fail_n <- sum(conversion_inventory$conversion_status == "failed", na.rm = TRUE)

  writeLines(
    c(
      "NHANES files downloaded from sources listed in `nhanes_download_plan.csv`.",
      paste0("Plan source: ", PLAN_PATH),
      paste0("Generated on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
      paste0("Planned files: ", nrow(plan)),
      paste0("Downloaded or already present: ", downloaded_n),
      paste0("Download failures: ", failed_n),
      paste0("Root download inventory: ", DOWNLOAD_INVENTORY_RAW),
      paste0("Root download failures: ", DOWNLOAD_FAILURES_RAW),
      "The plan CSV is copied into this folder for reference."
    ),
    RAW_SOURCES_MD
  )

  writeLines(
    c(
      "# NHANES Notes",
      "",
      sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
      "",
      "- The build uses `nhanes_download_plan.csv` as the single source of truth for local folder structure.",
      "- Raw files are downloaded into `data/raw/NHANES` using the `local_folder` and `file_name` columns from that plan.",
      "- Cleaned outputs mirror the same relative folder structure under `data/cleaned/NHANES`.",
      "- Readable raw `XPT` and `CSV` files are converted to `.parquet`, `.dta`, and optional `.csv` sidecars.",
      "- Fixed-width text files are converted when a readable SAS/setup script is available.",
      paste0("- Planned files: ", nrow(plan), "."),
      paste0("- Converted files: ", converted_n, "."),
      paste0("- Conversion failures: ", conversion_fail_n, "."),
      paste0("- Download inventory: `", DOWNLOAD_INVENTORY_CLEAN, "`."),
      paste0("- Conversion inventory: `", CONVERSION_INVENTORY_CLEAN, "`."),
      paste0("- Download failures: `", DOWNLOAD_FAILURES_CLEAN, "`."),
      paste0("- Conversion failures: `", CONVERSION_FAILURES_CLEAN, "`.")
    ),
    CLEAN_NOTES_MD
  )
}

# ------------------------------------------------------------------------------
# PLAN
# ------------------------------------------------------------------------------

if (!file.exists(PLAN_PATH)) {
  stop(sprintf("Plan file not found: %s", PLAN_PATH), call. = FALSE)
}

message("Reading NHANES download plan...")
plan <- readr::read_csv(PLAN_PATH, show_col_types = FALSE, progress = FALSE) %>%
  mutate(across(where(is.character), as.character))

required_cols <- c(
  "local_folder",
  "file_name",
  "source_url",
  "top_folder",
  "survey",
  "component_or_release",
  "link_type",
  "suggested_cleaning",
  "source_page"
)

missing_cols <- setdiff(required_cols, names(plan))
if (length(missing_cols) > 0) {
  stop(
    sprintf("Plan file is missing required columns: %s", paste(missing_cols, collapse = ", ")),
    call. = FALSE
  )
}

plan <- plan %>%
  mutate(
    local_folder = normalize_rel_path(.data$local_folder),
    file_name = trimws(as.character(.data$file_name)),
    source_url = trimws(as.character(.data$source_url)),
    clean_folder = vapply(.data$local_folder, raw_folder_to_clean_folder, character(1))
  )

plan <- resolve_plan_duplicates(plan) %>%
  mutate(raw_dest_path = file.path(.data$local_folder, .data$file_name))

bad_root_rows <- which(!startsWith(tolower(plan$local_folder), tolower(normalize_rel_path(RAW_ROOT))))
if (length(bad_root_rows) > 0) {
  stop(
    sprintf(
      "Some `local_folder` values do not live under %s. First bad row: %d",
      RAW_ROOT,
      bad_root_rows[[1]]
    ),
    call. = FALSE
  )
}

dup_dest <- plan %>%
  count(.data$raw_dest_path, name = "dup_n") %>%
  filter(.data$dup_n > 1)

if (nrow(dup_dest) > 0) {
  dup_path <- file.path(CLEAN_ROOT, "plan_destination_duplicates.csv")
  plan %>%
    semi_join(dup_dest, by = "raw_dest_path") %>%
    arrange(.data$raw_dest_path, .data$source_url) %>%
    readr::write_csv(dup_path)

  stop(
    sprintf(
      "The plan contains duplicate destination paths. Review: %s",
      dup_path
    ),
    call. = FALSE
  )
}

readr::write_csv(plan %>% select(-all_of(c("raw_dest_path", "clean_folder"))), RAW_PLAN_COPY)
readr::write_csv(plan %>% select(-all_of(c("raw_dest_path", "clean_folder"))), CLEAN_PLAN_COPY)

# ------------------------------------------------------------------------------
# DOWNLOAD
# ------------------------------------------------------------------------------

message("Downloading NHANES files...")

download_rows <- vector("list", nrow(plan))

for (i in seq_len(nrow(plan))) {
  r <- plan[i, , drop = FALSE]
  dest <- r$raw_dest_path[[1]]
  dir_ok(dirname(dest))

  message(sprintf("Download [%d/%d] %s", i, nrow(plan), basename(dest)))

  dl <- if (is_known_bad_source_url(r$source_url[[1]], r$link_type[[1]])) {
    list(
      status = "skipped_known_bad_url",
      error = "Skipped known bad legacy FTP URL."
    )
  } else {
    download_if_needed(r$source_url[[1]], dest)
  }

  download_rows[[i]] <- tibble::tibble(
    row_id = i,
    local_folder = r$local_folder[[1]],
    file_name = r$file_name[[1]],
    raw_dest_path = dest,
    source_url = r$source_url[[1]],
    top_folder = r$top_folder[[1]],
    survey = r$survey[[1]],
    component_or_release = r$component_or_release[[1]],
    link_type = r$link_type[[1]],
    suggested_cleaning = r$suggested_cleaning[[1]],
    source_page = r$source_page[[1]],
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

message("Converting NHANES files...")

downloaded_ok <- download_inventory %>%
  filter(.data$local_exists)

direct_candidates <- downloaded_ok %>%
  filter(tolower(tools::file_ext(.data$file_name)) %in% c("xpt", "csv")) %>%
  mutate(
    clean_dir = vapply(.data$local_folder, raw_folder_to_clean_folder, character(1))
  )

direct_conversion_rows <- vector("list", nrow(direct_candidates))

if (nrow(direct_candidates) > 0) {
  for (i in seq_len(nrow(direct_candidates))) {
    r <- direct_candidates[i, , drop = FALSE]
    message(sprintf("Direct conversion [%d/%d] %s", i, nrow(direct_candidates), r$file_name[[1]]))

    conv <- tryCatch(
      convert_cleanable_asset(r$raw_dest_path[[1]], r$clean_dir[[1]], write_csv = WRITE_CSV),
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
      survey = r$survey[[1]],
      local_folder = r$local_folder[[1]],
      file_name = r$file_name[[1]],
      raw_path = r$raw_dest_path[[1]],
      clean_dir = r$clean_dir[[1]],
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

survey_dirs <- downloaded_ok %>%
  distinct(.data$survey, .data$local_folder) %>%
  mutate(clean_dir = vapply(.data$local_folder, raw_folder_to_clean_folder, character(1)))

fw_rows <- vector("list", nrow(survey_dirs))

if (nrow(survey_dirs) > 0) {
  for (i in seq_len(nrow(survey_dirs))) {
    r <- survey_dirs[i, , drop = FALSE]
    message(sprintf("Fixed-width scan [%d/%d] %s", i, nrow(survey_dirs), r$survey[[1]]))
    fw_rows[[i]] <- convert_fixed_width_scripts(
      raw_dir = r$local_folder[[1]],
      clean_dir = r$clean_dir[[1]],
      survey_label = r$survey[[1]],
      write_csv = WRITE_CSV
    ) %>%
      mutate(conversion_type = "fixed_width")
  }
}

fixed_width_inventory <- bind_rows(fw_rows)

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

write_root_notes(plan, download_inventory, conversion_inventory)

message("Wrote: ", RAW_PLAN_COPY)
message("Wrote: ", CLEAN_PLAN_COPY)
message("Wrote: ", DOWNLOAD_INVENTORY_RAW)
message("Wrote: ", DOWNLOAD_FAILURES_RAW)
message("Wrote: ", CONVERSION_INVENTORY_CLEAN)
message("Wrote: ", CONVERSION_FAILURES_CLEAN)
message("Wrote: ", RAW_SOURCES_MD)
message("Wrote: ", CLEAN_NOTES_MD)
message("Done.")
