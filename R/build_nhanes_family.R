# R/build_nhanes_family.R
#
# PURPOSE
# -------
# Build a broader NHANES-family inventory for the warehouse.
#
# This umbrella script does two things:
#   1. Optionally run the continuous public NHANES XPT builder
#      (`R/build_nhanes_public.R`).
#   2. Create an official-page inventory spanning the wider NHANES family:
#      - continuous NHANES cycles and special release states
#      - NHANES I, NHANES II, NHANES III
#      - Hispanic HANES
#      - NHES I, NHES II, NHES III
#      - NHEFS
#      - NNYFS
#
# Important scope note
# --------------------
# Not all NHANES-family surveys share the same file format or page structure.
# The existing continuous builder handles modern public XPT table feeds.
# Older and special-study sections often expose DAT/TXT/SAS/PDF assets, limited
# access pages, or documentation-only pages and therefore need survey-specific
# ingest logic later.
#
# This script creates a structured inventory so we can manage those sources
# explicitly instead of silently excluding them.

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

RUN_CONTINUOUS_PUBLIC_BUILD <- !identical(tolower(Sys.getenv("NHANES_RUN_CONTINUOUS", "true")), "false")
MIRROR_FAMILY_ASSETS <- !identical(tolower(Sys.getenv("NHANES_MIRROR_FAMILY_ASSETS", "true")), "false")
SAVE_HTML_SNAPSHOTS  <- !identical(tolower(Sys.getenv("NHANES_SAVE_HTML_SNAPSHOTS", "true")), "false")
CRAWL_MAX_DEPTH      <- suppressWarnings(as.integer(Sys.getenv("NHANES_FAMILY_CRAWL_MAX_DEPTH", "2")))
if (is.na(CRAWL_MAX_DEPTH)) CRAWL_MAX_DEPTH <- 2L

RAW_ROOT   <- file.path("data", "raw", "NHANES")
CLEAN_ROOT <- file.path("data", "cleaned", "NHANES")

registry_raw_path <- file.path(RAW_ROOT, "family_registry.csv")
registry_clean_path <- file.path(CLEAN_ROOT, "family_registry.csv")
links_raw_path <- file.path(RAW_ROOT, "family_page_links.csv")
links_clean_path <- file.path(CLEAN_ROOT, "family_page_links.csv")
notes_md_path <- file.path(CLEAN_ROOT, "FAMILY_NOTES.md")

dir.create(RAW_ROOT, recursive = TRUE, showWarnings = FALSE)
dir.create(CLEAN_ROOT, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

safe_name <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("[/\\:*?\"<>|]+", "_", x)
  x <- gsub("\\s+", " ", x)
  x
}

survey_dir_name <- function(x) {
  safe_name(x)
}

survey_group_dir <- function(x) {
  x <- trimws(tolower(as.character(x)))

  dplyr::case_when(
    x == "continuous" ~ "Continuous NHANES",
    x %in% c("followup", "ancillary") ~ "NHANES Ancillary Studies",
    x == "legacy" ~ "NHANES Prior to 1999",
    TRUE ~ "Other NHANES"
  )
}

survey_folder_name <- function(reg_row) {
  sid <- trimws(as.character(reg_row$survey_id[[1]]))
  label <- trimws(as.character(reg_row$survey_label[[1]]))

  out <- switch(
    sid,
    continuous_all = "Continuous NHANES",
    nhanes_2025_2026 = "NHANES 2025-2026",
    nhanes_2021_2023 = "NHANES 08/2021-08/2023",
    nhanes_2017_march_2020 = "NHANES 2017-March 2020 Pre-Pandemic Data",
    nhanes_2019_2020 = "NHANES 2019-2020",
    nhanes_iii = "NHANES III",
    nhanes_ii = "NHANES II",
    nhanes_i = "NHANES I",
    hhanes = "Hispanic HANES",
    nhes_i = "NHES I",
    nhes_ii = "NHES II",
    nhes_iii = "NHES III",
    nhefs = "NHEFS",
    nnyfs_2012 = "NNYFS",
    label
  )

  survey_dir_name(out)
}

survey_storage_dirs <- function(reg_row) {
  group_dir <- survey_group_dir(reg_row$family_group[[1]])
  survey_folder <- survey_folder_name(reg_row)

  list(
    raw_dir = file.path(RAW_ROOT, group_dir, survey_folder),
    clean_dir = file.path(CLEAN_ROOT, group_dir, survey_folder)
  )
}

dir_ok <- function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE)

is_blank <- function(x) {
  is.na(x) | trimws(as.character(x)) == ""
}

absolute_url <- function(base_url, href) {
  if (length(href) == 0 || is.na(href) || !nzchar(href)) return(NA_character_)

  if (startsWith(href, "http://") || startsWith(href, "https://")) return(href)
  if (startsWith(href, "//")) return(paste0("https:", href))
  if (startsWith(href, "/")) return(paste0("https://wwwn.cdc.gov", href))

  xml2::url_absolute(href, base_url)
}

download_if_needed <- function(url, dest) {
  if (file.exists(dest) && isTRUE(file.info(dest)$size > 0)) return(invisible(TRUE))

  tmp <- paste0(dest, ".tmp")
  if (file.exists(tmp)) unlink(tmp, force = TRUE)
  dir_ok(dirname(dest))

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
  dir_ok(out_dir)

  arrow::write_parquet(df, file.path(out_dir, paste0(file_stem, ".parquet")))
  haven::write_dta(df, file.path(out_dir, paste0(file_stem, ".dta")))

  if (isTRUE(write_csv)) {
    readr::write_csv(df, file.path(out_dir, paste0(file_stem, ".csv")))
  }
}

convert_cleanable_asset <- function(asset_path, clean_base_dir, write_csv = TRUE) {
  ext <- tolower(tools::file_ext(asset_path))
  stem <- tools::file_path_sans_ext(basename(asset_path))

  if (ext == "xpt") {
    df <- haven::read_xpt(asset_path)
    write_named_sidecars(df, clean_base_dir, stem, write_csv = write_csv)
    return(list(status = "converted", rows = nrow(df), cols = ncol(df)))
  }

  if (ext == "csv") {
    df <- readr::read_csv(asset_path, show_col_types = FALSE, progress = FALSE)
    write_named_sidecars(df, clean_base_dir, stem, write_csv = write_csv)
    return(list(status = "converted", rows = nrow(df), cols = ncol(df)))
  }

  list(status = "unsupported", rows = NA_integer_, cols = NA_integer_)
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
  if (is.na(start_idx)) return(tibble::tibble(var = character(), start = integer(), end = integer(), is_char = logical()))

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
      select(.data$var, .data$start, .data$end, .data$is_char)

    if (!is.null(length_map) && nrow(length_map) > 0) {
      out <- out %>%
        left_join(length_map %>% select(.data$var, len_is_char = .data$is_char), by = "var") %>%
        mutate(is_char = dplyr::coalesce(.data$len_is_char, .data$is_char)) %>%
        select(.data$var, .data$start, .data$end, .data$is_char)
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
    out <- out %>%
      left_join(length_map %>% select(.data$var, .data$is_char), by = "var")
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

infer_sas_infile_target <- function(lines) {
  infile_hit <- stringr::str_match(
    paste(lines, collapse = "\n"),
    "(?i)infile\\s+['\"]([^'\"]+\\.txt)['\"]"
  )
  if (!is.na(infile_hit[1, 2]) && nzchar(infile_hit[1, 2])) {
    return(basename(infile_hit[1, 2]))
  }

  filename_hit <- stringr::str_match(
    paste(lines, collapse = "\n"),
    "(?i)filename\\s+(?:\\w+\\s+)?in\\s+['\"]([^'\"]+\\.txt)['\"]"
  )
  if (!is.na(filename_hit[1, 2]) && nzchar(filename_hit[1, 2])) {
    return(basename(filename_hit[1, 2]))
  }

  NA_character_
}

nhefs_script_txt_map <- function(script_name) {
  key <- tolower(basename(script_name))

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

  unname(mapping[[key]])
}

read_fixed_width_from_sas <- function(txt_path, sas_path) {
  lines <- readLines(sas_path, warn = FALSE, encoding = "UTF-8")
  length_map <- parse_sas_length_map(lines)
  layout <- parse_sas_input_layout(lines, length_map)
  if (nrow(layout) == 0) {
    stop("Could not parse INPUT layout from SAS script.")
  }

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

  layout <- layout %>%
    mutate(is_char = dplyr::coalesce(.data$is_char, FALSE))

  numeric_vars <- layout %>%
    filter(!.data$is_char, .data$var %in% names(df)) %>%
    pull(.data$var)

  if (length(numeric_vars) > 0) {
    for (nm in numeric_vars) {
      x <- trimws(df[[nm]])
      x[x %in% c("", ".")] <- NA_character_
      parsed <- suppressWarnings(readr::parse_double(x, na = c("", ".")))
      df[[nm]] <- parsed
    }
  }

  labels <- parse_sas_labels(lines)
  if (nrow(labels) > 0) {
    for (i in seq_len(nrow(labels))) {
      nm <- labels$var[[i]]
      if (nm %in% names(df)) {
        attr(df[[nm]], "label") <- labels$label[[i]]
      }
    }
  }

  list(
    data = df,
    layout = layout,
    labels = labels
  )
}

convert_fixed_width_sas_assets <- function(raw_dir, clean_dir, write_csv = TRUE, survey_id = NA_character_) {
  scripts <- list.files(raw_dir, pattern = "\\.(sas|txt)$", recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
  scripts <- scripts[grepl("\\.sas$|\\.inputs\\.labels\\.txt$", scripts, ignore.case = TRUE)]
  if (length(scripts) == 0) {
    return(tibble::tibble(
      script_path = character(),
      txt_path = character(),
      clean_parquet_path = character(),
      clean_dta_path = character(),
      clean_csv_path = character(),
      conversion_status = character()
    ))
  }

  out <- vector("list", length(scripts))

  for (i in seq_along(scripts)) {
    script_path <- scripts[[i]]
    lines <- tryCatch(readLines(script_path, warn = FALSE, encoding = "UTF-8"), error = function(e) character())
    txt_name <- infer_sas_infile_target(lines)

    if ((is.na(txt_name) || !nzchar(txt_name)) && identical(as.character(survey_id), "nhefs")) {
      txt_name <- nhefs_script_txt_map(script_path)
    }

    txt_path <- if (!is.na(txt_name) && nzchar(txt_name)) {
      candidates <- list.files(raw_dir, recursive = TRUE, full.names = TRUE)
      candidates <- candidates[tolower(basename(candidates)) == tolower(txt_name)]
      if (length(candidates) > 0) candidates[[1]] else NA_character_
    } else {
      NA_character_
    }

    result <- list(
      script_path = script_path,
      txt_path = txt_path,
      clean_parquet_path = NA_character_,
      clean_dta_path = NA_character_,
      clean_csv_path = NA_character_,
      conversion_status = "skipped"
    )

    if (!is.na(txt_path) && file.exists(txt_path)) {
      rel_dir <- dirname(txt_path)
      rel_dir <- sub(paste0("^", gsub("\\\\", "\\\\\\\\", raw_dir)), "", rel_dir)
      rel_dir <- sub("^[/\\\\]+", "", rel_dir)
      target_dir <- if (nzchar(rel_dir)) file.path(clean_dir, rel_dir) else clean_dir
      stem <- tools::file_path_sans_ext(basename(txt_path))

      conv <- tryCatch({
        parsed <- read_fixed_width_from_sas(txt_path, script_path)
        write_named_sidecars(parsed$data, target_dir, stem, write_csv = write_csv)
        if (nrow(parsed$labels) > 0) {
          readr::write_csv(parsed$labels, file.path(target_dir, paste0(stem, "_variables.csv")))
        }
        list(status = "converted")
      }, error = function(e) {
        list(status = paste0("failed: ", conditionMessage(e)))
      })

      result$clean_parquet_path <- file.path(target_dir, paste0(stem, ".parquet"))
      result$clean_dta_path <- file.path(target_dir, paste0(stem, ".dta"))
      result$clean_csv_path <- file.path(target_dir, paste0(stem, ".csv"))
      result$conversion_status <- conv$status
    }

    out[[i]] <- tibble::as_tibble(result)
  }

  bind_rows(out) %>%
    mutate(
      dedupe_key = dplyr::if_else(
        is.na(.data$txt_path) | .data$txt_path == "",
        .data$script_path,
        .data$txt_path
      )
    ) %>%
    distinct(.data$dedupe_key, .keep_all = TRUE) %>%
    select(-.data$dedupe_key) %>%
    arrange(.data$txt_path)
}

flatten_old_survey_paths <- function(root_dir, survey_id) {
  if (!dir.exists(root_dir)) return(invisible(NULL))

  prefixes <- switch(
    survey_id,
    nhanes_i = c(file.path("nchs", "data", "nhanes1")),
    nhanes_ii = c(file.path("nchs", "data", "nhanes2")),
    hhanes = c(file.path("nchs", "data", "hhanes")),
    nhes_i = c(file.path("nchs", "data", "nhes123"), file.path("nchs", "data", "nhes1")),
    nhes_ii = c(file.path("nchs", "data", "nhes123"), file.path("nchs", "data", "nhes2")),
    nhes_iii = c(file.path("nchs", "data", "nhes123"), file.path("nchs", "data", "nhes3")),
    nhefs = c(file.path("nchs", "data", "nhefs")),
    nnyfs_2012 = c(file.path("nchs", "data", "nnyfs", "Public", "2012", "DataFiles")),
    character(0)
  )

  if (length(prefixes) == 0) return(invisible(NULL))

  files <- list.files(root_dir, recursive = TRUE, full.names = TRUE, all.files = FALSE)
  files <- files[file.info(files)$isdir %in% FALSE]
  if (length(files) == 0) return(invisible(NULL))

  for (src in files) {
    rel <- sub(paste0("^", gsub("\\\\", "\\\\\\\\", root_dir), "[/\\\\]?"), "", src)
    rel_norm <- gsub("/", .Platform$file.sep, rel, fixed = TRUE)
    new_rel <- NULL

    if (survey_id == "nnyfs_2012" && startsWith(tolower(gsub("\\\\", "/", rel_norm)), "nchs/data/nnyfs/public/2012/datafiles/")) {
      new_rel <- sub("(?i)^nchs[/\\\\]data[/\\\\]nnyfs[/\\\\]public[/\\\\]2012[/\\\\]datafiles[/\\\\]?", paste0("data_files", .Platform$file.sep), rel_norm, perl = TRUE)
    } else {
      for (pref in prefixes) {
        pref_pattern <- paste0("(?i)^", gsub("\\\\", "[/\\\\]", pref), "[/\\\\]?")
        candidate <- sub(pref_pattern, paste0("data", .Platform$file.sep), rel_norm, perl = TRUE)
        if (!identical(candidate, rel_norm)) {
          new_rel <- candidate
          break
        }
      }
    }

    if (is.null(new_rel) || identical(new_rel, rel_norm)) next

    dest <- file.path(root_dir, new_rel)
    dir_ok(dirname(dest))
    if (!file.exists(dest)) file.rename(src, dest)
  }
}

classify_link_type <- function(url, text) {
  u <- tolower(coalesce(url, ""))
  t <- tolower(coalesce(text, ""))

  dplyr::case_when(
    str_detect(u, "\\.xpt($|\\?)") ~ "data_xpt",
    str_detect(u, "\\.dat($|\\?)") ~ "data_dat",
    str_detect(u, "\\.txt($|\\?)") & str_detect(t, "input|label|setup|sas") ~ "code_txt",
    str_detect(u, "\\.txt($|\\?)") & str_detect(t, "du[0-9]|n[0-9]{2}|data|file|public") ~ "data_txt",
    str_detect(u, "\\.csv($|\\?)") ~ "data_csv",
    str_detect(u, "\\.zip($|\\?)") ~ "data_zip",
    str_detect(u, "\\.sas($|\\?)") ~ "code_sas",
    str_detect(u, "\\.txt($|\\?)") ~ "doc_txt",
    str_detect(u, "\\.pdf($|\\?)") ~ "doc_pdf",
    str_detect(t, "limited access|rdc only|research data center") ~ "limited_access",
    str_detect(t, "demographics data|dietary data|examination data|laboratory data|questionnaire data|data files") ~ "data_page",
    str_detect(t, "release notes") ~ "release_notes",
    str_detect(t, "questionnaire instruments|laboratory methods|procedure manuals|overview|analytic guidance|brochures") ~ "documentation_page",
    str_detect(u, "\\.aspx($|\\?)|\\.htm(l)?($|\\?)") ~ "html_page",
    TRUE ~ "other"
  )
}

survey_scope_patterns <- function(survey_id) {
  switch(
    survey_id,
    nhanes_iii = c("/nchs/nhanes/nhanes3/", "/nchs/data/nhanes3/"),
    nhanes_ii = c("/nchs/nhanes/nhanes2/", "/nchs/data/nhanes2/"),
    nhanes_i = c("/nchs/nhanes/nhanes1/", "/nchs/data/nhanes1/"),
    hhanes = c("/nchs/nhanes/hhanes/", "/nchs/data/hhanes/"),
    nhes_i = c("/nchs/nhanes/nhes1/", "/nchs/data/nhes1/", "/nchs/data/nhes123/"),
    nhes_ii = c("/nchs/nhanes/nhes2/", "/nchs/data/nhes2/", "/nchs/data/nhes123/"),
    nhes_iii = c("/nchs/nhanes/nhes3/", "/nchs/data/nhes3/", "/nchs/data/nhes123/"),
    nhefs = c("/nchs/nhanes/nhefs/", "/nchs/data/nhefs/"),
    nnyfs_2012 = c("/nchs/nhanes/search/nnyfs12", "/nchs/nhanes/search/nnyfsdata\\.aspx\\?Component=", "/nchs/nhanes/search/datapage\\.aspx\\?Component=NNYFS", "/nchs/data/nnyfs/", "/Nchs/Data/Nnyfs/Public/"),
    character(0)
  )
}

in_survey_scope <- function(url, survey_id) {
  patterns <- survey_scope_patterns(survey_id)
  if (length(patterns) == 0) return(rep(FALSE, length(url)))

  url <- as.character(url)
  out <- rep(FALSE, length(url))
  valid <- !(is.na(url) | !nzchar(url))
  if (!any(valid)) return(out)

  out[valid] <- vapply(
    url[valid],
    function(u) any(vapply(
      patterns,
      function(pat) grepl(pat, u, ignore.case = TRUE),
      logical(1)
    )),
    logical(1)
  )

  out
}

is_downloadable_asset <- function(link_type) {
  link_type %in% c("data_xpt", "data_dat", "data_txt", "data_csv", "data_zip", "code_sas", "code_txt", "doc_txt", "doc_pdf")
}

is_crawlable_page <- function(url, link_type, survey_id) {
  url <- as.character(url)
  link_type <- as.character(link_type)

  out <- !(is.na(url) | !nzchar(url))
  out <- out & !grepl("#", url, fixed = TRUE)
  out <- out & in_survey_scope(url, survey_id)
  out <- out & (link_type %in% c("html_page", "data_page", "documentation_page", "release_notes", "other"))
  out
}

url_to_relative_path <- function(url) {
  path <- sub("^https?://[^/]+", "", url)
  path <- sub("\\?.*$", "", path)
  path <- utils::URLdecode(path)
  path <- gsub("^/+", "", path)
  path <- gsub("/", .Platform$file.sep, path, fixed = TRUE)
  path
}

infer_nnyfs_component_dir <- function(parent_page) {
  page <- tolower(as.character(parent_page))

  dplyr::case_when(
    grepl("component=demographics", page, fixed = TRUE) ~ "Demographics Data",
    grepl("component=dietary", page, fixed = TRUE) ~ "Dietary Data",
    grepl("component=examination", page, fixed = TRUE) ~ "Examination Data",
    grepl("component=laboratory", page, fixed = TRUE) ~ "Laboratory Data",
    grepl("component=questionnaire", page, fixed = TRUE) ~ "Questionnaire Data",
    TRUE ~ "Other Data"
  )
}

survey_relative_path <- function(url, survey_id, link_type = NA_character_, parent_page = NA_character_) {
  path <- tolower(sub("^https?://[^/]+", "", as.character(url)))
  path <- sub("\\?.*$", "", path)
  path <- utils::URLdecode(path)
  path <- gsub("^/+", "", path)

  path <- switch(
    survey_id,
    nhanes_i = sub("^nchs/data/nhanes1/?", "data/", path),
    nhanes_ii = sub("^nchs/data/nhanes2/?", "data/", path),
    hhanes = sub("^nchs/data/hhanes/?", "data/", path),
    nhes_i = sub("^nchs/data/nhes123/?", "data/", sub("^nchs/data/nhes1/?", "data/", path)),
    nhes_ii = sub("^nchs/data/nhes123/?", "data/", sub("^nchs/data/nhes2/?", "data/", path)),
    nhes_iii = sub("^nchs/data/nhes123/?", "data/", sub("^nchs/data/nhes3/?", "data/", path)),
    nhefs = sub("^nchs/data/nhefs/?", "", path),
    nnyfs_2012 = {
      if (grepl("^nchs/data/nnyfs/public/2012/datafiles/", path)) {
        file.path(
          infer_nnyfs_component_dir(parent_page),
          basename(path)
        )
      } else if (grepl("^nchs/data/nnyfs/", path)) {
        sub("^nchs/data/nnyfs/?", "docs/", path)
      } else {
        path
      }
    },
    path
  )

  path <- gsub("^/+", "", path)
  gsub("/", .Platform$file.sep, path, fixed = TRUE)
}

page_snapshot_path <- function(raw_dir, url) {
  path <- sub("^https?://[^/]+", "", url)
  query <- ""
  if (grepl("\\?", path)) {
    query <- sub("^[^?]*\\?", "", path)
    path <- sub("\\?.*$", "", path)
  }

  path <- utils::URLdecode(path)
  path <- gsub("^/+", "", path)
  path <- gsub("/", "_", path, fixed = TRUE)
  path <- safe_name(path)

  if (nzchar(query)) {
    path <- paste0(path, "__", safe_name(query))
  }

  file.path(raw_dir, "_pages", paste0(path, ".html"))
}

extract_href_candidates_from_html <- function(html_text, base_url) {
  if (length(html_text) == 0 || is.na(html_text) || !nzchar(html_text)) {
    return(tibble::tibble(
      href = character(),
      asset_url = character()
    ))
  }

  hits <- stringr::str_match_all(
    html_text,
    "(?i)href\\s*=\\s*['\"]([^'\"]+)['\"]"
  )[[1]]

  if (nrow(hits) == 0) {
    return(tibble::tibble(
      href = character(),
      asset_url = character()
    ))
  }

  hrefs <- unique(hits[, 2])
  hrefs <- hrefs[!is.na(hrefs) & nzchar(hrefs)]

  tibble::tibble(
    href = hrefs,
    asset_url = vapply(hrefs, absolute_url, FUN.VALUE = character(1), base_url = base_url)
  ) %>%
    filter(!is_blank(.data$asset_url))
}

scrape_page_links <- function(survey_id, page_url, survey_label) {
  page <- xml2::read_html(page_url)
  anchors <- rvest::html_elements(page, "a")

  if (length(anchors) == 0) {
    return(tibble::tibble(
      survey_id = character(),
      survey_label = character(),
      page_url = character(),
      link_text = character(),
      link_url = character(),
      link_type = character(),
      is_public_file = logical()
    ))
  }

  link_text <- trimws(rvest::html_text2(anchors))
  href <- rvest::html_attr(anchors, "href")
  link_url <- vapply(href, absolute_url, FUN.VALUE = character(1), base_url = page_url)

  out <- tibble::tibble(
    survey_id = survey_id,
    survey_label = survey_label,
    page_url = page_url,
    link_text = link_text,
    link_url = link_url,
    stringsAsFactors = FALSE
  ) %>%
    filter(!is_blank(.data$link_url)) %>%
    mutate(
      link_type = classify_link_type(.data$link_url, .data$link_text),
      is_public_file = .data$link_type %in% c("data_xpt", "data_dat", "data_csv", "data_zip", "code_sas", "doc_txt", "doc_pdf")
    ) %>%
    distinct(.data$page_url, .data$link_text, .data$link_url, .keep_all = TRUE)

  out
}

crawl_survey_assets <- function(reg_row) {
  sid <- reg_row$survey_id[[1]]
  survey_label <- reg_row$survey_label[[1]]
  dirs <- survey_storage_dirs(reg_row)
  raw_dir <- dirs$raw_dir

  seed_urls <- unique(c(
    reg_row$entry_url[[1]],
    if (sid == "nnyfs_2012") c(
      "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfsdata.aspx?Component=Demographics",
      "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfsdata.aspx?Component=Dietary",
      "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfsdata.aspx?Component=Examination",
      "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfsdata.aspx?Component=Laboratory",
      "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfsdata.aspx?Component=Questionnaire"
    ) else character(0),
    if (sid == "nhanes_iii") "https://wwwn.cdc.gov/nchs/nhanes/nhanes3/datafiles.aspx" else character(0)
  ))

  queue <- tibble::tibble(url = seed_urls, depth = 0L)
  visited <- character(0)
  pages_seen <- character(0)
  asset_rows <- list()

  while (nrow(queue) > 0) {
    current <- queue[1, , drop = FALSE]
    queue <- queue[-1, , drop = FALSE]
    page_url <- current$url[[1]]
    depth <- current$depth[[1]]

    if (page_url %in% visited) next
    visited <- c(visited, page_url)

    page <- tryCatch(xml2::read_html(page_url), error = function(e) NULL)
    if (is.null(page)) next

    pages_seen <- c(pages_seen, page_url)
    page_html <- as.character(page)

    if (SAVE_HTML_SNAPSHOTS) {
      snapshot <- page_snapshot_path(raw_dir, page_url)
      try(download_if_needed(page_url, snapshot), silent = TRUE)
    }

    anchors <- rvest::html_elements(page, "a")

    anchor_links <- if (length(anchors) == 0) {
      tibble::tibble(
        href = character(),
        link_text = character()
      )
    } else {
      tibble::tibble(
        href = rvest::html_attr(anchors, "href"),
        link_text = trimws(rvest::html_text2(anchors))
      )
    }

    raw_href_links <- extract_href_candidates_from_html(page_html, page_url) %>%
      transmute(
        href = .data$href,
        link_text = basename(sub("\\?.*$", "", .data$asset_url))
      )

    page_links <- bind_rows(anchor_links, raw_href_links) %>%
      filter(!is.na(.data$href), nzchar(.data$href)) %>%
      mutate(
        survey_id = sid,
        survey_label = survey_label,
        parent_page = page_url,
        link_text = ifelse(is.na(.data$link_text), "", .data$link_text),
        asset_url = vapply(.data$href, absolute_url, FUN.VALUE = character(1), base_url = page_url),
        link_type = classify_link_type(.data$asset_url, .data$link_text)
      ) %>%
      filter(in_survey_scope(.data$asset_url, sid)) %>%
      distinct(.data$parent_page, .data$asset_url, .keep_all = TRUE)

    if (nrow(page_links) == 0) next

    asset_rows[[length(asset_rows) + 1]] <- page_links

    if (depth < CRAWL_MAX_DEPTH) {
      next_pages <- page_links %>%
        filter(is_crawlable_page(.data$asset_url, .data$link_type, sid)) %>%
        pull(.data$asset_url) %>%
        unique()

      if (length(next_pages) > 0) {
        queue <- bind_rows(
          queue,
          tibble::tibble(url = next_pages, depth = depth + 1L)
        )
      }
    }
  }

  assets <- bind_rows(asset_rows)
  if (nrow(assets) == 0) {
      return(tibble::tibble(
      survey_id = sid,
      survey_label = survey_label,
      parent_page = character(),
      link_text = character(),
      asset_url = character(),
      link_type = character(),
      is_downloadable = logical(),
      local_path = character(),
      local_exists = logical(),
      discovered_at = character()
    ))
  }

  assets %>%
    mutate(
      is_downloadable = is_downloadable_asset(.data$link_type),
      local_path = ifelse(
        .data$is_downloadable,
        file.path(
          raw_dir,
          vapply(
            seq_along(.data$asset_url),
            function(i) survey_relative_path(
              .data$asset_url[[i]],
              sid,
              .data$link_type[[i]],
              .data$parent_page[[i]]
            ),
            character(1)
          )
        ),
        NA_character_
      ),
      local_exists = ifelse(!is.na(.data$local_path), file.exists(.data$local_path), FALSE),
      discovered_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    ) %>%
    select(
      "survey_id",
      "survey_label",
      "parent_page",
      "link_text",
      "asset_url",
      "link_type",
      "is_downloadable",
      "local_path",
      "local_exists",
      "discovered_at"
    ) %>%
    distinct(.data$asset_url, .keep_all = TRUE) %>%
    arrange(.data$link_type, .data$asset_url)
}

mirror_survey_assets <- function(reg_row) {
  survey_label <- reg_row$survey_label[[1]]
  survey_id <- reg_row$survey_id[[1]]
  dirs <- survey_storage_dirs(reg_row)
  raw_dir <- dirs$raw_dir
  clean_dir <- dirs$clean_dir

  if (reg_row$survey_id[[1]] == "nhanes_iii") {
    sys.source("R/build_nhanes3_public.R", envir = new.env(parent = globalenv()))
    return(invisible(NULL))
  }

  asset_inventory <- crawl_survey_assets(reg_row)

  if (MIRROR_FAMILY_ASSETS && nrow(asset_inventory) > 0) {
    downloads <- asset_inventory %>%
      filter(.data$is_downloadable, !is.na(.data$local_path), nzchar(.data$local_path))

    if (nrow(downloads) > 0) {
      for (i in seq_len(nrow(downloads))) {
        r <- downloads[i, , drop = FALSE]
        message(sprintf("Mirroring %s asset [%d/%d]: %s", survey_label, i, nrow(downloads), basename(r$local_path[[1]])))
        try(download_if_needed(r$asset_url[[1]], r$local_path[[1]]), silent = TRUE)
      }
    }
  }

  asset_inventory <- asset_inventory %>%
    mutate(
      local_exists = ifelse(!is.na(.data$local_path), file.exists(.data$local_path), FALSE),
      local_size = ifelse(
        .data$local_exists & !is.na(.data$local_path),
        as.numeric(file.info(.data$local_path)$size),
        NA_real_
      )
    )

  cleanable_rows <- which(
    asset_inventory$link_type %in% c("data_xpt", "data_csv") &
      asset_inventory$local_exists &
      !is.na(asset_inventory$local_path) &
      nzchar(asset_inventory$local_path)
  )

  if (length(cleanable_rows) > 0) {
    parquet_paths <- rep(NA_character_, nrow(asset_inventory))
    dta_paths <- rep(NA_character_, nrow(asset_inventory))
    csv_paths <- rep(NA_character_, nrow(asset_inventory))
    clean_status <- rep(NA_character_, nrow(asset_inventory))

    for (idx in cleanable_rows) {
      asset_path <- asset_inventory$local_path[[idx]]
      rel_dir <- dirname(asset_path)
      rel_dir <- sub(paste0("^", gsub("\\\\", "\\\\\\\\", raw_dir)), "", rel_dir)
      rel_dir <- sub("^[/\\\\]+", "", rel_dir)
      target_dir <- if (nzchar(rel_dir)) file.path(clean_dir, rel_dir) else clean_dir
      stem <- tools::file_path_sans_ext(basename(asset_path))

      res <- tryCatch(
        convert_cleanable_asset(asset_path, target_dir, write_csv = TRUE),
        error = function(e) list(status = paste0("failed: ", conditionMessage(e)))
      )

      parquet_paths[[idx]] <- file.path(target_dir, paste0(stem, ".parquet"))
      dta_paths[[idx]] <- file.path(target_dir, paste0(stem, ".dta"))
      csv_paths[[idx]] <- file.path(target_dir, paste0(stem, ".csv"))
      clean_status[[idx]] <- res$status
    }

    asset_inventory$clean_parquet_path <- parquet_paths
    asset_inventory$clean_dta_path <- dta_paths
    asset_inventory$clean_csv_path <- csv_paths
    asset_inventory$clean_conversion_status <- clean_status
  }

  fixed_width_inventory <- convert_fixed_width_sas_assets(
    raw_dir,
    clean_dir,
    write_csv = TRUE,
    survey_id = survey_id
  )

  write.csv(asset_inventory, file.path(raw_dir, "asset_inventory.csv"), row.names = FALSE)
  write.csv(asset_inventory, file.path(clean_dir, "asset_inventory.csv"), row.names = FALSE)
  write.csv(fixed_width_inventory, file.path(raw_dir, "fixed_width_conversion_inventory.csv"), row.names = FALSE)
  write.csv(fixed_width_inventory, file.path(clean_dir, "fixed_width_conversion_inventory.csv"), row.names = FALSE)
}

family_registry <- function() {
  tibble::tribble(
    ~survey_id, ~survey_label, ~family_group, ~years, ~entry_url, ~ingest_mode, ~public_status, ~notes,
    "continuous_all", "All Continuous NHANES", "continuous", "1999-present", "https://wwwn.cdc.gov/nchs/nhanes/default.aspx", "continuous_xpt_manifest", "public_mixed", "Modern continuous NHANES public tables are primarily exposed as public XPT files. This is the survey family covered by build_nhanes_public.R.",
    "nhanes_2025_2026", "NHANES 2025-2026", "continuous", "2025-2026", "https://wwwn.cdc.gov/nchs/nhanes/default.aspx", "documentation_only_so_far", "not_yet_public_data", "As of March 22, 2026, the CDC page lists instruments, methods, and release notes but not public data component pages.",
    "nhanes_2021_2023", "NHANES 08/2021-08/2023", "continuous", "2021-2023", "https://wwwn.cdc.gov/nchs/nhanes/default.aspx", "continuous_xpt_manifest", "public", "Included in the continuous public XPT build.",
    "nhanes_2017_march_2020", "NHANES 2017-March 2020", "continuous", "2017-2020", "https://wwwn.cdc.gov/nchs/nhanes/default.aspx", "continuous_xpt_manifest", "public", "Public release is the combined pre-pandemic file set rather than a standard standalone 2019-2020 cycle.",
    "nhanes_2019_2020", "NHANES 2019-2020", "continuous", "2019-2020", "https://wwwn.cdc.gov/nchs/nhanes/default.aspx", "limited_access_only", "limited_access", "CDC exposes limited-access material for the convenience sample, not a normal public cycle release.",
    "nhanes_iii", "NHANES III", "legacy", "1988-1994", "https://wwwn.cdc.gov/nchs/nhanes/nhanes3/default.aspx", "legacy_custom", "public_legacy", "Legacy file structure with dedicated data pages and DAT/SAS/PDF assets. Raw asset mirroring is handled by R/build_nhanes3_public.R.",
    "nhanes_ii", "NHANES II", "legacy", "1976-1980", "https://wwwn.cdc.gov/nchs/nhanes/nhanes2/default.aspx", "legacy_custom", "public_legacy", "Legacy survey pages need survey-specific ingestion logic.",
    "nhanes_i", "NHANES I", "legacy", "1971-1974", "https://wwwn.cdc.gov/nchs/nhanes/nhanes1/default.aspx", "legacy_custom", "public_legacy", "Legacy survey pages need survey-specific ingestion logic.",
    "hhanes", "Hispanic HANES", "legacy", "1982-1984", "https://wwwn.cdc.gov/nchs/nhanes/hhanes/default.aspx", "legacy_custom", "public_legacy", "Legacy survey with its own page structure.",
    "nhes_i", "NHES I", "legacy", "1959-1962", "https://wwwn.cdc.gov/nchs/nhanes/nhes1/default.aspx", "legacy_custom", "public_legacy", "Legacy precursor survey with its own page structure.",
    "nhes_ii", "NHES II", "legacy", "1963-1965", "https://wwwn.cdc.gov/nchs/nhanes/nhes2/default.aspx", "legacy_custom", "public_legacy", "Legacy precursor survey with its own page structure.",
    "nhes_iii", "NHES III", "legacy", "1966-1970", "https://wwwn.cdc.gov/nchs/nhanes/nhes3/default.aspx", "legacy_custom", "public_legacy", "Legacy precursor survey with its own page structure.",
    "nhefs", "NHEFS", "followup", "1982-1992 follow-up", "https://wwwn.cdc.gov/nchs/nhanes/nhefs/default.aspx", "followup_custom", "public_mixed", "Longitudinal follow-up study linked to NHANES I and managed separately from the modern NHANES table manifest.",
    "nnyfs_2012", "NNYFS 2012", "ancillary", "2012", "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfs12.aspx", "ancillary_custom", "public_mixed", "Ancillary study with separate discovery pages; some files are RDC only."
  ) %>%
    mutate(
      entry_url = as.character(.data$entry_url),
      discovered_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    )
}

summarise_links <- function(registry, links) {
  registry %>%
    left_join(
      links %>%
        group_by(.data$survey_id) %>%
        summarise(
          page_count = n_distinct(.data$page_url),
          link_count = n(),
          has_public_xpt = any(.data$link_type == "data_xpt"),
          has_legacy_dat = any(.data$link_type == "data_dat"),
          has_public_downloads = any(.data$is_public_file),
          has_limited_access = any(.data$link_type == "limited_access"),
          has_release_notes = any(.data$link_type == "release_notes"),
          has_documentation_pages = any(.data$link_type == "documentation_page"),
          .groups = "drop"
        ),
      by = "survey_id"
    ) %>%
    mutate(
      page_count = dplyr::coalesce(.data$page_count, 0L),
      link_count = dplyr::coalesce(.data$link_count, 0L),
      has_public_xpt = dplyr::coalesce(.data$has_public_xpt, FALSE),
      has_legacy_dat = dplyr::coalesce(.data$has_legacy_dat, FALSE),
      has_public_downloads = dplyr::coalesce(.data$has_public_downloads, FALSE),
      has_limited_access = dplyr::coalesce(.data$has_limited_access, FALSE),
      has_release_notes = dplyr::coalesce(.data$has_release_notes, FALSE),
      has_documentation_pages = dplyr::coalesce(.data$has_documentation_pages, FALSE)
    )
}

write_survey_scaffold <- function(reg_row, survey_links) {
  dirs <- survey_storage_dirs(reg_row)
  raw_dir <- dirs$raw_dir
  clean_dir <- dirs$clean_dir

  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(clean_dir, recursive = TRUE, showWarnings = FALSE)

  reg_out <- reg_row %>%
    mutate(
      raw_folder = raw_dir,
      cleaned_folder = clean_dir
    )

  write.csv(reg_out, file.path(raw_dir, "survey_registry.csv"), row.names = FALSE)
  write.csv(reg_out, file.path(clean_dir, "survey_registry.csv"), row.names = FALSE)

  write.csv(survey_links, file.path(raw_dir, "page_links.csv"), row.names = FALSE)
  write.csv(survey_links, file.path(clean_dir, "page_links.csv"), row.names = FALSE)

  writeLines(
    c(
      paste0(reg_row$survey_label[[1]], " official entry page and first-level link inventory."),
      paste0("Entry URL: ", reg_row$entry_url[[1]]),
      paste0("Survey group: ", reg_row$family_group[[1]]),
      paste0("Public status: ", reg_row$public_status[[1]]),
      paste0("Ingest mode: ", reg_row$ingest_mode[[1]]),
      paste0("Generated on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    file.path(raw_dir, "SOURCES.md")
  )

  writeLines(
    c(
      paste0("# ", reg_row$survey_label[[1]], " dataset notes"),
      "",
      sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
      "",
      paste0("- Survey id: `", reg_row$survey_id[[1]], "`."),
      paste0("- Years: ", reg_row$years[[1]]),
      paste0("- Family group: ", reg_row$family_group[[1]]),
      paste0("- Entry URL: ", reg_row$entry_url[[1]]),
      paste0("- Ingest mode: ", reg_row$ingest_mode[[1]]),
      paste0("- Public status: ", reg_row$public_status[[1]]),
      paste0("- Notes: ", reg_row$notes[[1]]),
      paste0("- `survey_registry.csv` stores the survey-level registry record for `", reg_row$survey_label[[1]], "`."),
      "- `page_links.csv` stores first-level link discovery from the official entry page.",
      "- Survey-specific raw mirroring/parsing may still require a dedicated build script."
    ),
    file.path(clean_dir, "DATASET_NOTES.md")
  )
}

# ------------------------------------------------------------------------------
# OPTIONAL CONTINUOUS BUILD
# ------------------------------------------------------------------------------

if (isTRUE(RUN_CONTINUOUS_PUBLIC_BUILD)) {
  message("Running continuous public NHANES XPT build...")
  source("R/build_nhanes_public.R")
}

# ------------------------------------------------------------------------------
# REGISTRY + PAGE INVENTORY
# ------------------------------------------------------------------------------

message("Building NHANES-family registry...")
registry <- family_registry()

message("Scraping official entry pages for NHANES-family link inventory...")
link_rows <- purrr::map2(
  registry$survey_id,
  registry$entry_url,
  function(survey_id, page_url) {
    label <- registry$survey_label[registry$survey_id == survey_id][1]

    tryCatch(
      scrape_page_links(survey_id, page_url, label),
      error = function(e) {
        warning(
          sprintf("Failed to scrape %s (%s): %s", survey_id, page_url, conditionMessage(e)),
          call. = FALSE
        )

        tibble::tibble(
          survey_id = survey_id,
          survey_label = label,
          page_url = page_url,
          link_text = NA_character_,
          link_url = NA_character_,
          link_type = "scrape_error",
          is_public_file = FALSE,
          error_message = conditionMessage(e),
          stringsAsFactors = FALSE
        )
      }
    )
  }
)

links <- bind_rows(link_rows) %>%
  mutate(
    link_text = ifelse(is.na(.data$link_text), "", .data$link_text),
    link_url = ifelse(is.na(.data$link_url), "", .data$link_url)
  )

registry_summary <- summarise_links(registry, links)

write.csv(registry_summary, registry_raw_path, row.names = FALSE)
write.csv(registry_summary, registry_clean_path, row.names = FALSE)
write.csv(links, links_raw_path, row.names = FALSE)
write.csv(links, links_clean_path, row.names = FALSE)

for (i in seq_len(nrow(registry_summary))) {
  reg_row <- registry_summary[i, , drop = FALSE]
  if (identical(reg_row$survey_id[[1]], "continuous_all")) next

  survey_links <- links %>%
    filter(.data$survey_id == reg_row$survey_id[[1]])

  write_survey_scaffold(reg_row, survey_links)

  if (reg_row$family_group[[1]] != "continuous") {
    mirror_survey_assets(reg_row)
  }
}

message("Wrote: ", registry_raw_path)
message("Wrote: ", registry_clean_path)
message("Wrote: ", links_raw_path)
message("Wrote: ", links_clean_path)

# ------------------------------------------------------------------------------
# NOTES
# ------------------------------------------------------------------------------

writeLines(
  c(
    "# NHANES Family Notes",
    "",
    sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "- `build_nhanes_public.R` remains the continuous public NHANES XPT builder.",
    "- `family_registry.csv` inventories broader NHANES-family survey sections from official CDC/NCHS pages.",
    "- `family_page_links.csv` stores first-level link discovery from those official survey entry pages.",
    "- Survey-family folders created by this umbrella build are grouped under `Continuous NHANES/`, `NHANES Ancillary Studies/`, `NHANES Prior to 1999/`, and `Other NHANES/` in both raw and cleaned trees.",
    "- Non-continuous survey folders now also receive `asset_inventory.csv` files, and downloadable public assets are mirrored locally where discoverable from official survey pages.",
    "- Where the mirrored raw asset is directly readable as `XPT` or `CSV`, the cleaned tree stores same-stem conversions such as `BFRPOL_G.parquet` and `BFRPOL_G.dta` while preserving the relative path structure.",
    "- Legacy surveys and special studies are intentionally separated because many do not use the modern manifest/XPT table model.",
    "- `nhanes_2025_2026` is currently marked as documentation-only because the official page lists methods/instruments/release notes but not public data component pages as of the build date.",
    "- `nhanes_2019_2020` is tracked separately because CDC exposes limited-access convenience-sample material rather than a normal public cycle release.",
    "- `build_nhanes3_public.R` now provides dedicated NHANES III raw asset mirroring and inventory for the official legacy data-files page.",
    "- `nnyfs_2012`, `nhefs`, `nhanes_i`, `nhanes_ii`, `hhanes`, and `nhes_i-iii` remain candidates for survey-specific raw mirroring and custom parsing scripts."
  ),
  notes_md_path
)

message("Wrote: ", notes_md_path)
message("Done.")
