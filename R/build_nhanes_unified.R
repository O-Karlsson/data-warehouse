# R/build_nhanes_unified.R
#
# PURPOSE
# -------
# Download NHANES-family public data into `data/raw/NHANES/` and convert directly
# readable datasets into `.parquet`, `.dta`, and optional `.csv` sidecars in
# `data/cleaned/NHANES/`.
#
# DESIGN
# ------
# This is a clean-slate builder focused on one simple task:
#   1. Mirror raw public NHANES-family files into a site-style folder structure.
#   2. Convert readable raw files into better local formats.
#
# Target top-level structure:
#   data/raw/NHANES/
#     Continuous NHANES/
#     NHANES Ancillary Studies/
#     NHANES Prior to 1999/
#     Other NHANES/
#
#   data/cleaned/NHANES/
#     Continuous NHANES/
#     NHANES Ancillary Studies/
#     NHANES Prior to 1999/
#     Other NHANES/
#
# Notes:
# - Continuous NHANES is organized by cycle and component.
# - NNYFS is organized by component.
# - NHEFS is kept flat inside its survey folder.
# - NHANES III keeps release subfolders because the source site is release-based.
# - Legacy fixed-width TXT + SAS setup pairs are converted where possible.

source("R/utils.R")

suppressPackageStartupMessages({
  library(xml2)
  library(rvest)
  library(nhanesA)
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

RAW_ROOT   <- file.path("data", "raw", "NHANES")
CLEAN_ROOT <- file.path("data", "cleaned", "NHANES")

RUN_CONTINUOUS <- !identical(tolower(Sys.getenv("NHANES_RUN_CONTINUOUS", "true")), "false")
RUN_LEGACY     <- !identical(tolower(Sys.getenv("NHANES_RUN_LEGACY", "true")), "false")
WRITE_CSV      <- !identical(tolower(Sys.getenv("NHANES_WRITE_CSV", "true")), "false")
CRAWL_MAX_DEPTH <- suppressWarnings(as.integer(Sys.getenv("NHANES_CRAWL_MAX_DEPTH", "2")))
if (is.na(CRAWL_MAX_DEPTH)) CRAWL_MAX_DEPTH <- 2L

dir.create(RAW_ROOT, recursive = TRUE, showWarnings = FALSE)
dir.create(CLEAN_ROOT, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

dir_ok <- function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE)

safe_name <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("[/\\:*?\"<>|]+", "_", x)
  x <- gsub("\\s+", " ", x)
  x
}

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

coalesce_chr <- function(...) {
  vals <- list(...)
  out <- as.character(vals[[1]])
  for (i in seq_along(vals)) {
    cur <- as.character(vals[[i]])
    take <- (is.na(out) | out == "") & !(is.na(cur) | cur == "")
    out[take] <- cur[take]
  }
  out
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

top_folder_name <- function(group) {
  dplyr::case_when(
    group == "continuous" ~ "Continuous NHANES",
    group == "ancillary" ~ "NHANES Ancillary Studies",
    group == "prior_1999" ~ "NHANES Prior to 1999",
    TRUE ~ "Other NHANES"
  )
}

continuous_cycle_folder <- function(x) {
  x <- trimws(as.character(x))
  dplyr::case_when(
    x %in% c("2025-2026", "2025/2026") ~ "NHANES 2025-2026",
    x %in% c("2021-2023", "08/2021-08/2023") ~ "NHANES 08/2021-08/2023",
    x %in% c("2017-2020", "2017-March 2020") ~ "NHANES 2017-March 2020 Pre-Pandemic Data",
    x == "2019-2020" ~ "NHANES 2019-2020",
    x == "2017-2018" ~ "NHANES 2017-2018",
    x == "2015-2016" ~ "NHANES 2015-2016",
    x == "2013-2014" ~ "NHANES 2013-2014",
    x == "2011-2012" ~ "NHANES 2011-2012",
    x == "2009-2010" ~ "NHANES 2009-2010",
    x == "2007-2008" ~ "NHANES 2007-2008",
    x == "2005-2006" ~ "NHANES 2005-2006",
    x == "2003-2004" ~ "NHANES 2003-2004",
    x == "2001-2002" ~ "NHANES 2001-2002",
    x == "1999-2000" ~ "NHANES 1999-2000",
    TRUE ~ paste("NHANES", x)
  )
}

continuous_component_folder <- function(x) {
  x2 <- toupper(as.character(x))
  dplyr::case_when(
    str_detect(x2, "DEMO|DEMOGRAPH") ~ "Demographics Data",
    str_detect(x2, "DIET|DR1|DR2|DBQ|DSQ") ~ "Dietary Data",
    str_detect(x2, "EXAM|BMX|BPX|AUX|DXX|DXA|OHX|OPX|VIX") ~ "Examination Data",
    str_detect(x2, "LAB|LBX|LBD|BIOPRO|SS|PFC|VID|TRIGLY|HDL|TCHOL") ~ "Laboratory Data",
    str_detect(x2, "Q|RHQ|MCQ|HUQ|PAQ|SMQ|SLQ|ALQ|DPQ|OCQ|WHQ|KIQ|HSQ") ~ "Questionnaire Data",
    TRUE ~ "Other Data"
  )
}

nhanes_public_manifest_fallback <- function(component = NULL) {
  data_url <- "https://wwwn.cdc.gov/Nchs/Nhanes/search/DataPage.aspx"
  if (!is.null(component) && nzchar(component)) {
    data_url <- paste0(
      data_url,
      "?Component=",
      utils::URLencode(component, reserved = TRUE)
    )
  }

  page <- xml2::read_html(data_url)
  rows <- rvest::html_elements(page, xpath = "//*[@id='GridView1']/tbody/tr")
  if (length(rows) == 0) {
    stop("Fallback CDC NHANES manifest scrape returned no rows.", call. = FALSE)
  }

  parse_row <- function(row) {
    cells <- rvest::html_elements(row, "td")
    if (length(cells) < 4) return(NULL)

    years <- rvest::html_text2(cells[[1]])
    doc_file <- rvest::html_text2(cells[[2]])
    data_file <- rvest::html_text2(cells[[3]])
    date_published <- rvest::html_text2(cells[[4]])

    links <- rvest::html_elements(row, "a")
    hrefs <- rvest::html_attr(links, "href")
    hrefs[!nzchar(hrefs)] <- NA_character_
    hrefs <- ifelse(
      !is.na(hrefs) & startsWith(hrefs, "/"),
      paste0("https://wwwn.cdc.gov", hrefs),
      hrefs
    )

    doc_url <- hrefs[grepl("\\.htm(l)?($|\\?)", hrefs, ignore.case = TRUE)][1]
    xpt_url <- hrefs[grepl("\\.xpt($|\\?)", hrefs, ignore.case = TRUE)][1]
    if (length(doc_url) == 0 || is.na(doc_url)) doc_url <- NA_character_
    if (length(xpt_url) == 0 || is.na(xpt_url)) xpt_url <- NA_character_

    table_name <- if (!is.na(xpt_url) && nzchar(xpt_url)) {
      tools::file_path_sans_ext(basename(xpt_url))
    } else {
      sub("[[:space:]]+Doc$", "", doc_file)
    }

    tibble::tibble(
      table_name = table_name,
      data_url = xpt_url,
      component = if (!is.null(component) && nzchar(component)) component else NA_character_,
      cycle = years,
      doc_url = doc_url,
      description = data_file,
      date_published = date_published
    )
  }

  bind_rows(purrr::map(rows, parse_row)) %>%
    filter(!is.na(.data$table_name), nzchar(.data$table_name)) %>%
    filter(!is.na(.data$data_url), nzchar(.data$data_url)) %>%
    filter(.data$date_published != "Withdrawn")
}

get_continuous_manifest <- function() {
  out <- tryCatch({
    mf <- nhanesA::nhanesManifest("public", sizes = TRUE, use_cache = TRUE)
    nms <- names(mf)
    col_table <- nms[nms %in% c("Table", "Data.File.Name", "Data File Name")][1]
    col_url   <- nms[nms %in% c("DataURL", "Data.Url", "Data URL", "DataURL.x")][1]
    col_comp  <- nms[nms %in% c("Component", "Data Category")][1]
    col_cycle <- nms[nms %in% c("Cycle", "Survey.Cycle", "Year.Range", "Years")][1]

    tibble::tibble(
      table_name = as.character(mf[[col_table]]),
      data_url   = as.character(mf[[col_url]]),
      component  = if (!is.na(col_comp)) as.character(mf[[col_comp]]) else NA_character_,
      cycle      = if (!is.na(col_cycle)) as.character(mf[[col_cycle]]) else NA_character_
    )
  }, error = function(e) {
    message("nhanesA manifest failed: ", conditionMessage(e))
    message("Falling back to direct CDC NHANES page scrape.")

    components <- c(
      "Demographics",
      "Dietary",
      "Examination",
      "Laboratory",
      "Questionnaire"
    )

    bind_rows(lapply(components, nhanes_public_manifest_fallback))
  })

  out %>%
    mutate(
      table_name = as.character(.data$table_name),
      data_url   = as.character(.data$data_url),
      component  = coalesce_chr(as.character(.data$component), "Other"),
      cycle      = coalesce_chr(as.character(.data$cycle), "unspecified")
    ) %>%
    filter(!is.na(.data$data_url), nzchar(.data$data_url))
}

nhanes_registry <- function() {
  tibble::tribble(
    ~survey_id, ~survey_label, ~group, ~entry_url,
    "nhanes_iii", "NHANES III", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhanes3/default.aspx",
    "nhanes_ii", "NHANES II", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhanes2/default.aspx",
    "nhanes_i", "NHANES I", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhanes1/default.aspx",
    "hhanes", "Hispanic HANES", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/hhanes/default.aspx",
    "nhes_i", "NHES I", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhes1/default.aspx",
    "nhes_ii", "NHES II", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhes2/default.aspx",
    "nhes_iii", "NHES III", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhes3/default.aspx",
    "nhefs", "NHEFS", "ancillary", "https://wwwn.cdc.gov/nchs/nhanes/nhefs/default.aspx",
    "nnyfs_2012", "NNYFS", "ancillary", "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfs12.aspx"
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
    nnyfs_2012 = c("/nchs/nhanes/search/nnyfs12", "/nchs/nhanes/search/nnyfsdata\\.aspx\\?Component=", "/nchs/data/nnyfs/"),
    character(0)
  )
}

in_survey_scope <- function(url, survey_id) {
  pats <- survey_scope_patterns(survey_id)
  if (length(pats) == 0) return(rep(FALSE, length(url)))

  url <- as.character(url)
  valid <- !(is.na(url) | !nzchar(url))
  out <- rep(FALSE, length(url))
  out[valid] <- vapply(
    url[valid],
    function(u) any(vapply(pats, function(p) grepl(p, u, ignore.case = TRUE), logical(1))),
    logical(1)
  )
  out
}

classify_link_type <- function(url, text) {
  u <- tolower(coalesce(url, ""))
  t <- tolower(coalesce(text, ""))

  dplyr::case_when(
    str_detect(u, "\\.xpt($|\\?)") ~ "data_xpt",
    str_detect(u, "\\.dat($|\\?)") ~ "data_dat",
    str_detect(u, "\\.csv($|\\?)") ~ "data_csv",
    str_detect(u, "\\.zip($|\\?)") ~ "data_zip",
    str_detect(u, "\\.sas($|\\?)") ~ "code_sas",
    str_detect(u, "\\.txt($|\\?)") & str_detect(t, "input|label|setup|sas") ~ "code_txt",
    str_detect(u, "\\.txt($|\\?)") ~ "data_txt",
    str_detect(u, "\\.pdf($|\\?)") ~ "doc_pdf",
    str_detect(u, "\\.aspx($|\\?)|\\.htm(l)?($|\\?)") ~ "html_page",
    TRUE ~ "other"
  )
}

is_downloadable_asset <- function(link_type) {
  link_type %in% c("data_xpt", "data_dat", "data_txt", "data_csv", "data_zip", "code_sas", "code_txt", "doc_pdf")
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

survey_relative_path <- function(url, survey_id, parent_page = NA_character_) {
  path <- tolower(sub("^https?://[^/]+", "", as.character(url)))
  path <- sub("\\?.*$", "", path)
  path <- utils::URLdecode(path)
  path <- gsub("^/+", "", path)

  rel <- switch(
    survey_id,
    nhanes_iii = sub("^nchs/data/nhanes3/?", "", path),
    nhanes_i = sub("^nchs/data/nhanes1/?", "data/", path),
    nhanes_ii = sub("^nchs/data/nhanes2/?", "data/", path),
    hhanes = sub("^nchs/data/hhanes/?", "data/", path),
    nhes_i = sub("^nchs/data/nhes123/?", "data/", sub("^nchs/data/nhes1/?", "data/", path)),
    nhes_ii = sub("^nchs/data/nhes123/?", "data/", sub("^nchs/data/nhes2/?", "data/", path)),
    nhes_iii = sub("^nchs/data/nhes123/?", "data/", sub("^nchs/data/nhes3/?", "data/", path)),
    nhefs = basename(path),
    nnyfs_2012 = {
      if (grepl("^nchs/data/nnyfs/public/2012/datafiles/", path)) {
        file.path(infer_nnyfs_component_dir(parent_page), basename(path))
      } else {
        basename(path)
      }
    },
    basename(path)
  )

  rel <- gsub("^/+", "", rel)
  gsub("/", .Platform$file.sep, rel, fixed = TRUE)
}

# ------------------------------------------------------------------------------
# FIXED-WIDTH PARSING
# ------------------------------------------------------------------------------

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

  tibble::tibble(var = hits[, 2], is_char = hits[, 3] == "$", width = as.integer(hits[, 4])) %>%
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

  at_hits <- stringr::str_match_all(text, "@\\s*(\\d+)\\s+([A-Za-z_][A-Za-z0-9_]*)\\s+(\\$?[A-Za-z]+)?(\\d+)\\.")[[1]]
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

  tibble::tibble(var = character(), start = integer(), end = integer(), is_char = logical())
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

  tibble::tibble(var = hits[, 2], label = hits[, 3]) %>%
    distinct(.data$var, .keep_all = TRUE)
}

infer_sas_infile_target <- function(lines) {
  infile_hit <- stringr::str_match(paste(lines, collapse = "\n"), "(?i)infile\\s+['\"]([^'\"]+\\.txt)['\"]")
  if (!is.na(infile_hit[1, 2]) && nzchar(infile_hit[1, 2])) return(basename(infile_hit[1, 2]))

  filename_hit <- stringr::str_match(paste(lines, collapse = "\n"), "(?i)filename\\s+(?:\\w+\\s+)?in\\s+['\"]([^'\"]+\\.txt)['\"]")
  if (!is.na(filename_hit[1, 2]) && nzchar(filename_hit[1, 2])) return(basename(filename_hit[1, 2]))

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

  fwf <- readr::fwf_positions(start = layout$start, end = layout$end, col_names = layout$var)
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
  numeric_vars <- layout %>% filter(!.data$is_char, .data$var %in% names(df)) %>% pull(.data$var)
  for (nm in numeric_vars) {
    x <- trimws(df[[nm]])
    x[x %in% c("", ".")] <- NA_character_
    df[[nm]] <- suppressWarnings(readr::parse_double(x, na = c("", ".")))
  }

  labels <- parse_sas_labels(lines)
  for (i in seq_len(nrow(labels))) {
    nm <- labels$var[[i]]
    if (nm %in% names(df)) attr(df[[nm]], "label") <- labels$label[[i]]
  }

  list(data = df, labels = labels)
}

convert_raw_tree <- function(raw_dir, clean_dir, survey_id = NA_character_) {
  files <- list.files(raw_dir, recursive = TRUE, full.names = TRUE)
  files <- files[file.info(files)$isdir %in% FALSE]
  if (length(files) == 0) return(invisible(NULL))

  # Directly readable files
  for (src in files[grepl("\\.(xpt|csv)$", files, ignore.case = TRUE)]) {
    rel_dir <- dirname(src)
    rel_dir <- sub(paste0("^", gsub("\\\\", "\\\\\\\\", raw_dir)), "", rel_dir)
    rel_dir <- sub("^[/\\\\]+", "", rel_dir)
    out_dir <- if (nzchar(rel_dir)) file.path(clean_dir, rel_dir) else clean_dir
    stem <- tools::file_path_sans_ext(basename(src))
    ext <- tolower(tools::file_ext(src))

    try({
      if (ext == "xpt") {
        df <- haven::read_xpt(src)
      } else {
        df <- readr::read_csv(src, show_col_types = FALSE, progress = FALSE)
      }
      write_named_sidecars(df, out_dir, stem, write_csv = WRITE_CSV)
    }, silent = TRUE)
  }

  # Fixed-width TXT + SAS/TXT setup files
  scripts <- files[grepl("\\.sas$|\\.inputs\\.labels\\.txt$", files, ignore.case = TRUE)]
  if (length(scripts) == 0) return(invisible(NULL))

  for (script_path in scripts) {
    lines <- tryCatch(readLines(script_path, warn = FALSE, encoding = "UTF-8"), error = function(e) character())
    txt_name <- infer_sas_infile_target(lines)
    if ((is.na(txt_name) || !nzchar(txt_name)) && identical(as.character(survey_id), "nhefs")) {
      txt_name <- nhefs_script_txt_map(script_path)
    }
    if (is.na(txt_name) || !nzchar(txt_name)) next

    candidates <- files[tolower(basename(files)) == tolower(txt_name)]
    if (length(candidates) == 0) next
    txt_path <- candidates[[1]]

    rel_dir <- dirname(txt_path)
    rel_dir <- sub(paste0("^", gsub("\\\\", "\\\\\\\\", raw_dir)), "", rel_dir)
    rel_dir <- sub("^[/\\\\]+", "", rel_dir)
    out_dir <- if (nzchar(rel_dir)) file.path(clean_dir, rel_dir) else clean_dir
    stem <- tools::file_path_sans_ext(basename(txt_path))

    try({
      parsed <- read_fixed_width_from_sas(txt_path, script_path)
      write_named_sidecars(parsed$data, out_dir, stem, write_csv = WRITE_CSV)
      if (nrow(parsed$labels) > 0) {
        readr::write_csv(parsed$labels, file.path(out_dir, paste0(stem, "_variables.csv")))
      }
    }, silent = TRUE)
  }

  invisible(NULL)
}

# ------------------------------------------------------------------------------
# CONTINUOUS NHANES
# ------------------------------------------------------------------------------

build_continuous_nhanes <- function() {
  message("Fetching continuous NHANES public manifest...")

  tables <- get_continuous_manifest() %>%
    filter(!is.na(.data$data_url), nzchar(.data$data_url)) %>%
    mutate(
      cycle_folder = safe_name(continuous_cycle_folder(.data$cycle)),
      component_folder = safe_name(continuous_component_folder(.data$component)),
      table_name = safe_name(.data$table_name)
    ) %>%
    distinct(.data$table_name, .keep_all = TRUE) %>%
    arrange(.data$cycle_folder, .data$component_folder, .data$table_name)

  raw_root <- file.path(RAW_ROOT, top_folder_name("continuous"))
  clean_root <- file.path(CLEAN_ROOT, top_folder_name("continuous"))
  dir_ok(raw_root)
  dir_ok(clean_root)

  write.csv(tables, file.path(raw_root, "continuous_manifest.csv"), row.names = FALSE)

  for (i in seq_len(nrow(tables))) {
    r <- tables[i, , drop = FALSE]
    raw_path <- file.path(raw_root, r$cycle_folder[[1]], r$component_folder[[1]], paste0(r$table_name[[1]], ".XPT"))
    clean_dir <- file.path(clean_root, r$cycle_folder[[1]], r$component_folder[[1]])

    message(sprintf("Continuous [%d/%d] %s", i, nrow(tables), r$table_name[[1]]))
    try({
      download_if_needed(r$data_url[[1]], raw_path)
      df <- haven::read_xpt(raw_path)
      write_named_sidecars(df, clean_dir, r$table_name[[1]], write_csv = WRITE_CSV)
    }, silent = TRUE)
  }
}

# ------------------------------------------------------------------------------
# LEGACY / ANCILLARY CRAWL
# ------------------------------------------------------------------------------

crawl_survey_assets <- function(reg_row) {
  sid <- reg_row$survey_id[[1]]
  survey_label <- reg_row$survey_label[[1]]
  group_root_raw <- file.path(RAW_ROOT, top_folder_name(reg_row$group[[1]]))
  survey_raw_dir <- file.path(group_root_raw, safe_name(survey_label))

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
  rows <- list()

  while (nrow(queue) > 0) {
    current <- queue[1, , drop = FALSE]
    queue <- queue[-1, , drop = FALSE]
    page_url <- current$url[[1]]
    depth <- current$depth[[1]]

    if (page_url %in% visited) next
    visited <- c(visited, page_url)

    page <- tryCatch(xml2::read_html(page_url), error = function(e) NULL)
    if (is.null(page)) next

    anchors <- rvest::html_elements(page, "a")
    if (length(anchors) == 0) next

    page_links <- tibble::tibble(
      parent_page = page_url,
      link_text = trimws(rvest::html_text2(anchors)),
      href = rvest::html_attr(anchors, "href")
    ) %>%
      mutate(
        asset_url = vapply(.data$href, absolute_url, FUN.VALUE = character(1), base_url = page_url),
        link_type = classify_link_type(.data$asset_url, .data$link_text)
      ) %>%
      filter(!is_blank(.data$asset_url)) %>%
      filter(in_survey_scope(.data$asset_url, sid)) %>%
      distinct(.data$asset_url, .keep_all = TRUE)

    if (nrow(page_links) == 0) next
    rows[[length(rows) + 1]] <- page_links

    if (depth < CRAWL_MAX_DEPTH) {
      next_pages <- page_links %>%
        filter(.data$link_type %in% c("html_page", "other")) %>%
        pull(.data$asset_url) %>%
        unique()
      if (length(next_pages) > 0) {
        queue <- bind_rows(queue, tibble::tibble(url = next_pages, depth = depth + 1L))
      }
    }
  }

  assets <- bind_rows(rows)
  if (nrow(assets) == 0) return(tibble::tibble())

  assets %>%
    mutate(
      is_downloadable = is_downloadable_asset(.data$link_type),
      local_path = ifelse(
        .data$is_downloadable,
        file.path(
          survey_raw_dir,
          vapply(
            seq_along(.data$asset_url),
            function(i) survey_relative_path(.data$asset_url[[i]], sid, .data$parent_page[[i]]),
            character(1)
          )
        ),
        NA_character_
      )
    ) %>%
    select("parent_page", "link_text", "asset_url", "link_type", "is_downloadable", "local_path")
}

build_noncontinuous_survey <- function(reg_row) {
  survey_label <- reg_row$survey_label[[1]]
  survey_id <- reg_row$survey_id[[1]]
  group_root_raw <- file.path(RAW_ROOT, top_folder_name(reg_row$group[[1]]))
  group_root_clean <- file.path(CLEAN_ROOT, top_folder_name(reg_row$group[[1]]))
  raw_dir <- file.path(group_root_raw, safe_name(survey_label))
  clean_dir <- file.path(group_root_clean, safe_name(survey_label))

  dir_ok(raw_dir)
  dir_ok(clean_dir)

  message("Crawling ", survey_label, "...")
  assets <- crawl_survey_assets(reg_row)
  if (nrow(assets) > 0) {
    downloads <- assets %>% filter(.data$is_downloadable, !is.na(.data$local_path), nzchar(.data$local_path))
    for (i in seq_len(nrow(downloads))) {
      r <- downloads[i, , drop = FALSE]
      message(sprintf("%s [%d/%d] %s", survey_label, i, nrow(downloads), basename(r$local_path[[1]])))
      try(download_if_needed(r$asset_url[[1]], r$local_path[[1]]), silent = TRUE)
    }
    write.csv(assets, file.path(raw_dir, "asset_inventory.csv"), row.names = FALSE)
    write.csv(assets, file.path(clean_dir, "asset_inventory.csv"), row.names = FALSE)
  }

  convert_raw_tree(raw_dir, clean_dir, survey_id = survey_id)
}

# ------------------------------------------------------------------------------
# BUILD
# ------------------------------------------------------------------------------

write.csv(nhanes_registry(), file.path(RAW_ROOT, "nhanes_registry.csv"), row.names = FALSE)
write.csv(nhanes_registry(), file.path(CLEAN_ROOT, "nhanes_registry.csv"), row.names = FALSE)

if (isTRUE(RUN_CONTINUOUS)) {
  build_continuous_nhanes()
}

if (isTRUE(RUN_LEGACY)) {
  reg <- nhanes_registry()
  for (i in seq_len(nrow(reg))) {
    build_noncontinuous_survey(reg[i, , drop = FALSE])
  }
}

message("Done.")
