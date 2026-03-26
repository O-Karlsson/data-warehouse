# R/build_nhanes_public.R
#
# PURPOSE
# -------
# Download continuous-era public NHANES data tables that are exposed as public
# SAS transport (.XPT) files, store the original files in data/raw/NHANES/,
# and create warehouse-style minimally cleaned outputs in data/cleaned/NHANES/.
#
# Minimal cleaning only:
#   - keep source table structure as-is (no harmonization, no merging, no reshaping)
#   - read .XPT and write <table>.parquet, <table>.dta, <table>.csv
#   - preserve original NHANES variable names
#   - create metadata inventories and lightweight documentation
#
# Notes
# -----
# - Table discovery is automatic via nhanesA::nhanesManifest("public").
# - URLs do NOT need to be hard-coded manually.
# - Harmonization across cycles/files should be done later in project-specific code.
# - This script is intended for continuous/public NHANES XPT data tables only.
# - Legacy NHANES/NHES/Hispanic HANES, NHEFS, and NNYFS are inventoried separately
#   by R/build_nhanes_family.R because they do not share one common ingest model.
#
# Output structure
# ----------------
# data/raw/NHANES/Continuous NHANES/
#   metadata_raw.csv
#   SOURCES.md
#   <cycle>/<component>/<table>/<table>.XPT
#
# data/cleaned/NHANES/Continuous NHANES/
#   metadata.csv
#   variable_inventory.csv
#   DATASET_NOTES.md
#   <cycle>/<component>/<table>/
#     <table>.parquet
#     <table>.dta
#     <table>.csv
#     DATASET_NOTES.md
#     variables_info.csv
#     _archive/

source("R/utils.R")

suppressPackageStartupMessages({
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

FORCE_REDOWNLOAD <- FALSE
FORCE_REBUILD    <- FALSE

WRITE_PARQUET <- TRUE
WRITE_DTA     <- TRUE
WRITE_CSV     <- TRUE

RAW_ROOT   <- file.path("data", "raw", "NHANES", "Continuous NHANES")
CLEAN_ROOT <- file.path("data", "cleaned", "NHANES", "Continuous NHANES")

# If TRUE, keep only standard public .XPT data tables that can be downloaded from
# nhanesA manifests. Documentation and codebooks are represented in metadata/docs,
# but not mirrored as separate HTML/PDF files.
DATA_ONLY <- TRUE

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

pick_col <- function(df, candidates, required = TRUE) {
  nms <- names(df)
  hit <- nms[nms %in% candidates]
  if (length(hit) > 0) return(hit[[1]])

  # fallback: ignore punctuation/case
  norm <- function(x) tolower(gsub("[^a-z0-9]", "", x))
  idx <- match(norm(candidates), norm(nms), nomatch = 0)
  idx <- idx[idx > 0]
  if (length(idx) > 0) return(nms[idx[[1]]])

  if (required) {
    stop(
      "Could not find any of these columns in manifest: ",
      paste(candidates, collapse = ", "),
      call. = FALSE
    )
  }
  NULL
}

na_chr <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x
}

coalesce_chr <- function(...) {
  vals <- list(...)
  out <- vals[[1]]
  for (i in seq_along(vals)) {
    cur <- vals[[i]]
    take <- (is.na(out) | out == "") & !(is.na(cur) | cur == "")
    out[take] <- cur[take]
  }
  out
}

read_local_public_manifest <- function(path = file.path(RAW_ROOT, "metadata_raw.csv")) {
  if (!file.exists(path)) {
    stop("Local continuous NHANES metadata_raw.csv fallback not found.", call. = FALSE)
  }

  out <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE) %>%
    mutate(across(everything(), as.character))

  required <- c("table_name", "data_url", "component", "cycle")
  missing <- setdiff(required, names(out))
  if (length(missing) > 0) {
    stop(
      "Local continuous NHANES metadata_raw.csv is missing required columns: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  out
}

read_local_variable_manifest <- function(path = file.path(CLEAN_ROOT, "variable_inventory.csv")) {
  if (!file.exists(path)) {
    stop("Local continuous NHANES variable_inventory.csv fallback not found.", call. = FALSE)
  }

  out <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE) %>%
    mutate(across(everything(), as.character))

  required <- c("table_name", "var_name")
  missing <- setdiff(required, names(out))
  if (length(missing) > 0) {
    stop(
      "Local continuous NHANES variable_inventory.csv is missing required columns: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  out
}

infer_cycle_from_table <- function(tbl) {
  tbl <- as.character(tbl)
  sfx <- str_extract(tbl, "_[A-Z0-9]+$")
  ifelse(is.na(sfx), "unspecified", str_remove(sfx, "^_"))
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

infer_component_from_name <- function(x) {
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

nhanes_public_manifest_fallback <- function(sizes = TRUE, component = NULL) {
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
    stop("Fallback NHANES manifest scrape returned no table rows.", call. = FALSE)
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
    data_url <- hrefs[grepl("\\.xpt($|\\?)", hrefs, ignore.case = TRUE)][1]

    if (length(doc_url) == 0 || is.na(doc_url)) doc_url <- NA_character_
    if (length(data_url) == 0 || is.na(data_url)) data_url <- NA_character_

    table_name <- if (!is.na(data_url) && nzchar(data_url)) {
      tools::file_path_sans_ext(basename(data_url))
    } else {
      sub("[[:space:]]+Doc$", "", doc_file)
    }
    file_size <- stringr::str_match(data_file, "\\[XPT\\s*-\\s*([^\\]]+)\\]")[, 2]

    tibble::tibble(
      Table = table_name,
      Description = data_file,
      DocURL = doc_url,
      DataURL = data_url,
      Years = years,
      Date.Published = date_published,
      Component = if (!is.null(component)) component else NA_character_,
      Data.File.Size = if (sizes) file_size else NA_character_
    )
  }

  bind_rows(purrr::map(rows, parse_row)) %>%
    filter(!is.na(.data$Table), nzchar(.data$Table)) %>%
    filter(.data$Date.Published != "Withdrawn")
}

manifest_with_urls <- function() {
  out <- tryCatch({
    mf <- tryCatch(
      nhanesA::nhanesManifest("public", sizes = TRUE, use_cache = TRUE),
      error = function(e) {
        message("nhanesA::nhanesManifest('public') failed: ", conditionMessage(e))
        message("Falling back to direct scrape of the CDC NHANES data page.")
        nhanes_public_manifest_fallback(sizes = TRUE)
      }
    )
    mf <- as.data.frame(mf, stringsAsFactors = FALSE)

    # Try to identify the standard columns, but be forgiving about names.
    col_table <- pick_col(mf, c("Table.Name", "Data.File.Name", "FileName", "table", "Table"))
    col_url   <- pick_col(mf, c("Data.File.URL", "Data File URL", "URL", "url", "DataURL"))
    col_doc   <- pick_col(mf, c("Doc.File.URL", "Documentation.URL", "DocURL", "doc_url"), required = FALSE)
    col_comp  <- pick_col(mf, c("Component", "component", "Data.Group", "data_group", "Group"), required = FALSE)
    col_cycle <- pick_col(mf, c("Cycle", "Survey.Cycle", "Year.Range", "Begin.End.Year", "Years", "SurveyYears"), required = FALSE)
    col_begin <- pick_col(mf, c("Begin.Year", "BeginYear", "Start.Year", "StartYear"), required = FALSE)
    col_end   <- pick_col(mf, c("End.Year", "EndYear", "Stop.Year", "StopYear"), required = FALSE)
    col_nrow  <- pick_col(mf, c("nrows", "Rows", "N.Obs", "nobs", "nrow"), required = FALSE)
    col_size  <- pick_col(mf, c("Data.File.Size", "File.Size", "size", "DataSize"), required = FALSE)

    tibble::tibble(
      table_name = as.character(mf[[col_table]]),
      data_url   = as.character(mf[[col_url]]),
      doc_url    = if (!is.null(col_doc))  as.character(mf[[col_doc]])  else NA_character_,
      component  = if (!is.null(col_comp)) as.character(mf[[col_comp]]) else NA_character_,
      cycle      = if (!is.null(col_cycle)) as.character(mf[[col_cycle]]) else NA_character_,
      begin_year = if (!is.null(col_begin)) as.character(mf[[col_begin]]) else NA_character_,
      end_year   = if (!is.null(col_end))   as.character(mf[[col_end]])   else NA_character_,
      nrows      = if (!is.null(col_nrow))  as.character(mf[[col_nrow]])  else NA_character_,
      file_size  = if (!is.null(col_size))  as.character(mf[[col_size]])  else NA_character_
    )
  }, error = function(e) {
    message("Live continuous manifest discovery failed: ", conditionMessage(e))
    message("Falling back to existing local metadata_raw.csv inventory.")
    read_local_public_manifest()
  })

  out$cycle <- coalesce_chr(
    na_chr(out$cycle),
    ifelse(nzchar(na_chr(out$begin_year)) & nzchar(na_chr(out$end_year)),
           paste0(out$begin_year, "-", out$end_year), ""),
    infer_cycle_from_table(out$table_name)
  )

  out$component <- coalesce_chr(na_chr(out$component), infer_component_from_name(out$table_name))
  out$component <- ifelse(out$component == "", "Other", out$component)
  out$cycle     <- ifelse(out$cycle == "", "unspecified", out$cycle)

  out
}

download_if_needed <- function(url, dest, force = FALSE) {
  if (!force && file.exists(dest) && isTRUE(file.info(dest)$size > 0)) return(invisible(TRUE))

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

write_sidecars_with_archive <- function(df, out_dir, file_stem) {
  arch_dir <- file.path(out_dir, "_archive")
  dir_ok(arch_dir)

  out_parquet_final <- file.path(out_dir, paste0(file_stem, ".parquet"))
  out_parquet_tmp   <- tempfile(fileext = ".parquet")

  if (WRITE_PARQUET) {
    arrow::write_parquet(df, out_parquet_tmp)

    res <- archive_and_promote(
      final_path  = out_parquet_final,
      tmp_path    = out_parquet_tmp,
      archive_dir = arch_dir
    )
  } else {
    res <- list(changed = TRUE)
  }

  if (isTRUE(res$changed)) {
    tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")

    if (WRITE_DTA && file.exists(file.path(out_dir, paste0(file_stem, ".dta")))) {
      file.copy(
        file.path(out_dir, paste0(file_stem, ".dta")),
        file.path(arch_dir, paste0(file_stem, "_", tag, ".dta")),
        overwrite = FALSE
      )
    }

    if (WRITE_CSV && file.exists(file.path(out_dir, paste0(file_stem, ".csv")))) {
      file.copy(
        file.path(out_dir, paste0(file_stem, ".csv")),
        file.path(arch_dir, paste0(file_stem, "_", tag, ".csv")),
        overwrite = FALSE
      )
    }
  }

  if (WRITE_DTA) haven::write_dta(df, file.path(out_dir, paste0(file_stem, ".dta")))
  if (WRITE_CSV) readr::write_csv(df, file.path(out_dir, paste0(file_stem, ".csv")))

  invisible(res)
}

build_var_dict <- function(table_name, vars_manifest, df) {
  vm <- vars_manifest %>%
    filter(.data$table_name == !!table_name) %>%
    distinct(.data$var_name, .keep_all = TRUE)

  if (nrow(vm) == 0) {
    return(data.frame(
      var = names(df),
      var_label = rep("", length(names(df))),
      notes = rep("", length(names(df))),
      stringsAsFactors = FALSE
    ))
  }

  out <- data.frame(
    var = names(df),
    stringsAsFactors = FALSE
  ) %>%
    left_join(
      vm %>% transmute(
        var = .data$var_name,
        var_label = coalesce_chr(na_chr(.data$sas_label), na_chr(.data$english_text)),
        notes = coalesce_chr(na_chr(.data$target), na_chr(.data$codebook_url))
      ),
      by = "var"
    )

  out$var_label[is.na(out$var_label)] <- ""
  out$notes[is.na(out$notes)] <- ""
  out
}

variables_manifest_clean <- function() {
  tryCatch({
    vm <- nhanesA::nhanesManifest("variables", use_cache = TRUE)
    vm <- as.data.frame(vm, stringsAsFactors = FALSE)

    col_table   <- pick_col(vm, c("Table.Name", "FileName", "table", "Table"))
    col_var     <- pick_col(vm, c("Variable.Name", "Variable Name", "varname", "Variable", "VarName"))
    col_label   <- pick_col(vm, c("SAS.Label", "SAS Label", "SASLabel", "Label"), required = FALSE)
    col_english <- pick_col(vm, c("English.Text", "English Text", "EnglishText", "Description"), required = FALSE)
    col_target  <- pick_col(vm, c("Target", "target"), required = FALSE)
    col_doc     <- pick_col(vm, c("Doc.File.URL", "Documentation.URL", "DocURL", "doc_url"), required = FALSE)

    tibble::tibble(
      table_name   = as.character(vm[[col_table]]),
      var_name     = as.character(vm[[col_var]]),
      sas_label    = if (!is.null(col_label)) as.character(vm[[col_label]]) else NA_character_,
      english_text = if (!is.null(col_english)) as.character(vm[[col_english]]) else NA_character_,
      target       = if (!is.null(col_target)) as.character(vm[[col_target]]) else NA_character_,
      codebook_url = if (!is.null(col_doc)) as.character(vm[[col_doc]]) else NA_character_
    )
  }, error = function(e) {
    message("Live variable manifest discovery failed: ", conditionMessage(e))
    message("Falling back to existing local variable_inventory.csv inventory.")
    read_local_variable_manifest()
  })
}

# ------------------------------------------------------------------------------
# SETUP
# ------------------------------------------------------------------------------

dir_ok(RAW_ROOT)
dir_ok(CLEAN_ROOT)

raw_meta_path <- file.path(RAW_ROOT, "metadata_raw.csv")
clean_meta_path <- file.path(CLEAN_ROOT, "metadata.csv")
var_inventory_path <- file.path(CLEAN_ROOT, "variable_inventory.csv")
failures_path <- file.path(CLEAN_ROOT, "build_failures.csv")

# ------------------------------------------------------------------------------
# GET MANIFESTS
# ------------------------------------------------------------------------------

message("Fetching NHANES public table manifest...")
tables <- manifest_with_urls()

if (DATA_ONLY) {
  tables <- tables %>%
    filter(!is.na(.data$data_url), nzchar(.data$data_url))
}

tables <- tables %>%
  mutate(
    cycle = safe_name(continuous_cycle_folder(.data$cycle)),
    component = safe_name(.data$component),
    table_name = safe_name(.data$table_name)
  ) %>%
  distinct(.data$table_name, .keep_all = TRUE) %>%
  arrange(.data$cycle, .data$component, .data$table_name)

write.csv(tables, raw_meta_path, row.names = FALSE)
message("Wrote: ", raw_meta_path)

message("Fetching NHANES variable manifest...")
var_manifest <- variables_manifest_clean()
write.csv(var_manifest, var_inventory_path, row.names = FALSE)
message("Wrote: ", var_inventory_path)

clean_meta_old <- if (file.exists(clean_meta_path)) {
  read.csv(clean_meta_path, stringsAsFactors = FALSE)
} else NULL

clean_rows <- list()
failure_rows <- list()

# ------------------------------------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------------------------------------

for (i in seq_len(nrow(tables))) {
  r <- tables[i, , drop = FALSE]

  table_name <- as.character(r$table_name)
  cycle      <- as.character(r$cycle)
  component  <- as.character(r$component)
  data_url   <- as.character(r$data_url)
  doc_url    <- as.character(r$doc_url)

  raw_dir   <- file.path(RAW_ROOT, cycle, component, table_name)
  clean_dir <- file.path(CLEAN_ROOT, cycle, component, table_name)
  dir_ok(raw_dir)
  dir_ok(clean_dir)

  raw_xpt_path <- file.path(raw_dir, paste0(table_name, ".XPT"))

  message(sprintf("[%d/%d] %s", i, nrow(tables), table_name))

  table_result <- tryCatch({
    # ---- Download raw XPT
    download_if_needed(data_url, raw_xpt_path, force = FORCE_REDOWNLOAD)

    # ---- Read XPT and write warehouse outputs
    need_rebuild <- FORCE_REBUILD ||
      (WRITE_PARQUET && !file.exists(file.path(clean_dir, paste0(table_name, ".parquet")))) ||
      (WRITE_DTA && !file.exists(file.path(clean_dir, paste0(table_name, ".dta")))) ||
      (WRITE_CSV && !file.exists(file.path(clean_dir, paste0(table_name, ".csv"))))

    if (need_rebuild) {
      df <- haven::read_xpt(raw_xpt_path)
      write_sidecars_with_archive(df, clean_dir, table_name)

      var_dict <- build_var_dict(table_name, var_manifest, df)
      vars_path <- file.path(clean_dir, "variables_info.csv")
      write.csv(var_dict, vars_path, row.names = FALSE, na = "")

      doc_warnings <- doc_warnings_for_var_dict(
        df,
        var_dict,
        dict_name = "variables_info.csv",
        df_name   = table_name
      )

      if (length(doc_warnings) > 0) {
        warning(
          paste(doc_warnings, collapse = "\n"),
          call. = FALSE
        )
      }
    }

    # ---- Per-table notes
    notes_path <- file.path(clean_dir, "DATASET_NOTES.md")
    notes_lines <- c(
      "# Dataset notes",
      "",
      sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
      "",
      paste0("- NHANES public-use table: `", table_name, "`."),
      paste0("- Cycle: ", cycle),
      paste0("- Component: ", component),
      paste0("- Raw source file stored as: `", raw_xpt_path, "`."),
      paste0("- Minimal cleaning only: XPT imported and re-exported as `", table_name, ".parquet`, `", table_name, ".dta`, and `", table_name, ".csv` when enabled."),
      "- No harmonization, reshaping, renaming, recoding, or merging was performed.",
      if (!is.na(doc_url) && nzchar(doc_url)) paste0("- Documentation/codebook URL: ", doc_url) else "- Documentation/codebook URL not captured in manifest."
    )
    writeLines(notes_lines, notes_path)

    tibble::tibble(
      table_name = table_name,
      cycle = cycle,
      component = component,
      data_url = data_url,
      doc_url = doc_url,
      raw_xpt = raw_xpt_path,
      cleaned_folder = clean_dir,
      parquet_file = if (WRITE_PARQUET) file.path(clean_dir, paste0(table_name, ".parquet")) else NA_character_,
      dta_file = if (WRITE_DTA) file.path(clean_dir, paste0(table_name, ".dta")) else NA_character_,
      csv_file = if (WRITE_CSV) file.path(clean_dir, paste0(table_name, ".csv")) else NA_character_,
      status = "success",
      error_message = NA_character_,
      built_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      stringsAsFactors = FALSE
    )
  }, error = function(e) {
    warning(
      sprintf("Failed to build NHANES table %s: %s", table_name, conditionMessage(e)),
      call. = FALSE
    )

    failure_rows[[length(failure_rows) + 1]] <<- tibble::tibble(
      table_name = table_name,
      cycle = cycle,
      component = component,
      data_url = data_url,
      doc_url = doc_url,
      raw_xpt = raw_xpt_path,
      cleaned_folder = clean_dir,
      error_message = conditionMessage(e),
      failed_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      stringsAsFactors = FALSE
    )

    tibble::tibble(
      table_name = table_name,
      cycle = cycle,
      component = component,
      data_url = data_url,
      doc_url = doc_url,
      raw_xpt = raw_xpt_path,
      cleaned_folder = clean_dir,
      parquet_file = if (WRITE_PARQUET) file.path(clean_dir, paste0(table_name, ".parquet")) else NA_character_,
      dta_file = if (WRITE_DTA) file.path(clean_dir, paste0(table_name, ".dta")) else NA_character_,
      csv_file = if (WRITE_CSV) file.path(clean_dir, paste0(table_name, ".csv")) else NA_character_,
      status = "failed",
      error_message = conditionMessage(e),
      built_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      stringsAsFactors = FALSE
    )
  })

  clean_rows[[length(clean_rows) + 1]] <- table_result
}

# ------------------------------------------------------------------------------
# WRITE CLEANED METADATA
# ------------------------------------------------------------------------------

if (length(clean_rows) > 0) {
  new_meta <- bind_rows(clean_rows)
  combined <- if (!is.null(clean_meta_old)) bind_rows(clean_meta_old, new_meta) else new_meta
  combined <- combined %>% distinct(.data$table_name, .keep_all = TRUE)
  write.csv(combined, clean_meta_path, row.names = FALSE)
  message("Updated: ", clean_meta_path)
}

if (length(failure_rows) > 0) {
  write.csv(bind_rows(failure_rows), failures_path, row.names = FALSE)
  message("Wrote failures log: ", failures_path)
}

# ------------------------------------------------------------------------------
# ROOT DOCUMENTATION
# ------------------------------------------------------------------------------

sources_md <- file.path(RAW_ROOT, "SOURCES.md")
writeLines(
  c(
    "NHANES public-use data mirrored from CDC.",
    "Access point: https://wwwn.cdc.gov/nchs/nhanes/",
    "Comprehensive data list: https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx",
    "Files are discovered programmatically using nhanesA::nhanesManifest(\"public\") with a direct CDC page fallback if the package parser fails.",
    "Original raw files are stored in SAS transport (.XPT) format.",
    paste0("Generated on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
  ),
  sources_md
)

notes_md <- file.path(CLEAN_ROOT, "DATASET_NOTES.md")
writeLines(
  c(
    "# NHANES dataset notes",
    "",
    sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "- This build mirrors continuous-era public NHANES data tables exposed through the modern public manifest/XPT workflow, with a direct CDC page scrape fallback when needed.",
    "- Raw files are stored unchanged as `.XPT` files under `data/raw/NHANES/Continuous NHANES/`.",
    "- Cleaned outputs are mechanical conversions only and preserve the original dataset stem, for example `BFRPOL_G.parquet`, `BFRPOL_G.dta`, and `BFRPOL_G.csv`.",
    "- No harmonization across cycles or components is performed here.",
    "- `metadata_raw.csv` is the table-level inventory from the public manifest.",
    "- `metadata.csv` is the table-level inventory of local cleaned outputs.",
    "- `variable_inventory.csv` is the cross-table variable manifest from NHANES.",
    "- Per-table `variables_info.csv` files are derived from the NHANES variable manifest and should be treated as lightweight documentation, not harmonized metadata.",
    "- Broader NHANES-family surveys such as NHANES I-III, Hispanic HANES, NHES I-III, NHEFS, and NNYFS are tracked separately by `R/build_nhanes_family.R`."
  ),
  notes_md
)

message("Done.")
