# R/build_nhanes_download_plan.R
#
# PURPOSE
# -------
# Build one editable CSV that proposes where NHANES-family public files should
# live locally before any downloads happen.
#
# Output columns include:
#   - local_folder
#   - file_name
#   - source_url
#   - top_folder
#   - survey
#   - component_or_release
#   - link_type
#   - suggested_cleaning
#   - source_page
#
# You can edit the CSV after it is created, then a later downloader can use your
# revised folder layout directly.

source("R/utils.R")

suppressPackageStartupMessages({
  library(xml2)
  library(rvest)
  library(dplyr)
  library(stringr)
  library(purrr)
  library(tibble)
  library(readr)
})

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

RAW_ROOT <- file.path("data", "raw", "NHANES")
PLAN_PATH <- file.path("data", "cleaned", "NHANES", "nhanes_download_plan.csv")
HOME_URL <- "https://wwwn.cdc.gov/nchs/nhanes/"
CRAWL_MAX_DEPTH <- suppressWarnings(as.integer(Sys.getenv("NHANES_PLAN_CRAWL_MAX_DEPTH", "2")))
if (is.na(CRAWL_MAX_DEPTH)) CRAWL_MAX_DEPTH <- 2L

dir.create(dirname(PLAN_PATH), recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

safe_name <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("[/\\:*?\"<>|]+", "_", x)
  x <- gsub("\\s+", " ", x)
  x
}

is_blank <- function(x) {
  is.na(x) | trimws(as.character(x)) == ""
}

has_url_fragment <- function(x) {
  grepl("#", as.character(x), fixed = TRUE)
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

clean_cell_text <- function(x) {
  x <- as.character(x)
  x <- gsub("[\r\n\t]+", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
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
    str_detect(t, "limited access|rdc only|research data center") ~ "limited_access",
    str_detect(u, "\\.aspx($|\\?)|\\.htm(l)?($|\\?)") ~ "html_page",
    TRUE ~ "other"
  )
}

suggested_cleaning <- function(link_type) {
  dplyr::case_when(
    link_type == "data_xpt" ~ "read_xpt -> parquet,dta,csv",
    link_type == "data_csv" ~ "read_csv -> parquet,dta,csv",
    link_type == "data_txt" ~ "needs setup file or survey-specific parser",
    link_type == "data_dat" ~ "needs setup file or survey-specific parser",
    link_type %in% c("code_sas", "code_txt") ~ "supporting setup file",
    link_type == "doc_pdf" ~ "documentation only",
    link_type == "data_zip" ~ "inspect archive contents",
    TRUE ~ "manual review"
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

nhanes_registry <- function() {
  tibble::tribble(
    ~survey_id, ~survey_label, ~group, ~entry_url,
    "nnyfs_2012", "NNYFS", "ancillary", "https://wwwn.cdc.gov/nchs/nhanes/search/nnyfs12.aspx",
    "nhefs", "NHEFS", "ancillary", "https://wwwn.cdc.gov/nchs/nhanes/nhefs/default.aspx",
    "hhanes", "Hispanic HANES", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/hhanes/default.aspx",
    "nhanes_iii", "NHANES III", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhanes3/default.aspx",
    "nhanes_ii", "NHANES II", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhanes2/default.aspx",
    "nhanes_i", "NHANES I", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhanes1/default.aspx",
    "nhes_iii", "NHES III", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhes3/default.aspx",
    "nhes_ii", "NHES II", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhes2/default.aspx",
    "nhes_i", "NHES I", "prior_1999", "https://wwwn.cdc.gov/nchs/nhanes/nhes1/default.aspx"
  )
}

continuous_component_labels <- function() {
  c(
    "Demographics Data",
    "Dietary Data",
    "Examination Data",
    "Laboratory Data",
    "Questionnaire Data"
  )
}

continuous_cycle_sources <- function() {
  tibble::tribble(
    ~survey, ~cycle_page,
    "NHANES 08/2021-08/2023", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?Cycle=2021-2023",
    "NHANES 2017-March 2020", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?Cycle=2017-2020",
    "NHANES 2019-2020", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2019",
    "NHANES 2017-2018", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2017",
    "NHANES 2015-2016", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2015",
    "NHANES 2013-2014", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2013",
    "NHANES 2011-2012", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2011",
    "NHANES 2009-2010", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2009",
    "NHANES 2007-2008", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2007",
    "NHANES 2005-2006", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2005",
    "NHANES 2003-2004", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2003",
    "NHANES 2001-2002", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=2001",
    "NHANES 1999-2000", "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear=1999"
  )
}

get_cycle_component_links <- function(cycle_page, survey_label) {
  page <- tryCatch(xml2::read_html(cycle_page), error = function(e) NULL)
  if (is.null(page)) return(tibble::tibble())

  anchors <- rvest::html_elements(page, "a")
  if (length(anchors) == 0) return(tibble::tibble())

  tibble::tibble(
    survey = survey_label,
    component_or_release = trimws(rvest::html_text2(anchors)),
    href = rvest::html_attr(anchors, "href")
  ) %>%
    mutate(source_page = vapply(.data$href, absolute_url, FUN.VALUE = character(1), base_url = cycle_page)) %>%
    filter(!is_blank(.data$source_page)) %>%
    filter(.data$component_or_release %in% continuous_component_labels()) %>%
    distinct(.data$survey, .data$component_or_release, .keep_all = TRUE)
}

get_home_continuous_component_pages <- function() {
  cycles <- continuous_cycle_sources()
  out <- vector("list", nrow(cycles))

  for (i in seq_len(nrow(cycles))) {
    survey_label <- cycles$survey[[i]]
    cycle_page <- cycles$cycle_page[[i]]
    message("Discovering continuous components: ", survey_label)

    rows <- get_cycle_component_links(cycle_page, survey_label)
    if (nrow(rows) > 0) {
      out[[i]] <- rows
      next
    }

    home <- xml2::read_html(HOME_URL)
    li <- rvest::html_element(
      home,
      xpath = sprintf("//a[normalize-space()='%s']/ancestor::li[1]", survey_label)
    )
    if (length(li) == 0 || is.na(li)) next

    anchors <- rvest::html_elements(li, "ul a")
    rows <- tibble::tibble(
      survey = survey_label,
      component_or_release = trimws(rvest::html_text2(anchors)),
      href = rvest::html_attr(anchors, "href")
    ) %>%
      filter(.data$component_or_release %in% components) %>%
      mutate(source_page = vapply(.data$href, absolute_url, FUN.VALUE = character(1), base_url = HOME_URL)) %>%
      filter(!is_blank(.data$source_page))

    out[[i]] <- rows
  }

  bind_rows(out) %>%
    distinct(.data$survey, .data$component_or_release, .keep_all = TRUE)
}

# ------------------------------------------------------------------------------
# CONTINUOUS NHANES PLAN
# ------------------------------------------------------------------------------

continuous_data_page_plan <- function(page_url, survey_label, component_label) {
  page <- xml2::read_html(page_url)
  rows <- rvest::html_elements(page, xpath = "//*[@id='GridView1']/tbody/tr")
  if (length(rows) == 0) {
    return(tibble::tibble())
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
      source_url = xpt_url,
      component = component_label,
      cycle = survey_label,
      source_page = page_url,
      description = data_file,
      date_published = date_published,
      link_type = "data_xpt"
    )
  }

  bind_rows(purrr::map(rows, parse_row)) %>%
    filter(!is.na(.data$table_name), nzchar(.data$table_name)) %>%
    filter(!is.na(.data$source_url), nzchar(.data$source_url)) %>%
    filter(.data$date_published != "Withdrawn")
}

get_continuous_plan <- function() {
  component_pages <- get_home_continuous_component_pages()

  out <- bind_rows(lapply(seq_len(nrow(component_pages)), function(i) {
    r <- component_pages[i, , drop = FALSE]
    message("Scraping continuous page: ", r$survey[[1]], " / ", r$component_or_release[[1]])
    continuous_data_page_plan(
      page_url = r$source_page[[1]],
      survey_label = r$survey[[1]],
      component_label = r$component_or_release[[1]]
    )
  }))

  out %>%
    mutate(
      component = coalesce_chr(.data$component, "Other Data"),
      cycle = coalesce_chr(.data$cycle, "Unspecified Continuous NHANES"),
      top_folder = top_folder_name("continuous"),
      survey = .data$cycle,
      component_or_release = .data$component,
      file_name = paste0(safe_name(.data$table_name), ".XPT"),
      local_folder = file.path(RAW_ROOT, .data$top_folder, safe_name(.data$survey)),
      suggested_cleaning = suggested_cleaning(.data$link_type)
    ) %>%
    select(
      "local_folder",
      "file_name",
      "source_url",
      "top_folder",
      "survey",
      "component_or_release",
      "link_type",
      "suggested_cleaning",
      "source_page",
      "description"
    ) %>%
    filter(!is.na(.data$source_url), nzchar(.data$source_url)) %>%
    distinct(.data$source_url, .keep_all = TRUE)
}

# ------------------------------------------------------------------------------
# LEGACY / ANCILLARY PLAN
# ------------------------------------------------------------------------------

get_noncontinuous_plan <- function(reg_row) {
  sid <- reg_row$survey_id[[1]]
  survey_label <- reg_row$survey_label[[1]]
  group_folder <- top_folder_name(reg_row$group[[1]])

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
      source_page = page_url,
      link_text = trimws(rvest::html_text2(anchors)),
      href = rvest::html_attr(anchors, "href")
    ) %>%
      mutate(
        source_url = vapply(.data$href, absolute_url, FUN.VALUE = character(1), base_url = page_url),
        link_type = classify_link_type(.data$source_url, .data$link_text)
      ) %>%
      filter(!is_blank(.data$source_url)) %>%
      filter(!has_url_fragment(.data$source_url)) %>%
      filter(in_survey_scope(.data$source_url, sid)) %>%
      filter(.data$link_type != "html_page") %>%
      distinct(.data$source_url, .keep_all = TRUE)

    if (nrow(page_links) == 0) next
    rows[[length(rows) + 1]] <- page_links

    if (depth < CRAWL_MAX_DEPTH) {
      next_pages <- tibble::tibble(
        href = rvest::html_attr(anchors, "href")
      ) %>%
        mutate(url = vapply(.data$href, absolute_url, FUN.VALUE = character(1), base_url = page_url)) %>%
        filter(!is_blank(.data$url)) %>%
        filter(!has_url_fragment(.data$url)) %>%
        filter(in_survey_scope(.data$url, sid)) %>%
        filter(grepl("\\.aspx($|\\?)|\\.htm(l)?($|\\?)", .data$url, ignore.case = TRUE)) %>%
        pull(.data$url) %>%
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
      relative_path = vapply(
        seq_along(.data$source_url),
        function(i) survey_relative_path(.data$source_url[[i]], sid, .data$source_page[[i]]),
        character(1)
      ),
      local_folder = file.path(RAW_ROOT, group_folder, safe_name(survey_label)),
      file_name = basename(.data$relative_path),
      top_folder = group_folder,
      survey = survey_label,
      component_or_release = dirname(.data$relative_path),
      component_or_release = ifelse(.data$component_or_release %in% c(".", ""), "", .data$component_or_release),
      suggested_cleaning = suggested_cleaning(.data$link_type)
    ) %>%
    select(
      "local_folder",
      "file_name",
      "source_url",
      "top_folder",
      "survey",
      "component_or_release",
      "link_type",
      "suggested_cleaning",
      "source_page",
      "link_text"
    ) %>%
    filter(!is.na(.data$file_name), nzchar(.data$file_name)) %>%
    distinct(.data$source_url, .keep_all = TRUE)
}

# ------------------------------------------------------------------------------
# BUILD
# ------------------------------------------------------------------------------

message("Building NHANES download plan...")

continuous_plan <- get_continuous_plan()
registry <- nhanes_registry()
legacy_plan <- bind_rows(lapply(seq_len(nrow(registry)), function(i) get_noncontinuous_plan(registry[i, , drop = FALSE])))

plan <- bind_rows(continuous_plan, legacy_plan) %>%
  mutate(across(where(is.character), clean_cell_text)) %>%
  arrange(.data$top_folder, .data$survey, .data$component_or_release, .data$file_name)

readr::write_csv(plan, PLAN_PATH)

message("Wrote: ", PLAN_PATH)
message("Done.")
