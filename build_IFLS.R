# R/build_IFLS.R
#
# Download IFLS ZIP archives (direct URLs; no API),
# store ZIPs in:
#   data/raw/IFLS/
# and extract each ZIP into:
#   data/cleaned/IFLS/<zipname>/
#
# Metadata:
#   data/raw/IFLS/metadata_raw.csv
#   data/cleaned/IFLS/metadata.csv
#

# -------------------------
# CONFIG
# -------------------------

FORCE_REDOWNLOAD <- FALSE
FORCE_REEXTRACT  <- TRUE

RAW_ROOT   <- file.path("data", "raw", "IFLS")
CLEAN_ROOT <- file.path("data", "cleaned", "IFLS")

URLS <- c(
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS1-RR/data/stata/hh93dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS2/data/stata/hh97dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS2/data/stata/cf97dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS3/data/stata/hh00_all_dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS3/data/stata/cf00_all_dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS4/data/stata/hh07_all_dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS4/data/stata/cf07_all_dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS4/data/stata/crp_dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS5/hh14_all_dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS5/cf14_all_dta.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS1-RR/data/commid93.zip",
  "https://sites.rand.org/labor/family/software_and_data/FLS/IFLS/IFLS1-RR/data/facxwalk.zip"
)

# -------------------------
# HELPERS
# -------------------------

dir_ok <- function(x) {
  dir.create(x, recursive = TRUE, showWarnings = FALSE)
}

safe_name <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("[/\\\\:*?\"<>|]+", "_", x)
  x
}

zip_basename_from_url <- function(u) {
  basename(u)
}

snippet_for_zip <- function(zip_base) {
  safe_name(tools::file_path_sans_ext(basename(zip_base)))
}

download_zip <- function(url, dest_zip, force = FALSE) {
  if (!force && file.exists(dest_zip)) {
    message("ZIP exists, skipping download: ", dest_zip)
    return(invisible(dest_zip))
  }
  
  message("Downloading: ", url)
  tmp <- paste0(dest_zip, ".tmp")
  if (file.exists(tmp)) unlink(tmp, force = TRUE)
  
  utils::download.file(url, destfile = tmp, mode = "wb", quiet = TRUE)
  
  ok <- file.rename(tmp, dest_zip)
  if (!ok) {
    file.copy(tmp, dest_zip, overwrite = TRUE)
    unlink(tmp, force = TRUE)
  }
  
  dest_zip
}

extract_whole_zip <- function(zip_path, out_dir, overwrite = TRUE) {
  dir_ok(out_dir)
  
  if (overwrite && dir.exists(out_dir)) {
    unlink(
      list.files(out_dir, full.names = TRUE, all.files = TRUE, no.. = TRUE),
      recursive = TRUE,
      force = TRUE
    )
  }
  
  utils::unzip(zipfile = zip_path, exdir = out_dir, overwrite = TRUE)
  invisible(out_dir)
}

# -------------------------
# SETUP
# -------------------------

dir_ok(RAW_ROOT)
dir_ok(CLEAN_ROOT)

raw_meta_path   <- file.path(RAW_ROOT,   "metadata_raw.csv")
clean_meta_path <- file.path(CLEAN_ROOT, "metadata.csv")

clean_meta_old <- if (file.exists(clean_meta_path)) {
  read.csv(clean_meta_path, stringsAsFactors = FALSE)
} else {
  NULL
}

raw_rows   <- list()
clean_rows <- list()

# -------------------------
# MAIN PIPELINE
# -------------------------

for (u in URLS) {
  
  zip_base <- zip_basename_from_url(u)
  dest_zip <- file.path(RAW_ROOT, zip_base)
  
  # ---- Download
  download_zip(u, dest_zip, force = FORCE_REDOWNLOAD)
  
  # ---- Inspect ZIP contents
  members_df <- utils::unzip(dest_zip, list = TRUE)
  members <- members_df$Name
  members <- members[!grepl("/$", members) & nzchar(members)]
  
  if (length(members) == 0) {
    warning("No files found in ZIP: ", zip_base)
    next
  }
  
  # ---- Raw metadata (one row per file in ZIP)
  for (m in members) {
    raw_rows[[length(raw_rows) + 1]] <- data.frame(
      url         = u,
      zip_file    = zip_base,
      zip_path    = dest_zip,
      member      = m,
      member_base = basename(m),
      stringsAsFactors = FALSE
    )
  }
  
  # ---- Extract whole ZIP
  snippet <- snippet_for_zip(zip_base)
  out_dir <- file.path(CLEAN_ROOT, snippet)
  
  message("Extracting ZIP: ", zip_base, " -> ", out_dir)
  extract_whole_zip(dest_zip, out_dir, overwrite = FORCE_REEXTRACT)
  
  # ---- Cleaned metadata (still one row per file)
  for (m in members) {
    clean_rows[[length(clean_rows) + 1]] <- data.frame(
      url            = u,
      zip_file       = zip_base,
      member         = m,
      snippet        = snippet,
      cleaned_folder = out_dir,
      cleaned_file   = file.path(out_dir, m),
      extracted_at   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      stringsAsFactors = FALSE
    )
  }
}

# -------------------------
# WRITE METADATA
# -------------------------

if (length(raw_rows) > 0) {
  raw_meta <- do.call(rbind, raw_rows)
  write.csv(raw_meta, raw_meta_path, row.names = FALSE)
  message("Wrote: ", raw_meta_path)
}

if (length(clean_rows) > 0) {
  new_meta <- do.call(rbind, clean_rows)
  
  combined <- if (!is.null(clean_meta_old)) {
    rbind(clean_meta_old, new_meta)
  } else {
    new_meta
  }
  
  key <- paste(combined$zip_file, combined$member, sep = "||")
  combined <- combined[!duplicated(key), ]
  
  write.csv(combined, clean_meta_path, row.names = FALSE)
  message("Updated: ", clean_meta_path)
}

# -------------------------
# DOCUMENTATION
# -------------------------

sources_md <- file.path(RAW_ROOT, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "IFLS data downloaded from RAND (direct ZIP links; no API).",
      "DATA URL: https://www.rand.org/health/surveys/FLS/IFLS/download.html",
      "",
      "ZIP files used:",
      paste0("- ", URLS),
      "",
      paste0("Generated on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
}

notes_md <- file.path(CLEAN_ROOT, "DATASET_NOTES.md")
writeLines(
  c(
    "# IFLS dataset notes",
    "",
    sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "- Files are downloaded as ZIPs and extracted without modification.",
    "- One folder per ZIP under `data/cleaned/IFLS/`.",
    "- Internal ZIP directory structure is preserved.",
    "- See `metadata.csv` for a file-level inventory.",
    "- Documentation: https://www.rand.org/health/surveys/FLS/IFLS/download.html"
  ),
  notes_md
)

message("IFLS build complete.")
