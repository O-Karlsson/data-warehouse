# R/utils.R
#
# OVERVIEW
# --------
# This file contains reusable utility functions that are shared
# across build scripts in the data warehouse.
#
# The goal of this file is to:
#   - Avoid duplicating boilerplate code
#   - Make build scripts shorter and easier to read
#   - Centralize behavior that should be consistent across datasets
#
# Nothing in this file is dataset-specific.
# If you find yourself copying a few lines between build scripts,
# that logic probably belongs here.
#
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# These functions are for versioning data files 
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
#
# Create a fingerprint of a file so we can tell whether two
# files are identical *byte-for-byte*.
file_hash <- function(path) {
  if (!file.exists(path)) {
    return(NA_character_)
  }
   unname(tools::md5sum(path)) # unname strips away the names attribute:
   # eg, "data/cleaned/data.parquet" = "5c1b7e4a9a2d3f1c8e9c6a0c9b7e1234"
   # removes "data/cleaned/data.parquet" from the output
   }

# ------------------------------------------------------------
# archive_and_promote()
#
# Purpose:
#   Safely update a "current" dataset while preserving the
#   previous version *only if the data actually changed*.
#
# Workflow:
#   1. Compare hashes of old vs new file
#   2. If identical:
#        - discard temporary file
#        - keep current file untouched
#   3. If different:
#        - copy old file into archive
#        - replace current file with new file
#
# This avoids:
#   - needless archive clutter
#   - downstream path changes
#   - accidental overwrites without provenance
#
# Arguments:
#   final_path : stable path used by downstream analysis
#   tmp_path   : newly built file written to a temp location
#   archive_dir: where old versions should be stored
#   tag        : optional label for archive filename
# ------------------------------------------------------------
archive_and_promote <- function(final_path,
                                tmp_path,
                                archive_dir,
                                tag = NULL) {
  
  # Ensure archive directory exists
  # recursive = TRUE allows nested directories
  dir.create(archive_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Compute hashes for old and new files
  old_hash <- file_hash(final_path)
  new_hash <- file_hash(tmp_path)
  
  # ----------------------------------------------------------
  # Case 1: No existing "current" file
  # ----------------------------------------------------------
  # This happens on first build.
  # There is nothing to archive — just promote the temp file.
  if (!file.exists(final_path)) {
    file.rename(tmp_path, final_path)
    return(invisible(list(
      changed  = TRUE,
      archived = FALSE
    )))
  }
  
  # ----------------------------------------------------------
  # Case 2: New output is identical to existing output
  # ----------------------------------------------------------
  # Data did not change; rebuilding was a no-op.
  # We discard the temp file and keep the current one.
  if (!is.na(old_hash) &&
      !is.na(new_hash) &&
      old_hash == new_hash) {
    
    unlink(tmp_path)
    
    return(invisible(list(
      changed  = FALSE,
      archived = FALSE
    )))
  }
  
  # ----------------------------------------------------------
  # Case 3: Output changed → archive old version
  # ----------------------------------------------------------
  
  # Timestamp used to identify the archived snapshot
  # (human-readable, sortable)
  stamp <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  
  # Allow optional custom tag
  if (is.null(tag)) {
    tag <- stamp
  }
  
  # Construct archive filename:
  base <- tools::file_path_sans_ext(basename(final_path))
  ext  <- tools::file_ext(final_path)
  
  archive_name <- paste0(
    base, "_", tag,
    if (nzchar(ext)) paste0(".", ext) else ""
  )
  
  archive_path <- file.path(archive_dir, archive_name)
  
  # Copy old file into archive (never overwrite)
  file.copy(final_path, archive_path, overwrite = FALSE)
  
  # Promote new file to stable path
  file.rename(tmp_path, final_path)
  
  invisible(list(
    changed       = TRUE,
    archived      = TRUE,
    archive_path = archive_path
  ))
}

# ------------------------------------------------------------------------------
# End of versioning functions
# ------------------------------------------------------------------------------



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# These functions are used to generate warnings about possible missing entries
# into data dictionaries (ie, dataframe includes more variables than dictionary)
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# doc_warnings_for_var_dict()
# Purpose:
#   Compare dataset variables (names(df)) to a variable dictionary data.frame
#   and return human-readable warning messages (as a character vector).
#
# Expected columns in var_dict:
#   - var
#
# Returns:
#   character vector; length 0 means "no issues"
# ------------------------------------------------------------
doc_warnings_for_var_dict <- function(df, var_dict, dict_name = "dictionary",
                                      df_name   = "data frame") {
  
  warnings <- character(0)
  
  # Basic sanity checks
  if (!is.data.frame(var_dict)) {
    return(paste0("Variable dictionary is not a data.frame (", dict_name, ")."))
  }
  if (!("var" %in% names(var_dict))) {
    return(paste0("Variable dictionary missing required column 'var' (", dict_name, ")."))
  }
  
  data_vars <- names(df)
  doc_vars  <- var_dict$var
  
  # In data but not documented
  missing_vars <- setdiff(data_vars, doc_vars)
  if (length(missing_vars) > 0) {
    warnings <- c(
      warnings,
      paste0(
        "Variables present in ", df_name," but missing from ", dict_name, ": ",
        paste(missing_vars, collapse = ", ")
      )
    )
  }
  
  # Documented but not in data
  extra_vars <- setdiff(doc_vars, data_vars)
  if (length(extra_vars) > 0) {
    warnings <- c(
      warnings,
      paste0(
        "Variables documented in ", dict_name, " but not found in ", df_name, ": ",
        paste(extra_vars, collapse = ", ")
      )
    )
  }
  
  # Empty labels (if column exists)
  if ("var_label" %in% names(var_dict)) {
    empty_labels <- var_dict$var[!nzchar(trimws(var_dict$var_label))]
    if (length(empty_labels) > 0) {
      warnings <- c(
        warnings,
        paste0(
          "Empty var_label for variables in ", dict_name, ": ",
          paste(empty_labels, collapse = ", ")
        )
      )
    }
  }
  
  warnings
}

# ------------------------------------------------------------------------------
# End of functions for data dictionary warnings 
# ------------------------------------------------------------------------------

