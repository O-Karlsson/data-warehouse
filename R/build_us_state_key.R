# R/build_us_state_key.R
#
# PURPOSE
# -------
# Create a simple lookup dataset with:
#   - state_code (2-letter USPS codes, lowercase)
#   - state_name (full state name)
#
# Output:
#   data/cleaned/metadata/us_states/
#     - data.csv
#     - data.parquet
#     - data.dta
#
# No raw input required (dataset generated from built-in R vectors).
#
# Created: `r Sys.Date()`

# ------------------------------------------------------------------------------
# Libraries
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(arrow)
  library(haven)
})

# ------------------------------------------------------------------------------
# Directories
# ------------------------------------------------------------------------------

dir_clean <- file.path("data", "cleaned", "metadata", "us_states")
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# Build dataset
# ------------------------------------------------------------------------------

state_key <- tibble(
  state_code = state.abb,   # built-in R vector
  state_name = state.name            # built-in R vector
) %>%
  add_row(state_code = "DC", state_name = "District of Columbia") %>%
  add_row(state_code = "USA", state_name = "United States")

# ------------------------------------------------------------------------------
# Write outputs
# ------------------------------------------------------------------------------

readr::write_csv(state_key, file.path(dir_clean, "data.csv"))
arrow::write_parquet(state_key, file.path(dir_clean, "data.parquet"))
haven::write_dta(state_key, file.path(dir_clean, "data.dta"))

message("US state key written to: ", dir_clean)

# ------------------------------------------------------------------------------
# Documentation file
# ------------------------------------------------------------------------------

notes_file <- file.path(dir_clean, "README.md")

if (!file.exists(notes_file)) {
  writeLines(
    c(
      "# US State Lookup Key",
      "",
      "This dataset contains two columns:",
      "- state_code: 2-letter USPS state codes (lowercase)",
      "- state_name: Full state name",
      "",
      "Source: Built-in R vectors `state.abb` and `state.name`.",
      paste0("Created on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    notes_file
  )
}

message("Done.")
