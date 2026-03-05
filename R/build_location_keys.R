# R/build_location_keys.R
#
# Build UNWPP isoRegions ↔ IHME location hierarchy key

source("R/utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(stringi)
  library(arrow)
  library(haven)
})

# -----------------------------
# Helpers
# -----------------------------

neat_location <- function(x) {
  x %>%
    as.character() %>%
    stri_trans_general("Latin-ASCII") %>%
    str_replace_all("\\([^\\)]*\\)", " ") %>%  # drop parentheticals
    str_to_lower() %>%
    str_replace_all("[^a-z0-9]+", " ") %>%
    str_squish()
}

write_outputs <- function(df, out_dir, stem = "data") {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  out_csv     <- file.path(out_dir, paste0(stem, ".csv"))
  out_parquet <- file.path(out_dir, paste0(stem, ".parquet"))
  out_dta     <- file.path(out_dir, paste0(stem, ".dta"))
  
  readr::write_csv(df, out_csv, na = "")
  arrow::write_parquet(df, out_parquet)
  haven::write_dta(df, out_dta)
  
  message("Wrote: ", out_csv)
  message("Wrote: ", out_parquet)
  message("Wrote: ", out_dta)
}

# -----------------------------
# Paths
# -----------------------------

in_un   <- file.path("data", "cleaned", "unwpp", "metadata", "isoRegions", "data.csv")
in_ihme <- file.path("data", "cleaned", "ihme_keys", "ihme_location_hierarchy.csv")
in_dhs  <- file.path("data", "cleaned", "DHS", "metadata.csv")
in_cih  <- file.path("data", "manual", "cih_regions.csv")

out_dir <- file.path("data", "cleaned", "keys", "location_keys")
qa_dir  <- file.path(out_dir, "_qa")

manual_overrides_path <- file.path("data", "manual", "keys", "location_key_overrides.csv")

stopifnot(file.exists(in_un))
stopifnot(file.exists(in_ihme))
stopifnot(file.exists(in_dhs))
stopifnot(file.exists(in_cih))

# -----------------------------
# Read UNWPP isoRegions
# -----------------------------

un_raw <- readr::read_csv(
  in_un,
  show_col_types = FALSE,
  locale = readr::locale(encoding = "UTF-8")
)
# Defensive checks
stopifnot(all(c("location", "iso3", "region", "subregion", "incomegr") %in% names(un_raw)))

un <- un_raw %>%
  transmute(
    unwpp_location = location,
    
    # what you want to DISPLAY
    location_label = dplyr::case_when(
      location == "Bolivia (Plurinational State of)"      ~ "Bolivia",
      location == "Democratic Republic of the Congo"      ~ "Congo DR",
      location == "Falkland Islands (Malvinas)"           ~ "Falkland Islands",
      location == "China, Hong Kong SAR"                  ~ "Hong Kong",
      location == "Iran (Islamic Republic of)"            ~ "Iran",
      location == "Kosovo (under UNSC res. 1244)"         ~ "Kosovo",
      location == "Lao People's Democratic Republic"      ~ "Lao",
      location == "China, Macao SAR"                      ~ "Macao",
      location == "Micronesia (Fed. States of)"           ~ "Micronesia",
      location == "Republic of Moldova"                   ~ "Moldova",
      location == "Dem. People's Republic of Korea"       ~ "North Korea",
      location == "State of Palestine"                    ~ "Palestine",
      location == "Russian Federation"                    ~ "Russia",
      location == "Saint Martin (French part)"            ~ "Saint Martin (French)",
      location == "Sint Maarten (Dutch part)"             ~ "Sint Maarten (Dutch)",
      location == "Republic of Korea"                     ~ "South Korea",
      location == "Syrian Arab Republic"                  ~ "Syria",
      location == "China, Taiwan Province of China"       ~ "Taiwan",
      location == "United Republic of Tanzania"           ~ "Tanzania",
      location == "United States of America"              ~ "United States",
      location == "United States Virgin Islands"          ~ "US Virgin Islands",
      location == "Venezuela (Bolivarian Republic of)"    ~ "Venezuela",
      TRUE ~ location
    ),
    
    # what you want to MATCH on (only if you need different logic)
    match_location = dplyr::case_when(
      location == "State of Palestine"               ~ "Palestine",
      location == "Dem. People's Republic of Korea"  ~ "Democratic People's Republic of Korea",
      location == "China, Taiwan Province of China"  ~ "Taiwan",
      TRUE ~ location
    ),
    
    iso3, region, subregion, incomegr
  ) %>%
  mutate(
    neat_location = neat_location(match_location)
  )

# -----------------------------
# Read IHME locations (country level only: level == 3)
# -----------------------------

ihme_raw <- readr::read_csv(in_ihme, show_col_types = FALSE)

stopifnot(all(c("location_name", "location_id", "level") %in% names(ihme_raw)))

ihme <- ihme_raw %>%
  filter(level == 3) %>%                     # keep countries only
  transmute(
    location_id = as.integer(location_id),
    ihme_location = location_name
  ) %>%
  distinct(location_id, .keep_all = TRUE) %>% # safeguard
  mutate(
    neat_location = neat_location(ihme_location)
  )

# -----------------------------
# Read DHS country codes (2-letter)
# -----------------------------

dhs_raw <- readr::read_csv(
  in_dhs,
  show_col_types = FALSE,
  locale = readr::locale(encoding = "UTF-8")
)

stopifnot(all(c("CountryName", "DHS_CountryCode") %in% names(dhs_raw)))

dhs <- dhs_raw %>%
  transmute(
    CountryName = as.character(CountryName),
    dhs_countrycode = as.character(DHS_CountryCode)
  ) %>%
  # Multiple rows per country in DHS metadata; keep unique country-level mapping
  distinct(CountryName, .keep_all = TRUE) %>%
  mutate(
    CountryName = case_when(
      CountryName == "Bolivia"                    ~ "Bolivia (Plurinational State of)",
      CountryName == "Congo Democratic Republic"  ~ "Democratic Republic of the Congo",
      CountryName == "Cote d'Ivoire"              ~ "Côte d'Ivoire",
      CountryName == "Kyrgyz Republic"            ~ "Kyrgyzstan",
      CountryName == "Moldova"                    ~ "Republic of Moldova",
      CountryName == "Tanzania"                   ~ "United Republic of Tanzania",
      CountryName == "Turkey"                     ~ "Türkiye",
      CountryName == "Vietnam"                    ~ "Viet Nam",
      TRUE ~ CountryName
    )
  ) %>%
  # This is subnational in DHS and won't match UNWPP country list
  filter(CountryName != "Nigeria (Ondo State)")

# -----------------------------
# Read CIH regions
# -----------------------------

cih_raw <- readr::read_csv(
  in_cih,
  show_col_types = FALSE,
  locale = readr::locale(encoding = "UTF-8")
)

stopifnot(all(c("iso3", "cih_region") %in% names(cih_raw)))

cih <- cih_raw %>%
  transmute(
    iso3 = as.character(iso3),
    cih_region = as.character(cih_region)
  ) %>%
  distinct(iso3, .keep_all = TRUE)

# -----------------------------
# Optional manual overrides
# -----------------------------

overrides <- NULL
if (file.exists(manual_overrides_path)) {
  overrides <- readr::read_csv(manual_overrides_path, show_col_types = FALSE) %>%
    transmute(
      unwpp_location = as.character(unwpp_location),
      location_id = as.integer(location_id)
    )
  message("Using manual overrides: ", manual_overrides_path)
}

# -----------------------------
# Match on neat_location
# -----------------------------

key_exact <- un %>%
  left_join(
    ihme %>% select(location_id, ihme_location, neat_location),
    by = "neat_location"
  ) %>%
  mutate(match_method = if_else(!is.na(location_id), "exact_neat_location", NA_character_))

# Overrides win
if (!is.null(overrides)) {
  key_exact <- key_exact %>%
    left_join(overrides, by = "unwpp_location", suffix = c("", "_override")) %>%
    mutate(
      location_id = coalesce(location_id_override, location_id),
      match_method = if_else(!is.na(location_id_override), "manual_override", match_method)
    ) %>%
    select(-location_id_override)
}

# Ensure IHME name filled if match came from override
key <- key_exact %>%
  left_join(ihme %>% select(location_id, ihme_location),
            by = "location_id",
            suffix = c("", "_from_id")) %>%
  mutate(ihme_location = coalesce(ihme_location, ihme_location_from_id)) %>%
  # Attach DHS 2-letter country code
  left_join(dhs, by = c("unwpp_location" = "CountryName")) %>%
  # Attach CIH region
  left_join(cih, by = "iso3") %>%
  # NCD-RisC country naming convention
  mutate(
    NCD_RisC_country = case_when(
      location_label == "Hong Kong" ~ "China (Hong Kong SAR)",
      location_label == "C\u00f4te d'Ivoire" ~ "Cote d'Ivoire",
      location_label == "Czechia" ~ "Czech Republic",
      location_label == "Congo DR" ~ "DR Congo",
      location_label == "Guinea-Bissau" ~ "Guinea Bissau",
      location_label == "Lao" ~ "Lao PDR",
      location_label == "North Macedonia" ~ "Macedonia (TFYR)",
      location_label == "Micronesia" ~ "Micronesia (Federated States of)",
      location_label == "Palestine" ~ "Occupied Palestinian Territory",
      location_label == "Russia" ~ "Russian Federation",
      location_label == "Eswatini" ~ "Swaziland",
      location_label == "Syria" ~ "Syrian Arab Republic",
      location_label == "T\u00fcrkiye" ~ "Turkey",
      location_label == "United States" ~ "United States of America",
      TRUE ~ location_label
    )
  ) %>%
  select(-ihme_location_from_id, -neat_location) %>%   # <-- drop here
  relocate(
    unwpp_location, location_label, ihme_location, location_id,
    iso3, dhs_countrycode, cih_region, region, subregion, incomegr, NCD_RisC_country, match_method
  )




# -----------------------------
# QA outputs
# -----------------------------


dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

un_unmatched <- key_exact %>%
  filter(is.na(location_id)) %>%
  select(unwpp_location, location_label, neat_location, iso3, region, subregion, incomegr)


ihme_unmatched <- ihme %>%
  anti_join(key %>% filter(!is.na(location_id)) %>% distinct(location_id), by = "location_id") %>%
  select(location_id, ihme_location, neat_location)

dhs_unmatched <- dhs %>%
  anti_join(key %>% distinct(unwpp_location), by = c("CountryName" = "unwpp_location")) %>%
  arrange(CountryName)

dupe_location_id <- key_exact %>%
  filter(!is.na(location_id)) %>%
  count(location_id, sort = TRUE) %>%
  filter(n > 1)

readr::write_csv(un_unmatched,   file.path(qa_dir, "unwpp_unmatched.csv"), na = "")
readr::write_csv(ihme_unmatched, file.path(qa_dir, "ihme_unmatched.csv"),  na = "")
readr::write_csv(dupe_location_id, file.path(qa_dir, "duplicate_location_id_matches.csv"), na = "")
readr::write_csv(dhs_unmatched,  file.path(qa_dir, "dhs_unmatched.csv"),   na = "")

message("QA wrote: ", qa_dir)

# -----------------------------
# Write outputs
# -----------------------------

write_outputs(key, out_dir, stem = "data")

message("UN rows: ", nrow(un))
message("IHME unique location_id rows: ", nrow(ihme))
message("Matched UN rows: ", sum(!is.na(key$location_id)))
message("Unmatched UN rows: ", sum(is.na(key$location_id)))
