library(arrow)
library(dplyr)

# Load existing parquet
df <- read_parquet("C:/Users/Karls/OneDrive/Everything/data/data-warehouse/data/cleaned/HMD/life tables/Canada/data.parquet")

# Add column
df <- df %>%
  mutate(delete = "del")

# Overwrite the original parquet file
write_parquet(df, "C:/Users/Karls/OneDrive/Everything/data/data-warehouse/data/cleaned/HMD/life tables/Canada/data.parquet")