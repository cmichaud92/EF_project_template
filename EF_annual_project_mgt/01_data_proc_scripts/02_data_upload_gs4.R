
###################################################################
#                      EF_V7  Database loading                    #
###################################################################

library(tidyverse)
library(lubridate)
library(DBI)
library(googledrive)
library(googlesheets4)


# Create db connection
con <-  dbConnect(RSQLite::SQLite(), "./TEST_EL_database.sqlite")  ## Change database name!!!
#dbDisconnect(con)


# Authenticate to google drive
drive_auth(email = "cmichaud@utah.gov")

gs4_auth(token = drive_token())

# Download proofed dataset
sets <- drive_get("Test_Proof_Dino2_Deso")


# View db tables (only works on esisting db)
# dbListTables(con)
# dbListFields(con, "site")

# Import data
site_tmp <- read_sheet(sets[1, ], range = "ck_site") %>%
  mutate_if(is.POSIXct, as.character) %>%
  rename(project_code = project)

fish_tmp <- read_sheet(sets[1, ], range = "ck_fish")%>%
  mutate_if(is.POSIXct, as.character)

pit_tmp <- read_sheet(sets[1, ], range = "ck_pit") %>%
  mutate_at("pit_type", as.character)

# floy_tmp <- read_sheet(sets[1, ], range = "ck_floy")

water_tmp <- read_sheet(sets[1, ], range = "water")

meta <- read_sheet(sets[1, ], range = "meta")
#--------------------------------
# Remove index columns
#--------------------------------

site <- select(site_tmp, -c(matches("_flg$|_index$|^key_")))
fish <- select(fish_tmp, -c(matches("_flg$|_index$|^key_"), reach)) %>%
  filter(!is.na(fish_id))
pit <- select(pit_tmp, -c(species, matches("_flg$|_index$|^key_|^site")))

# floy <- select(floy_tmp, -c(species, matches("_flg$|_index$|^key_|^site")))

water <- water_tmp %>%
  select(-key_a)



# Upload tables
dbWriteTable(con, name = "meta", value = meta, append = TRUE)

dbWriteTable(con, name = "site", value = site, append = TRUE)

dbWriteTable(con, name = "water_qual", value = water, append = TRUE)

dbWriteTable(con, name = "fish", value = fish, append = TRUE)

dbWriteTable(con, name = "pittag", value = pit, append = TRUE)

#dbWriteTable(con, name = "floytag", value = floy, append = TRUE)


# Disconnect

dbDisconnect(con)

## End
