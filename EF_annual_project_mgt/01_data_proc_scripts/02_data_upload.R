
###################################################################
#                      EF_V7  Database loading                    #
###################################################################

library(tidyverse)
library(lubridate)
library(DBI)


# Create db connection
con <-  dbConnect(RSQLite::SQLite(), "./EXAMPLE.sqlite")  ## Change database name!!!
#dbDisconnect(con)

# View db tables (only works on esisting db)
# dbListTables(con)
# dbListFields(con, "site")

# Import data
site_tmp <- read_csv("./EF_annual_project_mgt/data/proofed_csv/proof_site_Dino1.csv") %>%
  mutate_if(is.POSIXct, as.character) %>%
  mutate(river = "GR") %>%
  rename(project_code = project)

fish_tmp <- read_csv("./EF_annual_project_mgt/data/proofed_csv/proof_fish_Dino1.csv")%>%
  mutate_if(is.POSIXct, as.character)

pit_tmp <- read_csv("./EF_annual_project_mgt/data/proofed_csv/proof_pittag_Dino1.csv") %>%
  mutate_at("pit_type", as.character)

floy_tmp <- read_csv("./EF_annual_project_mgt/output/qaqc_check/123a_ECHO_pass1-2/raw_floy.csv")

water_tmp <- read_csv("./EF_annual_project_mgt/output/qaqc_check/123a_ECHO_pass1-2/raw_water.csv")

meta <- read_csv("./EF_annual_project_mgt/output/qaqc_check/123a_ECHO_pass1-2/raw_meta.csv")
#--------------------------------
# Remove index columns
#--------------------------------

site <- select(site_tmp, -c(matches("_flg$|_index$|^key_")))
fish <- select(fish_tmp, -c(matches("_flg$|_index$|^key_"))) %>%
  filter(!is.na(fish_id))
pit <- select(pit_tmp, -c(species, matches("_flg$|_index$|^key_|^site")))

floy <- select(floy_tmp, -c(species, matches("_flg$|_index$|^key_|^site")))

water <- water_tmp %>%
  select(-key_a)



# Upload tables
dbWriteTable(con, name = "meta", value = meta, append = TRUE)

dbWriteTable(con, name = "site", value = site, append = TRUE)

dbWriteTable(con, name = "water_qual", value = water, append = TRUE)

dbWriteTable(con, name = "fish", value = fish, append = TRUE)

dbWriteTable(con, name = "pittag", value = pit, append = TRUE)

dbWriteTable(con, name = "floytag", value = floy, append = TRUE)


# Disconnect

dbDisconnect(con)

## End
