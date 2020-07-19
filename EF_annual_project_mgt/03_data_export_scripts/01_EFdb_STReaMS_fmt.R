
#######################################################################
#                        STReaMS Upload Format                        #
#######################################################################

# Attach packages
library(tidyverse)
library(DBI)
library(UCRBtools)

## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## Issues:
##   1. sub tag size endangereds require "N" in recap field
##   2. need floy tag data to test floytag related modifications
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#------------------------------
# Connect to local SQLite
#------------------------------
con <-  dbConnect(RSQLite::SQLite(), "./EXAMPLE.sqlite")  ## Change database name!!!

#dbDisconnect(con)

dbListTables(con)


# Set year of interest
yoi = 2020

#--------------------------------------
# Import current year's STReaMS views
#--------------------------------------
site_tmp <- tbl(con, "STReaMS_site") %>%
  filter(year == yoi) %>%
  collect()

rare_tmp <- tbl(con, "STReaMS_rare") %>%
  filter(year == yoi) %>%
  collect()

ntf_tmp <- tbl(con, "STReaMS_ntf") %>%
  filter(year == yoi) %>%
  collect()

# Disconnect
dbDisconnect(con)


#-------------------------------------
# Modify data types and vars
#-------------------------------------

# Site table
site <- site_tmp %>%
  mutate_at(c("STARTDATETIME", "ENDDATETIME"), as.POSIXct, tz = "UTC") %>%
  mutate_at("PASS", as.character) %>%
  select(-year)

# Rare table
rare <- rare_tmp %>%
  mutate(`PIT TAG 134` = ifelse(pit_type == 134 & !is.na(pit_num), pit_num, NA),
         `PIT TAG 400` = ifelse(pit_type == 400 & !is.na(pit_num), pit_num, NA),
         RIPE = ifelse(grepl("(EXP)", RIPE), "Y", "N"),
         `NEW TAG` = ifelse(RECAPTURE == "N" & !is.na(pit_num), "Y", "N"),
         `DATE TIME` = as.POSIXct(`DATE TIME`, tz = "UTC"),
         `UTM ZONE` = ifelse(epsg %in% c(32612, 26912), 12, NA)) %>%
  select(-c(pit_type, pit_num,))

# NTF table
# Needs work reshaping the floytag data
ntf <- ntf_tmp %>%
  mutate(RIPE = ifelse(grepl("(EXP)", RIPE), "Y", "N"),
         `DATE TIME` = as.POSIXct(`DATE TIME`, tz = "UTC"),
         `UTM ZONE` = ifelse(epsg %in% c(32612, 26912), 12, NA)) %>%
  keep(~!all(is.na(.)))


#ntf$`DATE TIME`
#------------------------------
# Source required functions
#------------------------------
#source("./src/fun/get_template.R")
#source("./src/fun/STReaMS_xlsx_wkbk.R")

#----------------------------------
# Import current STReaMS template
#----------------------------------

get_template() %>%
  list2env(.GlobalEnv)

#----------------------------------
# Bind data to template
#----------------------------------
site_fnl <- site_tmplt %>%
  bind_rows(site)

rare_fnl <- rare_tmplt %>%
  bind_rows(rare)

ntf_fnl <- ntf_tmplt %>%
  bind_rows(ntf)


# combine data tables into list
data <- list(site_fnl, rare_fnl, ntf_fnl)
names(data) <- c("site", "rare", "ntf")

dir.create("./EF_annual_project_mgt/output/db_export_xlsx", showWarnings = FALSE)

# Save formatted data to workbook for submission to STReaMS

STReaMS_xlsx_wkbk(
  data = data,
  year = yoi,
  Project = "EXAMPLE",
  output_path = "./EF_annual_project_mgt/output/db_export_xlsx"
)


## End
