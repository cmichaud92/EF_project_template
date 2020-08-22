
###########################################################################
#    Create consistant data products for analysis, archive and sharing    #
###########################################################################

#----------------------------------------------------
# Creating a fully sharable dataproduct
#
# Connect to database
# Filter data
# Extract data
# Modify data for archive/sharing
# Save as .csv
#----------------------------------------------------

#----------------------------------------------------
# Attach packages
#----------------------------------------------------

library(dplyr)
library(dbplyr)
library(tidyr)
library(lubridate)
library(DBI)
library(UCRBtools)
library(waterData)

#----------------------------------------------------
# User defined parameters
#----------------------------------------------------

# Year of interest
yoi <- 2020

#----------------------------------------------------
# Establish database connection
#----------------------------------------------------

con <- dbConnect(RSQLite::SQLite(),
                 "c:/Users/cmichaud/proj_mgt/database/sqlite/123d.sqlite")

#dbDisconnect(con)

#----------------------------------------------------
# Generate generic tables from 123d.sqlite
#----------------------------------------------------

# -----Meta pointer-----

meta_p <- tbl(con, "meta") %>%
  filter(year %in% yoi) %>%
  mutate(principal = paste(principal_lname,
                           principal_fname,
                           sep = ", ")) %>%
  select(year, project_code, principal, agency, gear_type = data_type)


# -----Site pointer and site table-----

site_p <- tbl(con, "site") %>%
  rename(rvr_code = river,
         rch_code = reach) %>%
  left_join(select(tbl_river, rvr_code, rvr_name),
            by = "rvr_code", copy = TRUE) %>%
  left_join(select(tbl_reach, rch_code, rch_name),
            by = "rch_code", copy = TRUE) %>%
  left_join(tbl(con, "water_qual"), by = "site_id") %>%
  inner_join(meta_p, by = c("project_code", "year")) %>%
  select(-c(water_id))


site_tbl <- site_p %>%
  collect() %>%
  mutate_at(vars(contains("date")), as.POSIXct) %>%
  mutate(last_modified = now())%>%
  select(site_id, project_code, year,
         rvr_name, rch_name,
         everything())


# -----Fish table-----

fish_tbl <- tbl(con, "fish") %>%
  semi_join(site_p, by = "site_id") %>%
  left_join(tbl(con, "pittag"), by = "fish_id") %>%
  collect() %>%
  mutate_at(vars(contains("date")), as.POSIXct) %>%
  rename(spp_code = species) %>%
  left_join(select(tbl_spp, -nativity), by = "spp_code") %>%
  mutate(ripe = case_when(grepl("^INT+", rep_cond) ~ "N",
                          grepl("^EXP+", rep_cond) ~ "Y")) %>%
  select(fish_id:rmi, spp_code, com_name, sci_name, tot_length, weight, sex, ripe, tubercles:pit_notes)


# -----Floy table-----

floy_tbl <- tbl(con, "floytag") %>%
  collect()


#grepl("^EXP+", fish_tbl$rep_cond)


#-----------------------------------------------------------
# Scrape water data from usgs
#-----------------------------------------------------------


# -----Green River UT-----

# Green river discharge
gr_dis <- importDVs(staid = "09315000",
                    code = "00060",
                    stat = "00003",
                    sdate = paste0(yoi - 1, "-12-31"),
                    edate = paste0(yoi, "-12-31")) %>%
  rename(discharge = val,
         date = dates) %>%
  select(staid, date, discharge)

# Temperature data
gr_temp <- importDVs(staid = "09315000",
                     code = "00010",
                     stat = "00011",
                     sdate = paste0(yoi - 1, "-12-31"),
                     edate = paste0(yoi, "-12-31")) %>%
  rename(temp = val,
         date = dates) %>%
  select(-qualcode)

gr_waterdata <- full_join(gr_dis, gr_temp, by = c("staid", "date")) %>%
  mutate(sta_name = "Green River, Utah",
         rvr_code = "GR")

#----------------------
# Near Cisco, UT (Dewey)

# Discharge
co_dis <- importDVs(staid =  "09180500",
                    code = "00060",
                    stat = "00003",
                    sdate = paste0(yoi - 1, "-12-31"),
                    edate = paste0(yoi, "-12-31")) %>%
  rename(discharge = val,
         date = dates) %>%
  select(-qualcode)

# Temperature
co_temp <- importDVs(staid = "09180500",
                     code = "00010",
                     stat = "00003",
                     sdate = paste0(yoi - 1, "-12-31"),
                     edate = paste0(yoi, "-12-31")) %>%
  rename(temp = val,
         date = dates) %>%
  select(-qualcode)

co_waterdata <- full_join(co_dis, co_temp, by = c("staid", "date")) %>%
  mutate(sta_name = "Near Cisco, Utah (Dewey)",
         rvr_code = "CO")


#-----------------------------------------------------------
# Final water data set

waterdata <- bind_rows(gr_waterdata, co_waterdata) %>%
  mutate(last_modified = now())


#----------------------------------
# Aggregated data sets
#----------------------------------

# ----- Simple CPUE table -----

cpue <- tbl(con, "site") %>%
  left_join(tbl(con, "fish"), by = "site_id") %>%
  filter(year %in% yoi) %>%

  collect() %>%

  group_by(site_id, el_sec, rvr_code = river,
           rch_code = reach, spp_code = species,
           startdatetime) %>%
  summarise(fish_ct = n(),
            .groups = 'drop') %>%
  complete(nesting(site_id, el_sec, rvr_code,
                   rch_code, startdatetime),
           nesting(spp_code),
           fill = list(fish_ct = 0)) %>%
  filter(!is.na(spp_code)) %>%

  mutate(last_modified = now())


# ----- CPUE by lifestage (WE, SM, NP) -----

nnf <- tbl(con, "fish") %>%
  filter(species %in% c("SM", "WE", "NP"))

cpue_ls <- tbl(con, "site") %>%
  left_join(nnf, by = "site_id") %>%
  filter(year %in% yoi) %>%
  collect() %>%
  life_stage(specvar = .$species, lenvar = .$tot_length) %>%
  mutate(ls = paste(species, ls, sep = "_")) %>%
  na_if("NA_NA") %>%

  group_by(site_id, el_sec, rvr_code = river,
           rch_code = reach, ls,
           startdatetime) %>%
  summarise(fish_ct = n(),
            .groups = 'drop') %>%
  complete(nesting(site_id, el_sec, rvr_code,
                   rch_code, startdatetime),
           nesting(ls),
           fill = list(fish_ct = 0)) %>%
  filter(!is.na(ls)) %>%
  separate(ls, into = c("spp_code", "life_stage"), sep = "_") %>%

  mutate(cpue = round(fish_ct / (el_sec / 3600), 2),
         last_modified = now())



#--------------------------
# Disconnect
#--------------------------

dbDisconnect(con)


#--------------------------
# Write datasets to .csv
#--------------------------


write.csv(site_tbl,
          "./123d_analysis_2020/data/123d_2020_site.csv",
          na = "NA",
          row.names = FALSE)

write.csv(fish_tbl,
          "./123d_analysis_2020/data/123d_2020_fish.csv",
          na = "NA",
          row.names = FALSE)

write.csv(floy_tbl,
          "./123d_analysis_2020/data/123d_2020_floy.csv",
          na = "NA",
          row.names = FALSE)


write.csv(waterdata,
          "./123d_analysis_2020/data/123d_2020_usgswater.csv",
          na = "NA",
          row.names = FALSE)

write.csv(cpue,
          "./123d_analysis_2020/data/123d_2020_cpue.csv",
          na = "NA",
          row.names = FALSE)

write.csv(cpue_ls,
          "./123d_analysis_2020/data/123d_2020_cpue_ls.csv",
          na = "NA",
          row.names = FALSE)


