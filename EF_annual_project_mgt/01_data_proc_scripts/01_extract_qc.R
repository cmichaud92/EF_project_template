
##############################################################
#                         EFproj ETL                         #
##############################################################


#-------------------------------
# Attach packages
#-------------------------------

library(tidyverse)
library(lubridate)
library(stringr)
library(openxlsx)
library(sf)
library(googlesheets4)
library(googledrive)
library(UCRBtools)

# source functions
source("./EF_annual_project_mgt/src/fun/exclude.R")
#source("./src/fun/dbf_io_138.R")
source("./EF_annual_project_mgt/src/fun/dp_ef_qcfx_csv.R")

# Create common species vector
#com_spp <- c("CS", "RZ", "HB", "BT", "SD", "FM", "BH", "RT", "SM", "BC",
#             "LG", "BG", "GS", "GC", "BB", "YB", "WE", "GZ", "NP", "WS", "CH")
#native <- c("CS", "RZ", "HB", "BT", "FM", "BH", "RT", "SD", "CH")

#-------------------------------------------------------------------------------

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# Set starting sample number                                            !!!!!!!!
start_num <- 1                                                          #!!!!!!!

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#-------------------------------------------------------------------------------

#-------------------------------
# Create row for meta table
#-------------------------------

# Add project_code and principal!!!!!

meta <- tibble(
  project_code = "123a",
  year = 2020,
  principal_fname = "John",
  principal_lname = "Caldwell",
  agency = "UDWR-M",
  data_type = "EL"
)

#-------------------------------
# Import field data (dbf)
#-------------------------------

data <- dbf_io(file_path_in = "./EF_annual_project_mgt/data/dbf/123a_1") %>%
  map(rename_all, tolower) %>%
  compact()

#-------------------------------
# Create dataset identifier
#-------------------------------

min_pass <- min(data$site$pass)
max_pass <- max(data$site$pass)
proj <- data$site$project[1]
rch <- data$site$reach[1]
data_id <- paste0(proj, "_", rch, "_pass", min_pass, "-", max_pass)

#------------------------------
# Import reach table
#------------------------------

# rvr_join <- read_rds("./data/rvr_rch_tbl.rds")

#------------------------------
# Extract data from list
#------------------------------

# Combine like tables and...
# Remove "Z" and complete easy qc

# Site data
site_tmp <- map_df(data[grepl("site", names(data))], bind_rows) %>%
  mutate_all(na_if, "Z")                                                # Converts "Z"s to NA

# Water data
water_tmp <- map_df(data[grepl("water", names(data))], bind_rows) %>%
  mutate_at(c("cond_amb", "cond_spec", "rvr_temp", "secchi"),
            function(x) {ifelse(x == 0, NA, x)}) %>%                    # Converts 0's to NA
  mutate_all(na_if, "Z")                                                # Converts "Z"s to NA

# Fish data
fish_tmp <- map_df(data[grepl("fish", names(data))], bind_rows) %>%
  mutate_at(c("ilat", "ilon", "tot_length", "st_length", "weight"),     # Converts 0's to NA
            function(x) {ifelse(x == 0, NA, x)}) %>%
  mutate_all(na_if, "Z") %>%                                            # Converts "Z"s to NA
  mutate(ray_ct = na_if(ray_ct, "N"),
         tubercles = ifelse(species %in% spp_nat, tubercles, NA),        # Cleans up additional vars
         rep_cond = toupper(rep_cond))

# Pittag
pit_tmp <- map_df(data[grepl("pittag", names(data))], bind_rows) %>%
  filter(!is.na(pit_num)) %>%
  mutate_all(na_if, "Z")                                                # Converts "Z"s to NA

# Floytag
floy_tmp <- map_df(data[grepl("floytag", names(data))], bind_rows) %>%
  filter(!is.na(floy_num)) %>%
  mutate_all(na_if, "Z") %>%
  select(-floy_id)

#------------------------------
# Modify data
#------------------------------

# Create sample_number and index,
# Create fnl table structures

# Site table

site <- site_tmp %>%
  mutate(startdatetime = as.POSIXct(paste(mdy(date), starttime)),         # Replace `date` and `time` with `datetime`
         enddatetime = as.POSIXct(paste(mdy(date), endtime)),
         el_sec = effort_sec + (effort_min * 60),                         # Convert effort to seconds
         project = tolower(project),
         year = year(startdatetime)) %>%                                  # Add year varaible

  arrange(startdatetime) %>%                                              # this orders data for indexing

  mutate(s_index = row_number(),                                          # add index for qc/site_id
         site_num_crct = s_index + (start_num - 1),
         site_id = paste(project,
                         year(startdatetime),                    # Create sample number
                         str_pad(site_num_crct, 3, "left", "0"),
                         sep = "_")) %>%

  left_join(tbl_reach, by = c("reach" = "rch_code")) %>%                   # Add rvr_abbr variable

  select(s_index, site_id, project,
         year, river = rvr_code,
         reach, pass,
         startdatetime, enddatetime,
         start_rmi, end_rmi,
         shoreline, el_sec,
         boat, crew,
         site_notes, key_a) %>%

  mutate_at(vars(ends_with("rmi")), function(x) {ifelse(.$reach %in% c("DESO", "ECHO"),  # Simple Belknap correction
                                                        x + 120, x)})



samp_n <- select(site, key_a, site_id, t_stamp = startdatetime, reach)       # Create site_id df and apply to all tables.


# Water_qual table

water <- left_join(water_tmp, samp_n, by = "key_a") %>%
  rename(water_id = key_ab,
         water_notes = h2o_notes) %>%
  arrange(t_stamp) %>%
  select(water_id, site_id,
         cond_amb, cond_spec,
         rvr_temp, secchi,
         water_notes, key_a)

# Fish table

fish_1 <- left_join(fish_tmp, samp_n, by = "key_a") %>%
  mutate(datetime = as.POSIXct(paste(as.Date(t_stamp), time))) %>%
  arrange(datetime) %>%
  mutate(f_index = row_number()) %>%
  mutate_at(vars(ends_with("rmi")), function(x) {ifelse(.$reach %in% c("DESO", "ECHO"),
                                                        x + 120, x)}) %>%
  select(f_index,
         fish_id = key_aa,
         site_id, reach,
         rmi, datetime,
         species, tot_length,
         weight, sex,
         rep_cond, tubercles,
         ray_ct, disp,
         fish_notes, key_a,
         ilon, ilat)

fish_sf <- fish_1 %>%                                     # Convert long-lat to UTMs
  group_by(site_id, rmi) %>%
  summarise(ilon = mean(ilon, na.rm = TRUE),
            ilat = mean(ilat, na.rm = TRUE),
            .groups = "drop") %>%
  filter(!is.na(ilon)) %>%
  st_as_sf(coords = c("ilon", "ilat"), crs = 4326) %>%
  st_transform(crs = 32612) %>%
  mutate(loc_x = st_coordinates(geometry)[, 1],
         loc_y = st_coordinates(geometry)[, 2],
         epsg = 32612) %>%
  st_drop_geometry() %>%
  select(site_id, rmi, loc_x, loc_y, epsg)

fish <- full_join(fish_1, fish_sf, by = c("site_id", "rmi")) %>%
  select(-c(ilat, ilon))

# Pittag table

pittag <- left_join(pit_tmp, samp_n, by = "key_a") %>%
  rename(pit_id = key_aaa,
         fish_id = key_aa) %>%
  left_join(select(fish, fish_id, datetime, species), by = c("fish_id")) %>%
  arrange(datetime) %>%
  mutate(p_index = row_number(),
         pit_num = toupper(pit_num)) %>%
  select(p_index, pit_id, fish_id, site_id,
         species,pit_type, pit_num, pit_recap,
         pit_notes, key_a)

# Floytag table

floytag <- left_join(floy_tmp, samp_n, by = "key_a") %>%
  rename(floy_id = key_aab,
         fish_id = key_aa) %>%
  left_join(select(fish, fish_id, datetime, species), by = c("fish_id")) %>%
  arrange(datetime) %>%
  mutate(fl_index = row_number()) %>%
  select(fl_index, floy_id, fish_id, site_id,
         species, floy_color, floy_num, floy_recap,
         floy_notes)

#------------------------------
# QC data.tables
#------------------------------
ck_site <- site_qcfx(site_data = site) %>%
  mutate_if(is.POSIXct, force_tz, tzone = "UTC")


ck_fish <- fish_qcfx(fish_data = fish, site_data = site) %>%
  mutate_if(is.POSIXct, force_tz, tzone = "UTC")

ck_pit <- pit_qcfx(pit_data = pittag, fish_data = fish)

ck_floy <- floy_qcfx(floy_data = floytag, fish_data = fish)

#------------------------------
# Upload data to google drive
#------------------------------
drive_auth(email = "cmichaud@utah.gov")

gs4_auth(token = drive_token())

gs4_create(
  name = paste("raw", data_id, sep = "_"),
  sheets = list(meta = meta,
                ck_site = ck_site,
                ck_fish = ck_fish,
                ck_pit = ck_pit,
                ck_floy = ck_floy,
                water = water)
  )

drive_mv(paste("raw", data_id, sep = "_"),
         path = '123a/')

dir.create(paste0("./EF_annual_project_mgt/output/raw_csv/", data_id))
write_csv(meta, paste0("./EF_annual_project_mgt/output/raw_csv/", data_id, "/raw_meta.csv"), na = "")
write_csv(ck_site, paste0("./EF_annual_project_mgt/output/raw_csv/", data_id, "/raw_site.csv"), na = "")
write_csv(ck_fish, paste0("./EF_annual_project_mgt/output/raw_csv/", data_id, "/raw_fish.csv"), na = "")
write_csv(ck_pit, paste0("./EF_annual_project_mgt/output/raw_csv/", data_id, "/raw_pit.csv"), na = "")
write_csv(ck_floy,paste0("./EF_annual_project_mgt/output/raw_csv/", data_id, "/raw_floy.csv"), na = "")
write_csv(water, paste0("./EF_annual_project_mgt/output/raw_csv/", data_id, "/raw_water.csv"), na = "")

## End
