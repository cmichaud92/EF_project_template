
############################################################
#      EL project SQLite database quick stats              #
############################################################

library(dplyr)
library(dbplyr)
library(DBI)
library(googledrive)


#----------------------------------------------------
# User defined parameters
#----------------------------------------------------

# Project code
proj <- "123a"

# Year of interest
yoi <- 2020

# db_name <- "name_of_db.sqlite"
db_name <- "123a_WithUpload2.sqlite"

# db_path <-  "path/to/database/" (Google Drive!!! [INCLUDE trailing /])
db_path <- "data_mgt/123a/"

# Your email address (google auth)
# my_email <- "type it in here"
my_email <- "cmichaud@utah.gov"


#-------------------------------
# Google Drive auth and io
#-------------------------------

# ----- Authenticate to google drive -----

drive_auth(email = my_email)


# ----- Locate database -----

el_db <- drive_get(paste0(db_path, db_name))

tmp <- tempfile(fileext = ".sqlite")
drive_download(el_db, path = tmp, overwrite = TRUE)


# ----- Connect to database -----

con <-  dbConnect(RSQLite::SQLite(), tmp)
# dbListTables(con)



#-------------------------------------------
# Stats fx:  similar to the one used in `etl_a` script
#            but adds `reach` to grouping

stats_qcfx <- function(site_data, fish_data) {
    f <- fish_data %>%
        filter(species == "SM") %>%
        group_by(site_id) %>%
        summarise(SM = n(),
                  .groups = "drop")
    site_data %>%
        left_join(f, by = "site_id") %>%
        group_by(reach, pass) %>%
        summarise(n_site = n(),
                  effort_hr = round(sum(el_sec) / 3600, 2),
                  SM = sum(SM, na.rm = TRUE),
                  cpue = round(SM / effort_hr, 2),
                  .groups = "drop")
}

site <- tbl(con, "site") %>%
    filter(year == yoi) %>%
    collect()

fish <- tbl(con, "fish") %>%
    inner_join(site, by = "site_id", copy = TRUE) %>%
    collect()

summary <- stats_qcfx(site_data = site, fish_data = fish) %>%
    gt::gt()

dbDisconnect(con)
