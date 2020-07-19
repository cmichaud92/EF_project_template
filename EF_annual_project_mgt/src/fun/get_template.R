
##################################################################################################
#                                        get_template()                                          #
##################################################################################################


# This function accesses the most recient STReaMS field collection templates, sets column types
#       and returns 3 empty DFs analagous to each of the 3 collection sheets: 
#       site_tmplt, rare_tmplt, ntf_tmplt

library(httr)
library(readxl)
library(dplyr)
library(lubridate)
library(purrr)

source("./src/fun/mst.R")

get_template <- function() {
    
    # Create column type vectors to set col types.
    # NOTE: col_types based on v3_21_2019. May require modification to accomodate version updates
    site_col_type <- c(rep("text", 8), rep("numeric", 2), rep("date", 2), rep("text", 3), 
                       rep("numeric", 2), "text", "numeric", "text", rep("numeric", 6), "text")
    
    rare_col_type <- c(rep("text", 6), rep("numeric", 2), "date", rep("text", 2), rep("numeric", 3), 
                       rep("text", 10), rep("numeric", 3), rep("text", 2))
    
    ntf_col_type <- c(rep("numeric", 2), rep("text", 4), rep("numeric", 2), "date", rep("text", 2), 
                      "numeric", "text", rep("numeric", 3), rep("text", 23), rep("numeric", 3), "text")
    
    # access the most recient template from the STReaMS website 
    template <- tempfile(fileext = ".xlsx")  # creates a temp file
    
    GET(url = "https://streamsystem.org/documents/resources/fieldcollection_template_v3-21-2019.xlsx",
        write_disk(template) )        # accesses the web data and writes it to the temp file
    
    
    # Import templates, specifying col types and time zone
    site_tmplt <- read_xlsx(template, 
                            sheet = 4, 
                            col_types =  site_col_type) 
#        modify_if(is.POSIXct, mst)
    
    
    ntf_tmplt <- read_xlsx(template, 
                           sheet = 6,
                           col_types = ntf_col_type) %>% 
        slice(0)
#        modify_if(is.POSIXct, mst)
    
    rare_tmplt <- read_xlsx(template, 
                            sheet = 2,
                            col_types = rare_col_type)
#        modify_if(is.POSIXct, mst) 
    
    template <- list(site_tmplt, ntf_tmplt, rare_tmplt)
    names(template) <- c("site_tmplt", "ntf_tmplt", "rare_tmplt")
    
    template <- map(template, mst2)
}

#a <- get_template()