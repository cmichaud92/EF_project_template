
#######################################################################################################
#                                             dbf_io()                                                #
#######################################################################################################

# Generalized function intended to import .dbf files from a DataPlus application.
# Function then returns a list of named dataframes and exports raw data to .rds

#------------------------------------------------------------
# Trouble shooting
# file_path_in <- "./01_data/2018/01_rawData/01_dbfFiles"
# file_path_out <- "./01_data/2018/01_rawData/02_rdsFiles"
# output_name <- "ISMP2018 raw.rds"
#------------------------------------------------------------

dbf_io <- function(file_path_in) {
  
  require(tidyverse)
  require(foreign)
  
  files <- list.files(path = file_path_in, pattern = '.dbf$', full.names = TRUE)
  dat_name <- list()
  dat_name <- as.list(str_extract(files, "(?<=\\+).*(?=\\.dbf)"))
  
  read_dbf <- function(files) { 
    dbf <- read.dbf(files, as.is = TRUE)
    
  }
  
  
  data <- (map(files, read_dbf))
  names(data) <- dat_name
  data <- map(data, as_tibble)
 
  data <- compact(data)

  data
}

#a <- dbf_io(file_path_in = file_path_in, 
#            file_path_out = file_path_out, 
#            output_name = output_name)

