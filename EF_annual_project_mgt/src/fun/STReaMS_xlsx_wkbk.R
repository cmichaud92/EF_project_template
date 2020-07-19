


library(openxlsx)


STReaMS_xlsx_wkbk <- function(data = data, year, Project, output_path = "./05_output") {
    
    # Set Default options (only work on cols where data is present)
    options("openxlsx.datetimeFormat" = "yyyy-mm-dd hh:mm:ss")            # Sets default date format 
    options("openxlsx.numFmt" = "0.0")                                    # Sets default numeric to 1 decimal place
    
    # Set style formats for specific formatting requirements
    n0 <- createStyle(numFmt = "0")                                        # Creates a style object for interger 
    n1<- createStyle(numFmt = "0.0")                                       # One decimal place
    n4 <- createStyle(numFmt = "0.0000")                                   # Four decimal places
    t <- createStyle(numFmt = "@")                                         # Text formatting
    
    # Rare Fish fmt
    r_n0 <- c(8, 25)
    r_n1 <- c(7, 12:14)
    r_n4 <- c(26, 27)
    r_t <- c(1:6, 10, 11, 15:24, 28, 29)
    
    #Site-effort
    s_n0 <- c(16, 19, 21)
    s_n1 <- c(9, 10, 17, 24:26)
    s_n4 <- c(22, 23)
    s_t <- c(3:8, 13:15, 18, 20, 27)
    
    # Nontagged fish
    n_n0 <- c(1, 2, 8, 12, 40)
    n_n1 <- c(7, 14:16)
    n_n4 <- c(41, 42)
    n_t <- c(3:6, 10, 11, 13, 17:39, 43)
    
    # Create a blank workbook
    B <- createWorkbook()                                                 
    
    # Add worksheets and data
    addWorksheet(wb = B, sheetName = "Rare Fish datasheet")                # Adds a blank sheet and names it
    writeData(wb = B, sheet = 1, x = data$rare)                            # Writes data to the blank sheet
    # Add column formatting
    addStyle(B, 1, style = n0,                                             # Associates styles to specific cols
             rows = 2:(nrow(data$site)+1), 
             cols = r_n0, 
             gridExpand = TRUE)
    addStyle(B, 1, style = n1,
             rows = 2:(nrow(data$site)+1),
             cols = r_n1,
             gridExpand = TRUE)
    addStyle(B, 1, style = n4,
             rows = 2:(nrow(data$site)+1),
             cols = r_n4,
             gridExpand = TRUE)
    addStyle(B, 1, style = t,
             rows = 2:(nrow(data$site)+1),
             cols = r_t,
             gridExpand = TRUE)
    
    
    # Add site data
    addWorksheet(wb = B, sheetName = "Sample - Site-effort datasheet")    # Adds a blank sheet and names it
    writeData(wb = B, sheet = 2, x = data$site) 
    
    addStyle(B, 2, style = n0,                                             # Associates it with col 16
             rows = 2:(nrow(data$site)+1), 
             cols = s_n0, 
             gridExpand = TRUE)
    addStyle(B, 2, style = n1,
             rows = 2:(nrow(data$site)+1),
             cols = s_n1,
             gridExpand = TRUE)
    addStyle(B, 2, style = n4,
             rows = 2:(nrow(data$site)+1),
             cols = s_n4,
             gridExpand = TRUE)
    addStyle(B, 2, style = t,
             rows = 2:(nrow(data$site)+1),
             cols = s_t,
             gridExpand = TRUE)
    
    # Add nontagged fish data
    addWorksheet(wb = B, sheetName = "Non-tagged Fish datasheet")
    writeData(wb = B, sheet = 3, x = data$ntf)
    
    addStyle(B, 3, style = n0,                                             # Associates it with col 16
             rows = 2:(nrow(data$ntf)+1), 
             cols = n_n0, 
             gridExpand = TRUE)
    addStyle(B, 3, style = n1,
             rows = 2:(nrow(data$ntf)+1),
             cols = n_n1,
             gridExpand = TRUE)
    addStyle(B, 3, style = n4,
             rows = 2:(nrow(data$ntf)+1),
             cols = n_n4,
             gridExpand = TRUE)
    addStyle(B, 3, style = t,
             rows = 2:(nrow(data$ntf)+1),
             cols = n_t,
             gridExpand = TRUE)
    
    
    saveWorkbook(wb = B, file = file.path(output_path, paste0(paste("STReaMS_fmt", year, Project, sep = "_"), ".xlsx")),
                 overwrite = TRUE)
}


