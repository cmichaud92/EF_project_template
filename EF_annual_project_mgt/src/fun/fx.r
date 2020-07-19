
#######################################################
#                NNF BWR functions                    #
#######################################################

# life_stage() assigns life stage categories to SM, NP, WE based on recovery program specs

# reach() assigns management reaches to major rivers based on rmi


#---------------------------------
# life_stage()
#---------------------------------

life_stage <- function(.data, specvar, lenvar, ...) {
    
    require(dplyr)
    
    .data %>% 
        mutate(ls = case_when(specvar == "SM" &
                                               lenvar < 100 ~ "SUB-JUV",
                                      specvar == "SM" &
                                               lenvar >= 100 & lenvar < 200 ~ "JUV",
                                      specvar == "SM" &
                                               lenvar >= 200 ~ "ADULT",
                                      specvar == "NP" &
                                               lenvar < 300 ~ "JUV",
                                      specvar == "NP" &
                                               lenvar >= 300 ~ "ADULT",
                                      specvar == "WE" &
                                               lenvar < 300 ~ "JUV",
                                      specvar == "WE" &
                                               lenvar >= 300 ~ "ADULT"))
}

# ?addZeroCatch between(lenvar, 100, 200)
# 
# j <- life_stage(.data = nnf, specvar = nnf$species, lenvar = nnf$length)
# j <- life_stage(.data = nnf, specvar = "species", lenvar = "length")


#--------------------------------------------------------
# life_stage2()
#--------------------------------------------------------

life_stage2 <- function(.data, specvar, lenvar, ...) {
    
    require(dplyr)
    
    .data %>% 
        mutate(ls = case_when(specvar == "SM" &
                                  lenvar < 100 ~ "SUB_JUV",
                              specvar == "SM" &
                                  lenvar >= 100 & lenvar < 200 ~ "JUV",
                              specvar == "SM" &
                                  lenvar >= 200 & lenvar < 325 ~ "ADULT",
                              specvar == "SM" &
                                  lenvar >=325 ~ "ADULT_PIC",
                              specvar == "NP" &
                                  lenvar < 300 ~ "JUV",
                              specvar == "NP" &
                                  lenvar >= 300 & lenvar < 450 ~ "ADULT",
                              specvar == "NP" &
                                  lenvar >= 450 ~ "ADULT_PIC",
                              specvar == "WE" &
                                  lenvar < 300 ~ "JUV",
                              specvar == "WE" &
                                  lenvar >= 300 & lenvar < 375 ~ "ADULT",
                              specvar == "WE" &
                                  lenvar >= 375 ~ "ADULT_PIC"))
}

#---------------------------------
# is_piscivore()
#---------------------------------

is_piscivore <- function(.data, specvar, lenvar, ...) {
    
    require(dplyr)
    
    .data %>% 
        mutate(piscivore = as.logical(case_when(specvar == "SM" &
                                                    lenvar >=325 ~ "TRUE",
                                                specvar == "NP" &
                                                    lenvar >= 450 ~ "TRUE",
                                                specvar == "WE" &
                                                    lenvar >= 375 ~ "TRUE",
                                                specvar == "LG" &
                                                    lenvar >= 325 ~ "TRUE",
                                                specvar == "SB" &
                                                    lenvar >= 350 ~ "TRUE",
                                                specvar == "CC" &
                                                    lenvar >= 400 ~ "TRUE",
                                                specvar == "BU" &
                                                    lenvar >= 450 ~ "TRUE",
                                                TRUE ~ "FALSE")))
}


#--------------------------------------------
# reach()
#--------------------------------------------

reach <- function(.data, RiverCode, rmi) {
    
    require(dplyr)
    
    .data %>% 
        mutate(reach = case_when(RiverCode == "GR" &
                                     rmi < 129.5 ~ "LGR",
                                 RiverCode == "GR" &
                                     between(rmi, 129.5, 207.1) ~ "DESO",
                                 RiverCode == "GR" &
                                     between(rmi, 207.1, 321.3) ~ "MGR",
                                 RiverCode == "GR" &
                                     between(rmi, 321.3, 412.7) ~ "UGR",
                                 RiverCode == "CO" &
                                     between(rmi, 0, 111.8) ~ "LCO",
                                 RiverCode == "CO" &
                                     between(rmi, 111.8, 129.3) ~ "WW",
                                 RiverCode == "CO" &
                                     between(rmi, 129.3, 154.6) ~ "RUHT",
                                 RiverCode == "CO" &
                                     between(rmi, 154.6, 189.8) ~ "GRVA",
                                 RiverCode == "CO" &
                                     rmi > 189.8 ~ "UCO",
                                 RiverCode == "CO" &
                                     rmi < -0 ~ "CAT",
                                 RiverCode == "YA" &
                                     rmi < 46.9 ~ "LYA",
                                 RiverCode == "YA" &
                                     between(rmi, 46.9, 189.2) ~ "MYA",
                                 RiverCode == "YA" &
                                     rmi > 189.2 ~ "UYA",
                                 RiverCode == "WH" &
                                     rmi < 71.7 ~ "LWH",
                                 RiverCode == "WH" &
                                     between(rmi, 71.7, 103.6) ~ "MWH",
                                 RiverCode == "DU" &
                                     rmi < 16.7 ~ "LDU",
                                 RiverCode == "DU" &
                                     rmi >= 16.7 ~ "UDU",
                                 RiverCode == "PR" &
                                     rmi < 26.7 ~ "LPR",
                                 RiverCode == "PR" &
                                     between(rmi, 26.7, 91.7) ~ "MPR",
                                 RiverCode == "PR" &
                                     rmi > 91.7 ~ "UPR",
                                 RiverCode == "SR" &
                                     rmi < 45.8 ~ "LSR",
                                 RiverCode == "SR" &
                                     rmi >= 45.8 ~ "USR",
                                 RiverCode == "GU" &
                                     rmi < 30 ~ "LGU",
                                 RiverCode == "GU" &
                                     rmi >= 30 ~ "MGU",
                                 RiverCode == "SJ" &
                                     rmi < 4.5 ~ "LLSJ",
                                 RiverCode == "SJ" &
                                     between(rmi, 4.5, 61.2) ~ "LSJ",
                                 RiverCode == "SJ" &
                                     between(rmi, 61.2, 108) ~ "MSJ",
                                 RiverCode == "SJ" &
                                     between(rmi, 108, 170.5) ~ "USJ",
                                 RiverCode == "SJ" &
                                     rmi >= 170.5 ~ "UUSJ"))
}

