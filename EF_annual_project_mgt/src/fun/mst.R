
###########################################################
#             mst() Mountain Standard Time fx             #
###########################################################

# there is a far better method but these functions will get it done
# map mst2 to a list of dfs to convert to "MST"

mst <- function(x) ymd_hms(x, tz = "MST")


mst2 <- function(x) modify_if(x, is.POSIXct, mst)
