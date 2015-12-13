library(RMySQL)

# A route M-R
getRouteProgression(331)
getRouteProgression(331, 67)
getRouteProgression(354)
getRouteProgression(354, 67)

# B route M-R
getRouteProgression(357, 24, TRUE)
getRouteProgression(318, 24, TRUE)

# E route M-R
getRouteProgression(361, 62)
getRouteProgression(320, 62)

# X route M-R
getRouteProgression(364, 76, threshold=5, TRUE)
# Error in route 325
getRouteProgression(325, 76, threshold=5, TRUE)


getRouteProgression <- function(routeID, lastStop=-1,
                                verbose=FALSE, threshold=1000){
    ## connect to local db
    mydb = dbConnect(MySQL(), user='rojahend', password='password', dbname='buses',
        host='localhost')
    query = paste('SELECT * FROM intervaldata where route_id=', routeID, ';')
    Route<- dbGetQuery(mydb, query)
    ## only the stops that have been visited more than ~1000 times really matter
    trueStops <- as.numeric(names(table(Route$to)[which(table(Route$to) > threshold)]))
    trueStopstr <- paste(as.character(trueStops), collapse=",")
    if(verbose){
        print(paste("True stops: ", trueStopstr))
    }
    if(lastStop == "-1"){
        print("choose the last stop and try again:")
        dbDisconnect(mydb)
        return(trueStopstr)
    }
    ## print(trueStopstr)
    ## modify Route to only include datapoints for trueStops
    query <- paste("SELECT * FROM intervaldata where route_id=", routeID,
                   "and `to` IN (", trueStopstr, ") and `from` IN (",
                   trueStopstr, ") order by `when`;", collapse="")
    Route <- dbGetQuery(mydb, query)
    stop <- lastStop
    routeProgression = list()
    for (i in 1:length(trueStops)) {
        ## get the two most common `to` values associated with each `from`
        posibilities <- sort(
            table(Route$to[which(Route$from == stop)]),
            decreasing=TRUE)
        if(verbose){
            print("")
            print(paste("`to` frequency where `from` = ", stop))
            print(posibilities)
        }
        posibilities <- names(posibilities)
        ## one of these `to` values will match `from`. Get the other one.
        if (as.numeric(posibilities[1]) == stop){
            stop <- posibilities[2] }
        else {
            stop <- posibilities[1] }
        routeProgression <- c(routeProgression, stop)
    }
    dbDisconnect(mydb)
    return(unlist(as.numeric(routeProgression)))
}

