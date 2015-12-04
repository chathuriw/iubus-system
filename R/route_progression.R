library(RMySQL)


routeID <- 357


### connect to local db
mydb = dbConnect(MySQL(), user='rojahend', password='password', dbname='buses',
    host='localhost')
### all routeids and their frequency. We will start by focusing on route routeID
query = paste('SELECT * FROM intervaldata where route_id=', routeID, ';')
Route<- dbGetQuery(mydb, query)
### only the stops that have been visited more than ~1000 times really matter
trueStops <- as.numeric(names(table(Route$to)[which(table(Route$to) > 1000)]))
trueStopstr <- paste(as.character(trueStops), collapse=",")
print(trueStopstr)


lastStop <- 88


### modify Route to only include datapoints for trueStops
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
    posibilities <- names(posibilities)
    ## one of these `to` values will match `from`. Get the other one.
    if (as.numeric(posibilities[1]) == stop){
        stop <- posibilities[2] }
    else {
        stop <- posibilities[1] }
    routeProgression <- c(routeProgression, stop)
}
unlist(routeProgression)
