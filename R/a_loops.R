library(RMySQL)

### connect to local db
mydb = dbConnect(MySQL(), user='rojahend', password='password', dbname='buses',
    host='localhost')
dbListTables(mydb)

dbGetQuery(mydb, 'describe intervaldata;')

### get all the data and then get a small chunk of it.
intervaldata <- dbGetQuery(mydb, 'SELECT * FROM intervaldata;')
intdat <- dbGetQuery(mydb, 'SELECT * FROM intervaldata limit 1000;')

### all routeids and their frequency. We will start by focusing on route 331
ARoute <- dbGetQuery(mydb, 'SELECT * FROM intervaldata where routeid=331;')

### only the stops that have been visited more than ~1000 times really matter
trueStops <- as.numeric(names(table(ARoute$to)[which(table(ARoute$to) > 1000)]))
trueStopstr <- paste(as.character(trueStops), collapse=",")

### modify ARoute to only include datapoints for trueStops
query <- paste("SELECT * FROM intervaldata where routeid=331 and `to` IN (", trueStopstr, ") and `from` IN (", trueStopstr, ") order by `when`;", collapse="")
ARoute <- dbGetQuery(mydb, query)

stop <- 35
routeProgression = list()
for (i in 1:length(trueStops)) {
    ## get the two most common `to` values associated with each `from`
    posibilities <- names(sort(
        table(ARoute$to[which(ARoute$from == stop)]),
        decreasing=TRUE)[1:2])
    ## one of these `to` values will match `from`. Get the other one.
    if (as.numeric(posibilities[1]) == stop){
        stop <- posibilities[2] }
    else {
        stop <- posibilities[1] }
    routeProgression <- c(routeProgression, stop)
}

unlist(routeProgression)
### 67->39->38->37->41->1->4->6->8->10->11->12->13->14->36->30->34->35
