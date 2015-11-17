library(RMySQL)

mydb = dbConnect(MySQL(), user='b565', password='b565Pwd', dbname='b565',
    host='156.56.179.122')
dbListTables(mydb)

dbGetQuery(mydb, 'describe intervaldata;')

### load A route data
aRouteTimes <- dbGetQuery(mydb, 'SELECT * FROM (SELECT `FROM`, `TO`, BUS_ID, ROUTE_ID, Time(`WHEN`) FROM intervaldata i WHERE i.FROM=67 or i.TO=12 or i.TO=68 or i.TO=6 or i.TO=1 order by time(i.`WHEN`) asc ) e WHERE e.ROUTE_ID=331 and e.`TO`!=e.`FROM`;')
### Change the time column to a(n imperfect) format that R understands
aRouteTimes$Time <-strptime(aRouteTimes$Time, format="%H:%M:%S")

summary(aRouteTimes)

### load A route datapoints that are leaving the stadium
firstStop <- dbGetQuery(mydb, 'SELECT * FROM (SELECT `FROM`, `TO`, BUS_ID, ROUTE_ID, time(`WHEN`) FROM intervaldata i WHERE i.from=67 order by time(i.`WHEN`) asc ) e WHERE e.ROUTE_ID=331 and e.`TO`!=e.`FROM`;')
### convert the times to a format R can understand
firstStop$time <- strptime(firstStop$time, format="%H:%M:%S")

### average actual time of leaving the stadium (ignore the date)
mean(firstStop$time[72:155])

### load A route datapoints that are leaving Well Library
secondStop <- dbGetQuery(mydb, 'SELECT * FROM (SELECT `FROM`, `TO`, BUS_ID, ROUTE_ID, time(`WHEN`) FROM intervaldata i WHERE i.from=1 order by time(i.`WHEN`) asc ) e WHERE e.ROUTE_ID=331 and e.`TO`!=e.`FROM`;')
### R-interpretable date
secondStop$time <- strptime(secondStop$time, format="%H:%M:%S")

### average actual time of leaving Wells (ignore date)
mean(secondStop$time[63:146])

### The first 257 data points come from after midnight the night before
aRouteTimes$FROM[258:1000]
aRouteTimes$TO[258:1000]

### Which dates are described by route id 331?
dates <- as.Date(aRouteTimes$WHEN)
sort(unique(dates))

###############################################################################

### Get all times from August 25th
Aug25th <- dbGetQuery(mydb, "SELECT * FROM (SELECT * FROM intervaldata i WHERE i.FROM=67 or i.FROM=68 or i.TO=1 or i.TO=6 or i.TO=12 or i.TO=67 or i.TO=68) e WHERE e.ROUTE_ID=331 and e.`TO`!=e.`FROM`;")
Aug25th$WHEN <-strptime(Aug25th$WHEN, format="%Y-%m-%d %H:%M:%S")
Aug25th = Aug25th[543:1056,]
### and e.`WHEN`=={Date '2014-08-25}';")

### 25 buses were running at some point on this day
length(unique(Aug25th$BUS_ID))

### get the Mo-Fr A schedule
Aschedule <- dbGetQuery(mydb, 'SELECT * FROM A_Schedule_MR_allbuses;')

### bus stops on August 25th
length(unique(Aug25th$WHEN))

### bus stops schedules for August 25th
length(Aschedule[[1]])*4-4

length(unique())

###############################################################################

### Good table idea:

### int     date   time   time    time...
### BusID   Date   ->67   67->1   1->12   12->32   32->67   67->

### then we format our schedule data in a similar way and match each row of the schedule to the row of the schedule data it fits best.
