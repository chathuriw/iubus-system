library(RMySQL)
library(ggplot2)

mydb = dbConnect(MySQL(), user='b565', password='b565Pwd', dbname='b565',
    host='156.56.179.122')
dbListTables(mydb)

dbGetQuery(mydb, 'describe intervaldata;')

intdates <- dbGetQuery(mydb, 'SELECT Date(`when`) FROM intervaldata;')
intdates <- as.Date(intdates$Date)
intseconds <- as.numeric(intdates)

plot(density(intseconds, adjust=0.4), xaxt='n', xlab="date", ylab="datapoint density",main="When was the interval data collected?")
axis.Date(side=1, intdates, format="%m-%y")
