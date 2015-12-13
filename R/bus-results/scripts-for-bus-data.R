library(chron)


interval <- read.delim("intervaldata2014-2015.tsv")
i1 <- subset(interval, (route_id == '354' | route_id == "331"))
i2 <- subset(i1, (to == '67' | to== '68' | to == '1' | to == '12' | to =='6'))
i3 <- subset(i2, to != from)
aroute$Date <- sapply(strsplit(as.character(aroute$when), " "), "[", 1)
aroute$Time <- sapply(strsplit(as.character(aroute$when), " "), "[", 2)
aroute$Time <- chron(times = aroute$Time)
aroute <- subset(aroute, select = -when)
aroute67 <- subset(aroute, to == 67)

sched <- read.csv("DM-ScheduleData.csv")
stadiumS <- subset(sched, sched$Stop == 'Stadium' & Route == 'A1')



stadiumS_times <- c("7:25:00", "7:49:00", "8:20:00", "8:55:00" , "9:47:00", "10:29:00", "11:07:00", "11:57:00", "12:40:00", "13:18:00", "14:06:00", "14:52:00", "15:32:00", "16:08:00", "16:53:00", "17:31:00", "18:15:00", "18:55:00", "19:31:00", "20:07:00", "20:50:00", "21:25:00", "22:00:00", "22:35:00", "23:10:00", "23:45:00", "00:15:00")
stadiumS_times <- chron(times = stadiumS_times)
stadiumStop_data <- data.frame(stadiumS_times)
namevect <- c("sum_of_times", "num_times", "avg_time", "min", "max")
stadiumStop_data[, namevect] <- 0
stadiumStop_data$sum_of_times <- chron(times = stadiumStop_data$sum_of_times)
stadiumStop_data$avg_time <- chron(times = stadiumStop_data$avg_time)
stadiumStop_data$min <- chron(times = stadiumStop_data$min)
stadiumStop_data$max <- chron(times = stadiumStop_data$max)

ssd <- get_avg_stop_time(aroute67, stadiumStop_data)

ssd$avg_time <- ssd$sum_of_times / ssd$num_times
