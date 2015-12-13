library(chron)

getAvgVariance <- function(df) 
{
    result.df <- data.frame(Time = 0, Num.Days = 0, Avg.Var = 0, Avg.Ridership = 0,   
                            Avg.Var.Good.W = 0, Avg.Ridership.Good.W = 0, Avg.Var.Bad.W = 0, Avg.Ridership.Bad.W = 0)
    sched.times <- sort(unique(df$Sched.Time))
    for (time in sched.times) 
    {
        temp.df <- df[df$Sched.Time == time,]
        num.days <- nrow(temp.df)
        avg.riders.total <- mean(temp.df$TotalRidership, na.rm =TRUE)
        avg.var.total <- mean(temp.df$Variance)
        
        precip.split <- split(temp.df, cut(temp.df$TotalPrecipitation, c(-100,.025,100), include.lowest=TRUE))
        avg.ridership.good.w <- mean(as.numeric(precip.split[[1]]$TotalRidership), na.rm=TRUE)
        avg.ridership.bad.w <- mean(as.numeric(precip.split[[2]]$TotalRidership), na.rm=TRUE)
        avg.var.good.w <- mean(as.numeric(precip.split[[1]]$Variance), na.rm=TRUE)
        avg.var.bad.w <- mean(as.numeric(precip.split[[2]]$Variance), na.rm=TRUE)
        new.row <- c(time, num.days, avg.var.total, avg.riders.total, 
                     avg.var.good.w, avg.ridership.good.w, avg.var.bad.w, avg.ridership.bad.w)
       # print(new.row)
        result.df <- rbind(result.df, new.row)
        #print(head(result.df))
    }
    return(result.df)
}
















get_avg_stop_time <- function(routeData, stopData) { 
    for(i in 1:nrow(routeData)) { 
        row <- routeData[i,]
        # do stuff with row
        closestTimeRow <- 0
        ifelse(row$Time > "07:00:00" & row$Time <= "7:37:00", closestTimeRow <- 1,
        ifelse(row$Time > "07:37:00" & row$Time <= "8:04:30", closestTimeRow <- 2,
        ifelse(row$Time > "08:04:30" & row$Time <= "8:37:30", closestTimeRow <- 3,
        ifelse(row$Time > "08:37:30" & row$Time <= "9:21:00", closestTimeRow <- 4,
        ifelse(row$Time > "09:21:00" & row$Time <= "10:08:00", closestTimeRow <- 5,
        ifelse(row$Time > "10:08:00" & row$Time <= "10:48:00", closestTimeRow <- 6,
        ifelse(row$Time > "10:48:00" & row$Time <= "11:32:00", closestTimeRow <- 7,
        ifelse(row$Time > "11:32:00" & row$Time <= "12:18:30", closestTimeRow <- 8,
        ifelse(row$Time > "12:18:30" & row$Time <= "12:59:00", closestTimeRow <- 9,
        ifelse(row$Time > "12:59:00" & row$Time <= "13:42:00", closestTimeRow <- 10,
        ifelse(row$Time > "13:42:00" & row$Time <= "14:29:00", closestTimeRow <- 11,
        ifelse(row$Time > "14:29:00" & row$Time <= "15:12:00", closestTimeRow <- 12,
        ifelse(row$Time > "15:12:00" & row$Time <= "15:50:00", closestTimeRow <- 13,
        ifelse(row$Time > "15:50:00" & row$Time <= "16:30:30", closestTimeRow <- 14,
        ifelse(row$Time > "16:30:30" & row$Time <= "17:12:00", closestTimeRow <- 15,
        ifelse(row$Time > "17:12:00" & row$Time <= "17:53:00", closestTimeRow <- 16,
        ifelse(row$Time > "17:53:00" & row$Time <= "18:35:00", closestTimeRow <- 17,
        ifelse(row$Time > "18:35:00" & row$Time <= "19:13:00", closestTimeRow <- 18,
        ifelse(row$Time > "19:13:00" & row$Time <= "19:49:00", closestTimeRow <- 19,
        ifelse(row$Time > "19:49:00" & row$Time <= "20:28:30", closestTimeRow <- 20,
        ifelse(row$Time > "20:28:30" & row$Time <= "21:07:30", closestTimeRow <- 21,
        ifelse(row$Time > "21:07:30" & row$Time <= "21:42:30", closestTimeRow <- 22,
        ifelse(row$Time > "21:42:30" & row$Time <= "22:17:30", closestTimeRow <- 23,
        ifelse(row$Time > "22:17:30" & row$Time <= "22:52:30", closestTimeRow <- 24,
        ifelse(row$Time > "22:52:30" & row$Time <= "23:27:30", closestTimeRow <- 25,
        ifelse(row$Time > "23:27:30" & row$Time <= "23:59:59", closestTimeRow <- 26,
            closestTimeRow <- 27))))))))))))))))))))))))))
        
        stopRow <- stopData[closestTimeRow,]
        if (stopRow$num_times == 0) {
            stopRow$min = row$Time
            stopRow$max = row$Time
        }
        if (row$Time < stopRow$min) {stopRow$min = row$Time}
        if (row$Time > stopRow$max) {stopRow$max = row$Time}
        stopRow$sum_of_times <- stopRow$sum_of_times + row$Time
        stopRow$num_times <- stopRow$num_times + 1
        stopData[closestTimeRow,] <- stopRow
    }
    return(stopData)
}