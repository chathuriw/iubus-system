## Custom functions ########################################
library(chron)

# function for determining average ridership and variance in good and bad weather
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


# function to get the mean ridership when precipitation is above and below a threshold
precipMean <- function(df, precThres) {
    zero <- NULL
    great_zero <- NULL
    PrecipMeans <- data.frame(NoPrecip.Mean = 0, NoPrecip.SD = 0, Precip.Mean = 0, Precip.SD = 0)
    for (i in 1:nrow(df)) {
        row <- df[i,]
        if ((row$PrecipTotal == "0") | (grepl("T", row$PrecipTotal)) | (row$PrecipTotal < precThres)) {
            zero <- append(zero, row$Ridership)
        }
        if (row$PrecipTotal > precThres) {
            great_zero <- append(great_zero, row$Ridership)
        }
    }
    PrecipMeans$NoPrecip.Mean = mean(zero, na.rm =TRUE)
    PrecipMeans$NoPrecip.SD = sd(zero, na.rm =TRUE)
    PrecipMeans$NoPrecip.error = 1.96 * sd(zero, na.rm =TRUE) / sqrt(length(zero))
    PrecipMeans$Precip.Mean = mean(great_zero, na.rm =TRUE)    
    PrecipMeans$Precip.SD = sd(great_zero, na.rm =TRUE) 
    PrecipMeans$Precip.error = 1.96 * sd(great_zero, na.rm =TRUE) / sqrt(length(great_zero))
    return(PrecipMeans)
}
####
# function to get results using a series of thresholds
testThresholds <- function(df, day) {
    thresholds <- c(0,.01, .025, .05, .075, .1, .25, .5, .75, 1) # inches of rain
    results <- data.frame(Day = character(), Threshold = numeric(), meanBelowThres = numeric(), meanAboveThres = numeric(), stringsAsFactors = FALSE)
    i <- 1
    for (t in thresholds) {
        mean_vec <- precipMean(df,t)
        results[i, ] <- c(day, t, mean_vec[1], mean_vec[2])
        i <- i+1
    }
    return(results)
}
######################################################################################

# Set-up
f14 <- read.csv("ridership+weather_Fall-2014.csv", header=TRUE, stringsAsFactors = FALSE)
s15 <- read.csv("ridership+weather_Spring-2015.csv", header=TRUE, stringsAsFactors = FALSE)
daySplits <- split(f14, f14$Day)
daySplits15 <- split(s15, s15$Day)
days <- c("Monday", "Tuesday", "Wednesday","Thursday", "Friday", "Saturday", "Sunday")
avgRidersDay <- data.frame(Days = c("MonWed","TuesThur", "Friday"), AvgRiders = 0, AvgRidersNoPrecip = 0, AvgRidersPrecip = 0 )

# create data frames grouped into days of week-- i.e., Mon-Wed(MW), Tues-Thurs(TR), Friday(Fr)
MW <- daySplits$Monday
MW <- rbind(MW, daySplits15$Monday)
MW <- rbind(MW, daySplits$Wednesday)
MW <- rbind(MW, daySplits15$Wednesday)

TR <- daySplits$Tuesday
TR <- rbind(TR, daySplits15$Tuesday)
TR <- rbind(TR, daySplits$Thursday)
TR <- rbind(TR, daySplits15$Thursday)

Fr <- daySplits$Friday
Fr <- rbind(Fr, daySplits15$Friday)

# replace ridership values with less than 1000 riders with NA's (Holidays and end of school days)
MW$Ridership[MW$Ridership < 1000] <- NA 
TR$Ridership[TR$Ridership < 1000] <- NA 
Fr$Ridership[Fr$Ridership < 1000] <- NA 

# get means of ridership and put in avgRidersDay data frame
avgRidersDay$AvgRiders <- c(mean(MW$Ridership, na.rm = TRUE), mean(TR$Ridership, na.rm = TRUE), mean(Fr$Ridership, na.rm = TRUE))
avgRidersDay
#    Days   AvgRiders AvgRidersNoPrecip AvgRidersPrecip
#1   MonWed  17828.02                 0               0
#2 TuesThur  17617.17                 0               0
#3   Friday  10758.94 


##############################################
# Some results

mwdf <- testThresholds(MW, "MW") 
trdf <- testThresholds(TR, "TR") 
fdf <- testThresholds(Fr, "F") 
precipMeanDF <- data.frame(Day = character(), Threshold = numeric(), meanBelowThres = numeric(), meanAboveThres = numeric(), stringsAsFactors = FALSE)
precipMeanDF <- rbind(precipMeanDF, mwdf, trdf, fdf)
precipMeanDF
#     Day Threshold meanBelowThres meanAboveThres
# 1   MW     0.000       17629.76      18297.579
# 2   MW     0.010       17629.76      18229.778
# 3   MW     0.025       17344.98      19163.471
# 4   MW     0.050       17430.75      19136.400
# 5   MW     0.075       17506.57      19089.077
# 6   MW     0.100       17506.57      19089.077
# 7   MW     0.250       17685.39      18989.429
# 8   MW     0.500       17774.56      18915.000
# 9   MW     0.750       17798.42      18745.500
# 10  MW     1.000       17824.86      18027.000
# 11  TR     0.000       17627.64      17594.150
# 12  TR     0.010       17627.64      17743.105
# 13  TR     0.025       17564.00      17743.105
# 14  TR     0.050       17573.02      17749.625
# 15  TR     0.075       17604.10      17659.867
# 16  TR     0.100       17608.26      17649.000
# 17  TR     0.250       17546.11      17959.545
# 18  TR     0.500       17541.07      17841.875
# 19  TR     0.750       17599.16      18175.500
# 20  TR     1.000       17599.16      18175.500
# 21   F     0.000       11127.20       9443.714
# 22   F     0.010       11127.20       9443.714
# 23   F     0.025       11127.20       9443.714
# 24   F     0.050       11127.20       9443.714
# 25   F     0.075       11127.20       9443.714
# 26   F     0.100       11127.20       8816.500
# 27   F     0.250       11207.19       8816.500
# 28   F     0.500       10846.44      10286.400
# 29   F     0.750       10915.43       9663.500
# 30   F     1.000       10821.73       9817.000

########
## Days taken together (Mon, Tues, Wed, Thurs)
MTWR <- rbind(MW, TR)
mtwr <- testThresholds(MTWR, "MTWR")
mtwr
#     Day Threshold meanBelowThres meanAboveThres
# 1  MTWR     0.000       17628.71       17936.85
# 2  MTWR     0.010       17628.71       17979.86
# 3  MTWR     0.025       17452.11       18413.83
# 4  MTWR     0.050       17501.89       18420.65
# 5  MTWR     0.075       17554.36       18323.43
# 6  MTWR     0.100       17556.91       18342.37
# 7  MTWR     0.250       17618.28       18360.06
# 8  MTWR     0.500       17663.85       18134.55
# 9  MTWR     0.750       17698.79       18460.50
# 10 MTWR     1.000       17712.91       18126.00

# .025 seems to be the most useful threshold
# 17,452 vs 18,413 on Mon, Tues, Wed, Thurs (5.2% inrease)
# 11,127 vs 9,443 on Friday (17.8% decrease)
#

####################################################
######    Look at average temperature  #############

# Start with splitting below and above freezing
mtwr_freeze <- split(MTWR, cut(as.numeric(MTWR$Tavg), c(-100,32,100), include.lowest=TRUE))
mean(mtwr_freeze$`[-100,32]`$Ridership, na.rm=TRUE)
sd3.a <- sd(mtwr_freeze$`[-100,32]`$Ridership, na.rm=TRUE)
l3.a <- length(na.omit(mtwr_freeze$`[-100,32]`$Ridership))
# 18968.69
mean(mtwr_freeze$`(32,100]`$Ridership, na.rm=TRUE)
sd3.b <- sd(mtwr_freeze$`(32,100]`$Ridership, na.rm=TRUE)
l3.b <- length(na.omit(mtwr_freeze$`(32,100]`$Ridership))
# 17226.84
# not surprising results. Rideship increased 9.18 % when the average temperature for the day was below freezing



means <- precipMean(MTWR, .025)
#   NoPrecip.Mean NoPrecip.SD Precip.Mean Precip.SD NoPrecip.error Precip.error
#     17452.11    3558.608    18324.07  2039.477       701.0009     495.8134
means.F <- precipMean(Fr, .025)
###########################################################
# example of finding substring in weather codes
# test <- f14$CodeSum[1]
# grep("FG", test) # returns integer which is starting position in string


# reading in new .csv files for further processing and getting avg variance times

af <- read.csv("results_a_route_fall.csv", header=TRUE, stringsAsFactors = FALSE)
as <- read.csv("results_a_route_spring.csv", header=TRUE, stringsAsFactors = FALSE)
bf <- read.csv("results_b_route_fall.csv", header=TRUE, stringsAsFactors = FALSE)
bs <- read.csv("results_b_route_spring.csv", header=TRUE, stringsAsFactors = FALSE)
ef <- read.csv("results_e_route_fall.csv", header=TRUE, stringsAsFactors = FALSE)
es <- read.csv("results_e_route_spring.csv", header=TRUE, stringsAsFactors = FALSE)
xf <- read.csv("results_x_route_fall.csv", header=TRUE, stringsAsFactors = FALSE)
xs <- read.csv("results_x_route_spring.csv", header=TRUE, stringsAsFactors = FALSE)

# Create new Time column "Sched.Time" which contains "time" class scheduled times
af$Sched.Time <- chron(times = af$Start.Time.Schedule)
as$Sched.Time <- chron(times = as$Start.Time.Schedule)
bf$Sched.Time <- chron(times = bf$Start.Time.Schedule)
bs$Sched.Time <- chron(times = bs$Start.Time.Schedule)
ef$Sched.Time <- chron(times = ef$Start.Time.Schedule)
es$Sched.Time <- chron(times = es$Start.Time.Schedule)
xf$Sched.Time <- chron(times = xf$Start.Time.Schedule)
xs$Sched.Time <- chron(times = xs$Start.Time.Schedule)

# Change Date Column from character to "date"
af$Date <- chron(dates = af$Date) 
as$Date <- chron(dates = as$Date)
bf$Date <- chron(dates = bf$Date) 
bs$Date <- chron(dates = bs$Date) 
ef$Date <- chron(dates = ef$Date) 
es$Date <- chron(dates = es$Date) 
xf$Date <- chron(dates = xf$Date) 
xs$Date <- chron(dates = xs$Date) 

af$TotalRidership <- as.numeric(af$TotalRidership)
as$TotalRidership <- as.numeric(as$TotalRidership)
bf$TotalRidership <- as.numeric(bf$TotalRidership)
bs$TotalRidership <- as.numeric(bs$TotalRidership)
ef$TotalRidership <- as.numeric(ef$TotalRidership)
es$TotalRidership <- as.numeric(es$TotalRidership)
xf$TotalRidership <- as.numeric(xf$TotalRidership)
xs$TotalRidership <- as.numeric(xs$TotalRidership)

af.sched.times <- sort(unique(af$Sched.Time))
af.times <- data.frame(Time = sched.times)

# Get the results for each data frame
af.results <- getAvgVariance(af)
af.results$Time <- chron(times = af.results$Time)
af.results <- af.results[-1, ] # delete first row, which was intialized with zeros
write.csv(af.results, "A.F.averages.csv", row.names=FALSE)
as.results <- getAvgVariance(as)
as.results$Time <- chron(times = as.results$Time)
as.results <- as.results[-1, ]
write.csv(as.results, "A.S.averages.csv", row.names=FALSE)

bf.results <- getAvgVariance(bf)
bf.results$Time <- chron(times = bf.results$Time)
bf.results <- bf.results[-1, ]
write.csv(bf.results, "B.F.averages.csv", row.names=FALSE)
bs.results <- getAvgVariance(bs)
bs.results$Time <- chron(times = bs.results$Time)
bs.results <- bs.results[-1, ]
write.csv(bs.results, "B.S.averages.csv", row.names=FALSE)

ef.results <- getAvgVariance(ef)
ef.results$Time <- chron(times = ef.results$Time)
ef.results <- ef.results[-1, ]
write.csv(ef.results, "E.F.averages.csv", row.names=FALSE)
es.results <- getAvgVariance(es)
es.results$Time <- chron(times = es.results$Time)
es.results <- es.results[-1, ]
write.csv(es.results, "E.S.averages.csv", row.names=FALSE)

xf.results <- getAvgVariance(xf)
xf.results$Time <- chron(times = xf.results$Time)
xf.results <- xf.results[-1, ]
write.csv(xf.results, "X.F.averages.csv", row.names=FALSE)
xs.results <- getAvgVariance(xs)
xs.results$Time <- chron(times = xs.results$Time)
xs.results <- xs.results[-1, ]
write.csv(xs.results, "X.S.averages.csv", row.names=FALSE)





####################################################################################

# Creating bar chart for total daily ridership
c1 <- c(17452,11127,18968)
c2 <- c(18413,9443,17226)
df <- rbind(c1, c2)

barplot(df, beside = TRUE,
        main = "Weather Effects on Total Daily Ridership", 
        xlab = "Weather", ylab = "Ridership", 
        ylim = c(0, 24000), 
        names.arg = c("<.025   >.025in.\nPrecip M-R ", "<.025   >.025in.\nPrecip Friday", "<32deg  >32deg\nAvg Daily Temp"),
        col = c("skyblue4", "skyblue1"),
        legend.text = c("Below Threshold","Above Threshold"),
        args.legend = list(x = "topright"))
require(Hmisc)
e1.a <- means$NoPrecip.error
e1.b <- means$Precip.error
e2.a <- means.F$NoPrecip.error
e2.b <- means.F$Precip.error
e3.a <- 1.96 * sd3.a / sqrt(l3.a)
e3.b <- 1.96 * sd3.b / sqrt(l3.b)

heights = c(17452, 18413, 11127, 9443, 18968, 17226)
upper = c(17452+e1.a, 18413+e1.b, 11127+e2.a, 9443+e2.b, 18968+e3.a, 17226+e3.b)
lower = c(17452-e1.a, 18413-e1.b, 11127-e2.a, 9443-e2.b, 18968-e3.a, 17226-e3.b)
bpx <- c(1.5, 2.5, 4.5, 5.5, 7.5, 8.5)
errbar(bpx, heights, upper, lower, add=T, xlab="")


