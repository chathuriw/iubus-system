## Custom functions ########################################333
# function to get the mean ridership when precipitation is above and below a threshold
precipMean <- function(df, precThres) {
    zero <- NULL
    great_zero <- NULL
    PrecipMeans <- data.frame(NoPrecip = 0, Precip = 0)
    for (i in 1:nrow(df)) {
        row <- df[i,]
        if ((row$PrecipTotal == "0") | (grepl("T", row$PrecipTotal)) | (row$PrecipTotal < precThres)) {
            zero <- append(zero, row$Ridership)
        }
        if (row$PrecipTotal > precThres) {
            great_zero <- append(great_zero, row$Ridership)
        }
    }
    PrecipMeans$NoPrecip = mean(zero, na.rm =TRUE)
    PrecipMeans$Precip = mean(great_zero, na.rm =TRUE)    
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
# 18968.69
mean(mtwr_freeze$`(32,100]`$Ridership, na.rm=TRUE)
# 17226.84
# not surprising results. Rideship increased 9.18 % when the average temperature for the day was below freezing


###########################################################
# example of finding substring in weather codes
test <- f14$CodeSum[1]
grep("FG", test) # returns integer which is starting position in string
