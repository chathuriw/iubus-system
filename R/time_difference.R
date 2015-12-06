data <- read.csv("2015-11-30_230946.csv")
# get the number of rows and columns
nrow <- length(data[[1]])
ncol <- length(data[1,])-1
# get the mean time difference + standard deviation for each time of day across all days
meanDifs <- list()
diffsds <- list()
for(i in 2:length(data[[1]])){
    meanDifs <- c(meanDifs, mean(unlist(data[i,2:ncol])))
    diffsds <- c(diffsds, sd(unlist(data[i,2:ncol])))
}
# the earliest/lates times 95% of the time are +/- 2 standard deviations
earliest = unlist(meanDifs) - 2 * unlist(diffsds)
latest = unlist(meanDifs) + 2 * unlist(diffsds)
# times = seconds since midnight
times <- data[2:108,1]
# plot the results

meanLine <- smooth.spline(times, meanDifs, spar=0.5)
earlyLine <- smooth.spline(times, earliest, spar=0.5)
lateLine <- smooth.spline(times, latest, spar=0.5)
plot(earlyLine, ylim=c(-10, 15), xlab="time", ylab="time difference", type="l")
par(new=T)
plot(meanLine, ylim=c(-10, 15), ylab="", xlab="", type="l")
par(new=T)
plot(lateLine, ylim=c(-10, 15), ylab="", xlab="", type="l")

plot(meanLine, ylim=c(-10, 15), ylab="", xlab="", type="l")
par(new=T)
plot(times, meanDifs, ylim=c(-10, 15))
