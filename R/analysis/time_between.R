## what data do we want to investigate?

###################################  A ROUTE
RouteID_of_input <- "331_354"

###################################  B ROUTE
## RouteID_of_input <- "357_318"

###################################  E ROUTE (weird later in the day)
## RouteID_of_input <- "361_320"

###################################  X ROUTE
## RouteID_of_input <- "325"

######################################################################
## Run the hunk of code below to set up the predition function
time <- read.csv(paste("../../data/", RouteID_of_input, "_time.csv", sep=""))
rawhen <- read.csv(paste("../../data/", RouteID_of_input, "_when.csv", sep=""))
## 2500 seconds seems like a very reasonable cutoff based on the next line
## plot(density(unlist(time[,3])[which(time[,3] < TIME_CUTOFF)]))
TIME_CUTOFF <- 2500
## 25200 seconds since midnight = 7:00 in the morning.
## weird stuff happens before this, let's remove it
FIRST_TIME <- 25200
## get the number of rows and columns
nrows <- length(rawhen[[1]])
ncols <- length(rawhen[1,])
## Let's transform the when data into something R understands: seconds since midnight
fixTime <- function(strinput){
    splinput <- unlist(strsplit(as.character(strinput), ":"))
    return(
        as.numeric(splinput[1]) * 3600 +
            as.numeric(splinput[2]) * 60 +
                as.numeric(splinput[3]))
}
when <- as.data.frame(apply(rawhen, 1:2, fixTime))
## fixTime shouldn't have been applied to the first two rows
when[[1]] <- as.character(rawhen[[1]])
when[[2]] <- rawhen[[2]]
###################################
## Frontend calculation: get the shape of the spline of the spline
res <- 0.5
## only look at values for which the time is less than TIME_CUTOFF seconds and which occur after 7:00 am
splx <- list()
sply <- list()
## the first and last columns are really weird, exclude them from the model
for(i in 4:ncols-1){
    # filter out weirdly big times or times before the cutoff (ie 7:00 am)
    goodIndices <- which((time[,i] < TIME_CUTOFF) & (when[,i] > FIRST_TIME))
    x <- unlist(when[,i])[goodIndices]
    y <- unlist(time[,i])[goodIndices]
    smoothline<- smooth.spline(x, y, spar=res)
    x <- smoothline$x
    ## while building our model, normalize y values
    ##    so that every column has a max value of TIME_CUTOFF
    y <- smoothline$y * TIME_CUTOFF / max(smoothline$y)
    splx <- c(splx, x)
    sply <- c(sply, y)
}
splineofspline <- smooth.spline(unlist(splx), unlist(sply), spar=res)
splx <- splineofspline$x
sply <- splineofspline$y
## curious what the spline of spline looks like? run the following line
## plot(splx, sply, type='l')
###################################
PredictTime <- function(column, timeofday, shouldPlot=FALSE){
    ## Predict time relies on the presence of splx and sply
    if (! exists("splx") |  ! exists("sply")){
        print("Fatal Error, missing splx and/or sply")
        return()
    }
    ## filter out weirdly big times or times before the cutoff (ie 7:00 am)
    goodIndices <- which((time[,column] < TIME_CUTOFF) &
                             (when[,column] > FIRST_TIME))
    x <- unlist(when[,column])[goodIndices]
    y <- unlist(time[,column])[goodIndices]
    ## fit the normalized spline to the mean and sd of the actual data
    temp <- (sply / sd(sply) * sd(y))
    sply_temp <-  temp - mean(temp) + mean(y)
    ## anything that is predicted to be less than zero.. just make it zero
    sply_temp[which(sply_temp < 0)] <- 0
    if(shouldPlot){
        plot(x, y)
        par(new=T)
        lines(splx, sply_temp , type='l', col="red",
              lwd=5, xlab="", ylab="", main="")
    }
    ## return the predicted time
    return(sply_temp[which.min(abs(splx - timeofday))])
}
## what the labels for each column???
labels(time)[[2]]

## A Route: good predictions
PredictTime(11, 40000, TRUE)
PredictTime(13, 40000, TRUE)

## A Route: alright predictions
PredictTime(31, 40000, TRUE)
PredictTime(8, 40000, TRUE)

## A Route: bad preditions, the first and last columns don't fit in with the rest very well
PredictTime(3, 40000, TRUE)
PredictTime(38, 40000, TRUE)

## A Route: interesting dwell times. You can see a clear difference in times when the bus had to stop and when the bus could breeze through a stop
PredictTime(4, 40000, TRUE)
PredictTime(36, 40000, TRUE)

## All predictions:
PredictTime(3, 40000, TRUE)
PredictTime(4, 40000, TRUE)
PredictTime(5, 40000, TRUE)
PredictTime(6, 40000, TRUE)
PredictTime(7, 40000, TRUE)
PredictTime(8, 40000, TRUE)
PredictTime(9, 40000, TRUE)
PredictTime(10, 40000, TRUE)
PredictTime(11, 40000, TRUE)
PredictTime(12, 40000, TRUE)
PredictTime(13, 40000, TRUE)
PredictTime(14, 40000, TRUE)
PredictTime(15, 40000, TRUE)
PredictTime(16, 40000, TRUE)
PredictTime(17, 40000, TRUE)
PredictTime(18, 40000, TRUE)
PredictTime(19, 40000, TRUE)
PredictTime(20, 40000, TRUE)
PredictTime(21, 40000, TRUE)
PredictTime(22, 40000, TRUE)
PredictTime(23, 40000, TRUE)
PredictTime(24, 40000, TRUE)
PredictTime(25, 40000, TRUE)
PredictTime(26, 40000, TRUE)
PredictTime(27, 40000, TRUE)
PredictTime(28, 40000, TRUE)
PredictTime(29, 40000, TRUE)
PredictTime(30, 40000, TRUE)
PredictTime(31, 40000, TRUE)
PredictTime(32, 40000, TRUE)
PredictTime(33, 40000, TRUE)
PredictTime(34, 40000, TRUE)
PredictTime(35, 40000, TRUE)
PredictTime(36, 40000, TRUE)
PredictTime(37, 40000, TRUE)
PredictTime(38, 40000, TRUE)
PredictTime(39, 40000, TRUE)
PredictTime(40, 40000, TRUE)
PredictTime(41, 40000, TRUE)
PredictTime(42, 40000, TRUE)
PredictTime(43, 40000, TRUE)
PredictTime(44, 40000, TRUE)
PredictTime(45, 40000, TRUE)
PredictTime(46, 40000, TRUE)
