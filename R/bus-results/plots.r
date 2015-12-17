# creating plots for each route

af <- read.csv("A.F.averages.csv", header=TRUE, stringsAsFactors = FALSE)
as <- read.csv("A.S.averages.csv", header=TRUE, stringsAsFactors = FALSE)
bf <- read.csv("B.F.averages.csv", header=TRUE, stringsAsFactors = FALSE)
bs <- read.csv("B.S.averages.csv", header=TRUE, stringsAsFactors = FALSE)
ef <- read.csv("E.F.averages.csv", header=TRUE, stringsAsFactors = FALSE)
es <- read.csv("E.S.averages.csv", header=TRUE, stringsAsFactors = FALSE)
xf <- read.csv("X.F.averages.csv", header=TRUE, stringsAsFactors = FALSE)
xs <- read.csv("X.S.averages.csv", header=TRUE, stringsAsFactors = FALSE)

af$Time <- chron(times = af$Time)
as$Time <- chron(times = as$Time)
bf$Time <- chron(times = bf$Time)
bs$Time <- chron(times = bs$Time)
ef$Time <- chron(times = ef$Time)
es$Time <- chron(times = es$Time)
xf$Time <- chron(times = xf$Time)
xs$Time <- chron(times = xs$Time)


df <- es
title <- "Average Schedule Variance for route-E Spring"
x <- df$Time
y <- df$Avg.Var.Good.W
y.b <- df$Avg.Var.Bad.W
plot(x,y, xlab="Time", ylab ="Average Variance (Minutes)", main=title)
lo <- loess(y~x)
lo.b <- loess(y.b~x)
xl <- seq(min(x),max(x), (max(x) - min(x))/1000)
lines(xl, predict(lo,xl), col='red3', lwd=4)
lines(xl, predict(lo.b,xl), col='royalblue3', lwd=4)
legend("topright", c("Avg Variance No Precipitation", "Avg Variance Precipitation"), col= c("red3","royalblue3"), lwd=2, lty=c(1,1))


# Creating smoothed plots
plot(smooth.spline(af$Time, af$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-A Fall")
plot(smooth.spline(as$Time, as$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-A Spring")
plot(smooth.spline(bf$Time, bf$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-B Fall")
plot(smooth.spline(bs$Time, bs$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-B Spring")
plot(smooth.spline(ef$Time, ef$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-E Fall")
plot(smooth.spline(es$Time, es$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-E Spring")
plot(smooth.spline(xf$Time, xf$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-X Fall")
plot(smooth.spline(xs$Time, xs$Avg.Var), xlab="Time", ylab="Average Variance", main="Smoothed Plot for route-X Spring")


title2 <- "Average Ridership route-X Spring"
df.r <- xs
x.r <- df.r$Time
y.r <- df.r$Avg.Ridership
y.r.b <- df.r$Avg.Ridership.Bad.W
plot(x.r,y.r, xlab="Time", ylab ="Average Ridership", main=title2)
xl <- seq(min(x),max(x), (max(x) - min(x))/1000)
lo.r <- loess(y.r~x)
lo.r.b <- loess(y.r.b~x)
lines(xl, predict(lo.r,xl), col='blue', lwd=3)
lines(xl, predict(lo.r.b,xl), col='green', lwd=3)
legend("topright", c("Avg Ridership", "Avg Ridership w/ Precip."), col= c("blue","green"), lwd=2, lty=c(1,1))




