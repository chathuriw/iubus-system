library(chron)

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
