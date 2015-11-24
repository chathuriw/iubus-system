import sys
import MySQLdb
# import re
# from datetime import date
# from datetime import timedelta


def maybeAddZero(intput):
    if intput < 10:
        return "0" + str(intput)
    else:
        return str(intput)


def getDate(datetime):
    return str(datetime.year) + "-" + maybeAddZero(datetime.month)\
        + "-" + maybeAddZero(datetime.day)


def getTime(datetime):
    timeout = ""
    timeout = timeout + maybeAddZero(datetime.hour)
    timeout = timeout + ":"
    timeout = timeout + maybeAddZero(datetime.minute)
    timeout = timeout + ":"
    timeout = timeout + maybeAddZero(datetime.second)
    return timeout


# I pulled this chunk out to prevent duplication
# j and k are indices of aStops and aStops[j],aStops[k] returns the values:
#    67,67 -> increment -> 67,39 -> increment -> 39,39 -> etc
def increment(j, k):
    if j == k:
        j = j + 1
    else:
        k = j
    return [j, k]


# connect to remote mysql server
# mysqldb = MySQLdb.connect(host="156.56.179.122", user="b565",
#                           passwd="b565Pwd", db="b565")

# connect to local mysql server
mysqldb = MySQLdb.connect(host="localhost", user="rojahend",
                          passwd="password", db="buses")
cursor = mysqldb.cursor()

# these stops and their order were calculated using a_loops.R
aStops = [67, 39, 38, 37, 41, 1, 4, 6, 8, 10, 11, 12, 13, 14, 36, 30, 34, 35]

# USED TO BUILD THE FOLLOWING HARDCODED QUERY
# This code will probably be useful when we apply this script to other routes
# q = ""
# old = aStops[0]
# for i in aStops:
#     q = q + """
#     (`from`=%(fr)d and `to`=%(to)d) or """ % {"fr": old, "to": i}
#     old = i
#     q = q + """
#     (`from`=%(fr)d and `to`=%(to)d) or """ % {"fr": old, "to": i}
# query = """
# SELECT * FROM intervaldata where
# route_id=331 and (""" + q[:-4] + """)
# order by bus_id asc, `when` asc;
# """
query = """
SELECT * FROM intervaldata where
route_id=331 and (
(`from`=11 and `to`=67) or
(`from`=35 and `to`=67) or
(`from`=67 and `to`=67) or
(`from`=67 and `to`=39) or
(`from`=39 and `to`=39) or
(`from`=39 and `to`=38) or
(`from`=38 and `to`=38) or
(`from`=38 and `to`=37) or
(`from`=37 and `to`=37) or
(`from`=37 and `to`=41) or
(`from`=41 and `to`=41) or
(`from`=41 and `to`=1) or
(`from`=1 and `to`=1) or
(`from`=1 and `to`=4) or
(`from`=4 and `to`=4) or
(`from`=4 and `to`=6) or
(`from`=6 and `to`=6) or
(`from`=6 and `to`=8) or
(`from`=8 and `to`=8) or
(`from`=8 and `to`=10) or
(`from`=10 and `to`=10) or
(`from`=10 and `to`=11) or
(`from`=11 and `to`=11) or
(`from`=11 and `to`=12) or
(`from`=12 and `to`=12) or
(`from`=12 and `to`=13) or
(`from`=13 and `to`=13) or
(`from`=13 and `to`=14) or
(`from`=14 and `to`=14) or
(`from`=14 and `to`=36) or
(`from`=36 and `to`=36) or
(`from`=36 and `to`=30) or
(`from`=30 and `to`=30) or
(`from`=30 and `to`=34) or
(`from`=34 and `to`=34) or
(`from`=34 and `to`=35) or
(`from`=35 and `to`=35))
order by bus_id asc, `when` asc
limit 1000
"""

# execute the query for selecting valid a-route intervals
try:
    cursor.execute(query)
except:
    print "error"
    sys.exit()

# output = the header line of the output csv: 38 column names
output = "day,busid"
for n in aStops:
    output = output + ",to" + str(n) + ",from" + str(n)
print output

# write the column names to an output file
fout_when = open("a_route_when.csv", 'w')
fout_time = open("a_route_time.csv", 'w')
fout_when.write(output)
fout_time.write(output)

# query row format:
# (id, from, to, time, busid, routeid, datetime)
# example query row:
# (12345, 8L, 10L, 42L, 641L, 331L, datetime.datetime(2014, 8, 19, 11, 20, 25))

# `from` frequency across all a-route bus stops with `to` set as 67:
#   67   35   11   34   41   10   30   39   36   13    1    8   14   38
# 6852 6473  237   48   24   20    6    5    3    2    1    1    1    1

# clear the column names from output and initialize loop variables
when_output = ""
time_output = ""
inLoop = False
shouldWrite = True

# j and k are indices of aStops
# aStops[j],aStops[k] returns the values:
# 67,67 -> increment -> 67,39 -> increment -> 39,39 -> etc
# k = to
k = -1
# j = from
j = 0

# for every row in the result of our mysql query...
for i in range(1, cursor.rowcount):
    line = cursor.fetchone()
    shouldWrite = True

    # if we see the start of a loop...
    if line[2] == 67 and line[1] != 67:
        # and if we're currently in a loop
        if inLoop:
            # fill in the rest of the current `when` entry with blanks
            while when_output.count(',') < 37:
                when_output = when_output + ','
            # write out the current `when` entry
            print "`when` ended early.."
            print when_output[:-1], "\n"
            fout_when.write(when_output[:-1] + "\n")
            # fill in the rest of the current `time` entry with blanks
            while time_output.count(',') < 37:
                time_output = time_output + ','
            # write out the current `time` entry
            print "`time` ended early.."
            print time_output[:-1], "\n"
            fout_time.write(time_output[:-1] + "\n")
        inLoop = True
        # when we start a loop, start by printing the date and busid to both
        when_output = getDate(line[6]) + "," + str(line[4]) + ","
        time_output = getDate(line[6]) + "," + str(line[4]) + ","

    # If we're in a loop...
    if inLoop:
        # if this row doesn't match the right column of the output file...
        if line[1] != aStops[k] or line[2] != aStops[j]:
            print "Mismatch!"
            # assume the bus skipped this stop, check the next until
            #   either a match is found or we run out of stops
            while True:
                print "skipped stop"
                print "MISMATCH! expected from aStops[" + str(k) + "]="\
                    + str(aStops[k]), " to aStops[" + str(j) + "]="\
                    + str(aStops[j])
                print "observed:",
                print line[1:]
                when_output = when_output + ","
                time_output = time_output + ","
                [j, k] = increment(j, k)
                # check to see whether we match yet
                if line[1] == aStops[k] and line[2] == aStops[j]:
                    print "found a match! onwards!!"
                    break
                if j + k == 34:
                    print "ran out of stops"
                    shouldWrite = False
                    break
        if shouldWrite:
            when_output = when_output + getTime(line[6]) + ","
            time_output = time_output + str(line[3]) + ","

        # if this row matches the last column of our when_output table...
        if (line[1] == 35 and line[2] == 35) or j + k == 34:
            fout_when.write(when_output[:-1] + "\n")
            fout_time.write(time_output[:-1] + "\n")
            # print what we're writing out
            print when_output[:-1], "\n"
            print time_output[:-1], "\n\n"
            # reset our iterators
            j = -1
            k = -1
            # we're done with this loop
            inLoop = False

        [j, k] = increment(j, k)

    else:
        print "skipped line:", line[1:]

fout_when.close
fout_time.close
