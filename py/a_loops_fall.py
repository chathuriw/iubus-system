import sys
import MySQLdb
# import re
# from datetime import date
# from datetime import timedelta

# add 'limit 1000' to last row of query if DEBUG = True
newrow_i = 0
DEBUG = False


def maybeAddZero(intput):
    if intput < 10:
        return "0" + str(intput)
    else:
        return str(intput)


def getDate(datetime):
    return str(datetime.year) + "-" + maybeAddZero(datetime.month)\
        + "-" + maybeAddZero(datetime.day)


def getTime(datetime):
    timeout = maybeAddZero(datetime.hour)
    timeout = timeout + ":"
    timeout = timeout + maybeAddZero(datetime.minute)
    timeout = timeout + ":"
    timeout = timeout + maybeAddZero(datetime.second)
    return timeout


# writes out the current row and sets up a new one
def newRow(line):
    global newrow_i
    print "\rNew row number", newrow_i
    newrow_i = newrow_i + 1
    when_output = getDate(line[6]) + ',' + str(line[4]) + ','
    time_output = getDate(line[6]) + ',' + str(line[4]) + ','
    return [when_output, time_output]


def writeRow(when_output, time_output):
    global DEBUG
    # fill any missing columns with blanks
    while when_output.count(',') < 37:
        when_output = when_output + ','
        time_output = time_output + ','
    if DEBUG:
        print when_output, '\n'
    global fout_when
    fout_when.write(when_output[:-1] + "\n")
    if DEBUG:
        print time_output, '\n\n'
    global fout_time
    fout_time.write(time_output[:-1] + "\n")


# j and k are `to` and `from` indices of aStops respectively
# aStops[j],aStops[k] returns the values:
# 67,67 -> increment -> 67,39 -> increment -> 39,39 -> etc
# and these indicies will loop through the stops repeatedly
def increment(j, k):
    if j == k:
        # if we're at the last stop, return to the first one
        if j == 17:
            j = 0
        else:
            j = j + 1
    else:
        k = j
    return [j, k]


def resetJK():
    global DEBUG
    if DEBUG:
        print "resetting incrementors..."
    return [0, 17]


# connect to remote mysql server
mysqldb = MySQLdb.connect(host="156.56.179.122", user="b565",
                           passwd="b565Pwd", db="b565")

# connect to local mysql server
#mysqldb = MySQLdb.connect(host="localhost", user="rojahend",
#                          passwd="password", db="buses")
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
;
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
if DEBUG:
        print output

# write the column names to an output file
fout_when = open("a_route_fall_when.csv", 'w')
fout_time = open("a_route_fall_time.csv", 'w')
fout_when.write(output + "\n")
fout_time.write(output + "\n")

# `from` frequency across all a-route bus stops with `to` set as 67:
#   67   35   11   34   41   10   30   39   36   13    1    8   14   38
# 6852 6473  237   48   24   20    6    5    3    2    1    1    1    1

# query row format:
# (id, from, to, time, busid, routeid, datetime)
# example query row:
# (12345, 8L, 10L, 42L, 641L, 331L, datetime.datetime(2014, 8, 19, 11, 20, 25))

# first column: to67,  line[1] == 35, line[2] == 67  aka  j = 17, k = 0
# last column: from35, line[1] == 35, line[2] == 35  aka  j = 17, k = 17

# clear the column names from output and initialize loop variables
when_output = ""
time_output = ""
inLoop = False
shouldWrite = True

# k = to, j = from
[j, k] = resetJK()

# for every row in the result of our mysql query...
for i in range(1, cursor.rowcount):
    line = cursor.fetchone()

    # if we're just starting out, start a new row
    if when_output == "" or time_output == "":
        if DEBUG:
            print "let's start!"
        [when_output, time_output] = newRow(line)
        [j, k] = resetJK()
    # else, if we're suddenly looking at a different day
    elif getDate(line[6]) != when_output[0:10]:
        # let us know and start a new row
        if DEBUG:
            print "current day ", when_output[0:10],
            print " doesn't match ", getDate(line[6]), "...terminate!"
        writeRow(when_output, time_output)
        [when_output, time_output] = newRow(line)
        [j, k] = resetJK()
    # else, if we're suddenly looking at a different bus
    elif str(line[4]) != when_output[11:14]:
        # let us know and start a new row
        if DEBUG:
            print "current bus ", str(line[4])
            print " doesn't match ", when_output[11:14], "...terminate!"
        writeRow(when_output, time_output)
        [when_output, time_output] = newRow(line)
        [j, k] = resetJK()

    # if this row doesn't match the current column...
    while line[1] != aStops[k] or line[2] != aStops[j]:
        if DEBUG:
            print "trying to match: ", line[1], line[2],
            print "\tcurrently looking at ", aStops[k], aStops[j]
        # move forward until we find a match
        when_output = when_output + ","
        time_output = time_output + ","
        # make a new row if it comes to that...
        if j == 17 and k == 17:
            if DEBUG:
                print "looks like we'll have to continue on the next row..."
            writeRow(when_output, time_output)
            [when_output, time_output] = newRow(line)
            [j, k] = resetJK()
        [j, k] = increment(j, k)

    # after escaping the above while loop, this line should match this column
    # add data to this column and move on to the next
    when_output = when_output + getTime(line[6]) + ","
    time_output = time_output + str(line[3]) + ","
    [j, k] = increment(j, k)

    # if we hit the last column (from35), set up a new row
    if line[1] == 35 and line[2] == 35:
        if DEBUG:
            print "we've reached the end of the line!"
        writeRow(when_output, time_output)
        [when_output, time_output] = newRow(line)
        [j, k] = resetJK()

fout_when.close
fout_time.close
