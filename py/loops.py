import getopt
import sys
import MySQLdb


primary_routeID = 361

# A Route M-R
# stops = [39, 38, 37, 41,  1,  4,  6,  8, 10,
#          11, 12, 13, 14, 36, 30, 34, 35, 67]

# B Route M-R
# stops = [25, 26, 27, 28, 29, 31, 33, 4, 6, 8,
#          10, 87, 88, 16, 75, 20, 21, 22, 23, 24]

# E Route M-R
# stops = [
#

# X Route M-R
# stops = [
#



# secondary_routeID should have the same stops as primary_routeID
# script will ignore secondary_routeID if it == -1
secondary_routeID = -1

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
    if not DEBUG:
        global newrow_i
        print "\rNew row number", newrow_i, line[6],
        newrow_i = newrow_i + 1
    when_output = getDate(line[6]) + ',' + str(line[4]) + ','
    time_output = getDate(line[6]) + ',' + str(line[4]) + ','
    return [when_output, time_output]


def writeRow(when_output, fout_when, time_output, fout_time, tau):
    global DEBUG
    # fill any missing columns with blanks
    while when_output.count(',') < 37:
        when_output = when_output + ','
        time_output = time_output + ','
    if when_output.count(",,") >= tau:
        return
    if DEBUG:
        print when_output, '\n'
    fout_when.write(when_output[:-1] + "\n")
    if DEBUG:
        print time_output, '\n\n'
    fout_time.write(time_output[:-1] + "\n")


# j and k are `to` and `from` indices of stops respectively
# stops[j],stops[k] returns the values:
# 67,67 -> increment -> 67,39 -> increment -> 39,39 -> etc
# and these indicies will loop through the stops repeatedly
def increment(j, k, nstops):
    if j == k:
        # if we're at the last stop, return to the first one
        if j == nstops:
            j = 0
        else:
            j = j + 1
    else:
        k = j
    return [j, k]


def resetJK(nstops):
    global DEBUG
    if DEBUG:
        print "resetting incrementors..."
    return [0, nstops]


def main(argv):
    global stops
    nstops = len(stops) - 1
    global primary_routeID
    global secondary_routeID
    tau = 18

    # process command line arguments
    try:
        opts, args = getopt.getopt(argv, "r:s:t:h")
    except getopt.GetoptError:
        print "Fatal Error: Invalid Arguments"
        sys.exit()
    for opt, arg in opts:
        if opt == "-r":
            primary_routeID = arg
        elif opt == "-s":
            secondary_routeID = arg
        elif opt == "-t":
            tau = arg
        elif opt == "-h":
            print """
    -r <primary route id>
    -s <second route id>  (same route as primary route id)
    -t <threshold>        (0-16 to specify acceptable number of empty columns)
    """
            sys.exit()

    if secondary_routeID == -1:
        routeID = "(" + str(primary_routeID) + ")"
        outfile_name = str(primary_routeID)
    else:
        routeID = "(" + str(primary_routeID) + ", " +\
                  str(secondary_routeID) + ")"
        outfile_name = str(primary_routeID) + "_" + str(secondary_routeID)

    # connect to remote mysql server
    # mysqldb = MySQLdb.connect(host="156.56.179.122", user="b565",
    #                           passwd="b565Pwd", db="b565")

    # connect to local mysql server
    mysqldb = MySQLdb.connect(host="156.56.179.122", user="b565",
                              passwd="b565Pwd", db="b565")
    cursor = mysqldb.cursor()

    # build the MySQL query
    q = ""
    old = stops[0]
    for i in stops:
        q = q + "\n(`from`=%(fr)d and `to`=%(to)d) or " % {"fr": old, "to": i}
        old = i
        q = q + "(`from`=%(fr)d and `to`=%(to)d) or" % {"fr": old, "to": i}
    query = "SELECT * FROM intervaldata WHERE route_id IN " + routeID +\
            " and (" + q[:-4] + "))\nORDER BY bus_id ASC, `when` ASC;"
    query = query.replace(str(stops[0]), str(stops[-1]), 1)
    if DEBUG:
        query = query[:-1] + " limit 1000;"
        print query

    # execute the query for selecting valid a-route intervals
    try:
        cursor.execute(query)
    except:
        print "error"
        sys.exit()
#     # output = the header line of the output csv: 38 column names
    output = "day,busid"
    for n in stops:
        output = output + ",to" + str(n) + ",from" + str(n)
    if DEBUG:
        print output

    # write the column names to an output file
    fout_when = open(outfile_name + "_when.csv", 'w')
    fout_time = open(outfile_name + "_time.csv", 'w')
    fout_when.write(output + "\n")
    fout_time.write(output + "\n")

    # clear the column names from output and initialize loop variables
    when_output = ""
    time_output = ""

    # k = to, j = from
    [j, k] = resetJK(nstops)

    # for every row in the result of our mysql query...
    for i in range(1, cursor.rowcount):
        line = cursor.fetchone()

        # if we're just starting out, start a new row
        if when_output == "" or time_output == "":
            if DEBUG:
                print "let's start!"
            [when_output, time_output] = newRow(line)
            [j, k] = resetJK(nstops)
        # else, if we're suddenly looking at a different day
        elif getDate(line[6]) != when_output[0:10]:
            # let us know and start a new row
            if DEBUG:
                print "current day ", when_output[0:10],
                print " doesn't match ", getDate(line[6]), "...terminate!"
            writeRow(when_output, fout_when, time_output, fout_time, tau)
            [when_output, time_output] = newRow(line)
            [j, k] = resetJK(nstops)
        # else, if we're suddenly looking at a different bus
        elif str(line[4]) != when_output[11:14]:
            # let us know and start a new row
            if DEBUG:
                print "current bus ", str(line[4])
                print " doesn't match ", when_output[11:14],
                print "...terminate!"
            writeRow(when_output, fout_when, time_output, fout_time, tau)
            [when_output, time_output] = newRow(line)
            [j, k] = resetJK(nstops)

        # if this row doesn't match the current column...
        while line[1] != stops[k] or line[2] != stops[j]:
            if DEBUG:
                print "trying to match: ", line[1], line[2],
                print "\tlooking at ", stops[k], stops[j],
                print "\t[j,k] = [", j, ",", k, "]"
            # move forward until we find a match
            when_output = when_output + ","
            time_output = time_output + ","
            # make a new row if it comes to that...
            if j == nstops and k == nstops:
                if DEBUG:
                    print "looks like we've gotta continue on the next row..."
                writeRow(when_output, fout_when, time_output, fout_time, tau)
                [when_output, time_output] = newRow(line)
            [j, k] = increment(j, k, nstops)

        # after escaping the above while loop, line should match this column
        # add data to this column and move on to the next
        when_output = when_output + getTime(line[6]) + ","
        time_output = time_output + str(line[3]) + ","
        [j, k] = increment(j, k, nstops)

        # if we hit the last column (from35), set up a new row
        if line[1] == stops[nstops] and line[2] == stops[nstops]:
            if DEBUG:
                print "we've reached the end of the line!"
            writeRow(when_output, fout_when, time_output, fout_time, tau)
            [when_output, time_output] = newRow(line)
            [j, k] = resetJK(nstops)

        fout_when.close
        fout_time.close


if __name__ == "__main__":
    main(sys.argv[1:])
