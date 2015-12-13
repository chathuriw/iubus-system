import getopt
import sys
import MySQLdb


######################################################################
# ########## A Route M-R
primary_routeID = 331
secondary_routeID = 354
stops = [39, 38, 37, 41,  1,  4,  6,  8, 10,
         11, 12, 13, 14, 36, 30, 34, 35, 67]

# ########## B Route M-R
# primary_routeID = 357
# secondary_routeID = 318
# stops = [25, 26, 27, 28, 29, 31, 33, 4, 6, 8,
#          10, 87, 88, 16, 75, 20, 21, 22, 23, 24]

# ########## E Route M-R
# primary_routeID = 361
# secondary_routeID = 320
# stops = [48, 60, 50, 51, 52, 45, 46,  6,  8, 10, 11,
#          12, 13, 14,  1, 55, 57, 58, 59, 107, 61, 62]

# ########## X Route M-R
# primary_routeID = 325
# secondary_routeID = -1
# stops = [118, 64, 78, 76]
######################################################################
# secondary_routeID should have the same stops as primary_routeID
# script will ignore secondary_routeID if it == -1

# global variable. True: verbose output + limited data
#                  False: minimal output + all data
DEBUG = False

# global variable. True: connect to shared MySQL database, false: Bo's db
REMOTE = False

# where do we want to write the output?
OUTFILE_DIR = "..\\data\\"   # Windows
# OUTFILE_DIR = "../data/"     # Linux (and Apple?)


def main(argv):
    # get our global variables
    global DEBUG
    global REMOTE
    global OUTFILE_DIR
    global stops
    global primary_routeID
    global secondary_routeID
    nstops = len(stops) - 1
    # default: accept 17 or fewer blank columns (there are 18 columns)
    tau = 16

    # process command line arguments.
    try:
        opts, args = getopt.getopt(argv, "r:s:t:h")
    except getopt.GetoptError:
        print "Fatal Error: Invalid Arguments"
        sys.exit()
    for opt, arg in opts:
        if opt == "-r":
            print "consider changing the route ID in the code instead"
            primary_routeID = arg
        elif opt == "-s":
            print "consider changing the route ID in the code instead"
            secondary_routeID = arg
        elif opt == "-t":
            tau = arg
            print "no more than", tau, "blank columns will be accepted!"
        elif opt == "-h":
            print """
    -r <primary route id>
    -s <second route id>  (same route as primary route id)
    -t <threshold>        (0-16 to specify acceptable number of empty columns)
    """
            sys.exit()

    # consolidate one or two route_id values
    if secondary_routeID == -1:
        routeID = "(" + str(primary_routeID) + ")"
        outfile_name = str(primary_routeID)
    else:
        routeID = "(" + str(primary_routeID) + ", " +\
                  str(secondary_routeID) + ")"
        outfile_name = str(primary_routeID) + "_" + str(secondary_routeID)

    # connect to the right database
    if REMOTE:
        # connect to remote mysql server
        mysqldb = MySQLdb.connect(host="156.56.179.122", user="b565",
                                  passwd="b565Pwd", db="b565")
    else:
        # connect to local mysql server
        mysqldb = MySQLdb.connect(host="localhost", user="rojahend",
                                  passwd="password", db="buses")

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
    cursor = mysqldb.cursor()
    try:
        cursor.execute(query)
    except:
        print "Fatal Error: couldn't connect to database"
        sys.exit()

    # output = the header line of the output csv
    output = "day,busid"
    for n in stops:
        output = output + ",to" + str(n) + ",from" + str(n)
    if DEBUG:
        print output

    # write the column names to an output file
    fout_when = open(OUTFILE_DIR + outfile_name + "_when.csv", 'w')
    fout_time = open(OUTFILE_DIR + outfile_name + "_time.csv", 'w')
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
            # move forward columns until we find a match
            if DEBUG:
                print "trying to match: ", line[1], line[2],
                print "\tlooking at ", stops[k], stops[j],
                print "\t[j,k] = [", j, ",", k, "]"
            # add blank spots to columns that we're skipping
            when_output = when_output + ","
            time_output = time_output + ","
            # make a new row if it really comes down to that...
            if j == nstops and k == nstops:
                if DEBUG:
                    print "looks like we've gotta continue on the next row..."
                writeRow(when_output, fout_when, time_output, fout_time, tau)
                [when_output, time_output] = newRow(line)
            [j, k] = increment(j, k, nstops)

        # after escaping the above loop, this line should match this column
        # add data to this spot and move on to the next
        when_output = when_output + getTime(line[6]) + ","
        time_output = time_output + str(line[3]) + ","
        [j, k] = increment(j, k, nstops)

        # if we hit the last column, write out and start the next row
        if line[1] == stops[nstops] and line[2] == stops[nstops]:
            if DEBUG:
                print "we've reached the end of the line!"
            writeRow(when_output, fout_when, time_output, fout_time, tau)
            [when_output, time_output] = newRow(line)
            [j, k] = resetJK(nstops)

    # close the files when we're done with them
    fout_when.close
    fout_time.close


# keeps the times pretty ie 10:8:3  ->  10:08:03
def maybeAddZero(intput):
    if intput < 10:
        return "0" + str(intput)
    else:
        return str(intput)


# extract a well formatted date from python's datetime thing
def getDate(datetime):
    return str(datetime.year) + "-" + maybeAddZero(datetime.month)\
        + "-" + maybeAddZero(datetime.day)


# extract a well formatted time from python's datetime thing
def getTime(datetime):
    timeout = maybeAddZero(datetime.hour)
    timeout = timeout + ":"
    timeout = timeout + maybeAddZero(datetime.minute)
    timeout = timeout + ":"
    timeout = timeout + maybeAddZero(datetime.second)
    return timeout


# global variable used to keep track of DEBUG == False output
newrow_i = 0


# add the date and bus id to a new row, prints progress if DEBUG == False
def newRow(line):
    if not DEBUG:
        global newrow_i
        print "\rNew row number", newrow_i, line[6],
        newrow_i = newrow_i + 1
    when_output = getDate(line[6]) + ',' + str(line[4]) + ','
    time_output = getDate(line[6]) + ',' + str(line[4]) + ','
    return [when_output, time_output]


# tau = how many blank lines are we ok with? int from 0-18
# fills in any empty spots and writes the finished row to outfile_name.csv
def writeRow(when_output, fout_when, time_output, fout_time, tau):
    global DEBUG
    # fill any missing columns with blanks
    while when_output.count(',') < 37:
        when_output = when_output + ','
        time_output = time_output + ','
    # if there are too many blank lines, don't even bother
    if when_output.count(",,") > tau:
        if DEBUG:
            print "Too much data missing.. skip!"
        return
    if DEBUG:
        print when_output, '\n'
        print time_output, '\n\n'
    fout_when.write(when_output[:-1] + "\n")
    fout_time.write(time_output[:-1] + "\n")


# j and k are `to` and `from` indices of stops respectively
# stops[j],stops[k] returns the values:
# 67,67 -> increment -> 67,39 -> increment -> 39,39 -> etc
# and these indicies will repeatedly loop through the stops
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


# resets incrementers to those for the first column: the first and last indices
def resetJK(nstops):
    global DEBUG
    if DEBUG:
        print "resetting incrementors..."
    return [0, nstops]


if __name__ == "__main__":
    main(sys.argv[1:])
