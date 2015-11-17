import sys
import MySQLdb
# from datetime import date
# from datetime import timedelta


# connect to mysql
mysqldb = MySQLdb.connect(host="156.56.179.122",
                          user="b565",
                          passwd="b565Pwd",
                          db="b565")
cursor = mysqldb.cursor()

# these stops and their order were calculated using a_loops.R
aStops = [67, 39, 38, 37, 41, 1, 4, 6, 8, 10, 11, 12, 13, 14, 36, 30, 34, 35]

# USED TO BUILD THE FOLLOWING QUERY
# construct the MySQL query
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
# query = query.replace('67', '35', 1)
# print query

query = """
SELECT * FROM intervaldata where
route_id=331 and (
(`from`=67 and `to`=35) or
(`from`=67 and `to`=11) or
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
order by bus_id asc, `when` asc;
"""

# execute the query for selecting valid a-route intervals
try:
    cursor.execute(query)
except:
    print "error"
    sys.exit()

# output = the header line of my output csv: 38 column names
output = "day,busid"
for n in aStops:
    output = output + ",to" + str(n) + ",from" + str(n)
print output

# write the column names to an output file
fout = open("A_Route.csv", 'w')
fout.write(output)

# row format:
# (id, 8L, 10L, 42L, 641L, 331L, datetime.datetime(2014, 8, 19, 11, 20, 25))

output = ""
inLoop = False
for i in range(1, cursor.rowcount):
    line = cursor.fetchone()
    if line[2] == 67:
        inLoop = True
        # when we start a loop, start by printing the date and busid
        output = str(line[6].year) + "-" + str(line[6].month)\
            + "-" + str(line[6].day) + "," + str(line[4]) + ","
    if inLoop:
        # if we're in a loop, add the timestamp of each stop
        output = output + str(line[6].hour) + ":" + str(line[6].minute)\
            + ":" + str(line[6].second) + ","
    if line[1] == 35 and line[2] == 35:
        fout.write(output[:-1] + "\n")
        inLoop = False

fout.close
