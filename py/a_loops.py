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

# construct the MySQL query
q = ""
old = aStops[0]
for i in aStops:
    q = q + """
    (`from`=%(fr)d and `to`=%(to)d) or """ % {"fr": old, "to": i}
    old = i
    q = q + """
    (`from`=%(fr)d and `to`=%(to)d) or """ % {"fr": old, "to": i}
query = """
SELECT * FROM intervaldata where
route_id=331 and (""" + q[:-4] + """)
order by bus_id asc, `when` asc;
"""
query = query.replace('67', '35', 1)
# print query

# execute the query for selecting valid a-route intervals
try:
    cursor.execute(query)
except:
    print "error"
    sys.exit()

fout = open("A_Route.csv", 'w')

# output = the header line of my output csv, 38 columns
output = "day,busid,67"
for n in aStops[1:]:
    output = output + ",to" + str(n) + "," + str(n)
output = output + ",to67"
fout.write(output)

# row format:
# (id, 8L, 10L, 42L, 641L, 331L, datetime.datetime(2014, 8, 19, 11, 20, 25))

output = ""
inLoop = False
for i in range(1, cursor.rowcount):
    line = cursor.fetchone()
    if line[1] == 67 and line[2] == 67:
        inLoop = True
        output = str(line[6].year) + "-" + str(line[6].month)\
            + "-" + str(line[6].day) + "," + str(line[4]) + ","
    if inLoop:
        output = output + str(line[6].hour) + ":" + str(line[6].minute)\
            + ":" + str(line[6].second) + ","
    if line[1] == 35 and line[2] == 67:
        fout.write(output[:-1] + "\n")
        inLoop = False

fout.close
