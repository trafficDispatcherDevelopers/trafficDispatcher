import datetime
from csv import reader
from operator import add

def week(datetimestr):
	#get weeknumber of the yaer
	date, time = datetimestr.split(' ')
	yearstr, monthstr, daystr = date.split('-')
	year = int(yearstr)
	month = int(monthstr)
	day = int(daystr)
	isoyear, isoweeknum, isoweekdaynum = datetime.date(year, month, day).isocalendar()
	if isoyear == year:
		return isoweeknum
	else:
		return 0

#311 Service Requests 2016
rdd311 = sc.textFile('311clean.csv')
lines = rdd311.mapPartitions(lambda x: reader(x))
lines.count()
1722164 

#Bronx:
lines.filter(lambda x: x[2] == 'BRONX').count()
308509

bronx_wc = lines.filter(lambda x: x[2] == 'BRONX').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
bronx_wc.saveAsTextFile("311_bronx_wc.txt")

#Brooklyn:
lines.filter(lambda x: x[2] == 'BROOKLYN').count()
524609

brooklyn_wc = lines.filter(lambda x: x[2] == 'BROOKLYN').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
brooklyn_wc.saveAsTextFile("311_brooklyn_wc.txt")

#MANHATTAN
lines.filter(lambda x: x[2] == 'MANHATTAN').count()
391325

manhattan_wc = lines.filter(lambda x: x[2] == 'MANHATTAN').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
manhattan_wc.saveAsTextFile("311_manhattan_wc.txt")

#QUEENS
lines.filter(lambda x: x[2] == 'QUEENS').count()
410081

queen_wc = lines.filter(lambda x: x[2] == 'QUEENS').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
queen_wc.saveAsTextFile("311_queen_wc.txt")

#STATEN ISLAND
lines.filter(lambda x: x[2] == 'STATEN ISLAND').count()
87640

staten_wc = lines.filter(lambda x: x[2] == 'STATEN ISLAND').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
staten_wc.saveAsTextFile("311_staten_wc.txt")



#NYPD Complaint 2016
rddnypd = sc.textFile('nypdclean.csv')
lines = rddnypd.mapPartitions(lambda x: reader(x))
lines.count()
422655 

#Bronx:
lines.filter(lambda x: x[2] == 'BRONX').count()
95516

bronx_wc = lines.filter(lambda x: x[2] == 'BRONX').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
bronx_wc.saveAsTextFile("nypd_bronx_wc.txt")

#Brooklyn:
lines.filter(lambda x: x[2] == 'BROOKLYN').count()
123031

brooklyn_wc = lines.filter(lambda x: x[2] == 'BROOKLYN').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
brooklyn_wc.saveAsTextFile("nypd_brooklyn_wc.txt")

#MANHATTAN
lines.filter(lambda x: x[2] == 'MANHATTAN').count()
99776

manhattan_wc = lines.filter(lambda x: x[2] == 'MANHATTAN').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
manhattan_wc.saveAsTextFile("nypd_manhattan_wc.txt")

#QUEENS
lines.filter(lambda x: x[2] == 'QUEENS').count()
84058

queen_wc = lines.filter(lambda x: x[2] == 'QUEENS').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
queen_wc.saveAsTextFile("nypd_queen_wc.txt")

#STATEN ISLAND
lines.filter(lambda x: x[2] == 'STATEN ISLAND').count()
20274

staten_wc = lines.filter(lambda x: x[2] == 'STATEN ISLAND').map(lambda x:(week(x[1]), int(x[3]))).reduceByKey(add).filter(lambda x: x[0] > 0).map(lambda x: "%d,%d" % (x[0], x[1]))
staten_wc.saveAsTextFile("nypd_staten_wc.txt")
