import sys
from pyspark import SparkContext
from csv import reader
from operator import add
#for day
from datetime import datetime
#for week
import datetime

def hour(datetime):
	#round to hour
	return datetime[:-5] + '00:00'

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

def day(datetimestr):
	date, time = datetimestr.split(' ')
	day  = datetime.strptime(date,"%Y-%m-%d").timetuple().tm_yday
	return day

def month(datetimestr):
	date, time = datetimestr.split(' ')
	yearstr, monthstr, daystr = date.split('-')
	month = int(monthstr)
	return month


if __name__ == "__main__":
	sc = SparkContext()
	rddnypd = sc.textFile('nypdclean.csv')
	#rdd311 = sc.textFile(sys.argv[1])
	lines = rddnypd.mapPartitions(lambda x: reader(x))

	datetime_count = lines.map(lambda x: (hour(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%s,%d" % (x[0], x[1]))
	datetime_count.saveAsTextFile("nypd_datetime_count.txt")

	week_count = lines.map(lambda x:(week(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%d" % (x[0], x[1]))
	week_count.saveAsTextFile("nypd_week_count.txt")


	day_count = lines.map(lambda x:(day(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%d,%d" % (x[0], x[1]))
	day_count.saveAsTextFile("nypd_day_count.txt")


	month_count = lines.map(lambda x:(month(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%d,%d" % (x[0], x[1]))
	month_count.saveAsTextFile("nypd_month_count.txt")


	neighbor_count = lines.map(lambda x: (x[0], int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%s,%d" % (x[0], x[1]))
	neighbor_count.saveAsTextFile("nypd_n_count.txt")


	sc.stop()