import sys
from pyspark import SparkContext
from csv import reader
from operator import add
from datetime import datetime
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
	rdd311 = sc.textFile('311clean.csv')
	#rdd311 = sc.textFile(sys.argv[1])
	lines = rdd311.mapPartitions(lambda x: reader(x))

	datetime_count = lines.map(lambda x: (hour(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%s,%d" % (x[0], x[1]))
	datetime_count.saveAsTextFile("311_datetime_count.txt")

	week_count = lines.map(lambda x:(week(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%d" % (x[0], x[1]))
	week_count.saveAsTextFile("311_week_count.txt")


	day_count = lines.map(lambda x:(day(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%d,%d" % (x[0], x[1]))
	day_count.saveAsTextFile("311_day_count.txt")


	month_count = lines.map(lambda x:(month(x[1]), int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%d,%d" % (x[0], x[1]))
	month_count.saveAsTextFile("311_month_count.txt")


	neighbor_count = lines.map(lambda x: (x[0], int(x[3]))) \
							.reduceByKey(add) \
							.map(lambda x: "%s,%d" % (x[0], x[1]))
	neighbor_count.saveAsTextFile("311_n_count.txt")


	sc.stop()