import sys
import datetime
from pyspark import SparkContext
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

if __name__ == "__main__":
	sc = SparkContext()
	rddbike = sc.textFile('BikeResult.txt')

	lines = rddbike.mapPartitions(lambda x: reader(x))

	neighbor_count = lines.filter(lambda x: int(x[1]) >= 0).map(lambda x: (int(x[1]), 1)).reduceByKey(add).sortBy(lambda x: x[0]).map(lambda x: "%d,%d" % (x[0], x[1]))
	neighbor_count.saveAsTextFile("bike_n_count.txt")

	week_count = lines.filter(lambda x: int(x[1]) >= 0).map(lambda x:(week(x[0]), 1)).reduceByKey(add).sortBy(lambda x: x[0]).map(lambda x: "%d,%d" % (x[0], x[1]))
	week_count.saveAsTextFile("bike_week_count.txt")