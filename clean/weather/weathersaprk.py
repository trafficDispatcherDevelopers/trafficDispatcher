import sys
from pyspark import SparkContext
from csv import reader
from operator import add
import datetime

def week(datetimestr):
	#get weeknumber of the yaer
	date = int(datetimestr)
	year = date // 10000
	month = (date % 10000) // 100
	day = date % 100
	isoyear, isoweeknum, isoweekdaynum = datetime.date(year, month, day).isocalendar()
	if isoyear == year:
		return isoweeknum
	else:
		return 0


def hour(datestr, timestr):
	time = int(timestr)
	if time < 100:
		return datestr + ' 00:00:00'
	elif time >= 100 and time < 200:
		return datestr + ' 01:00:00'
	elif time >= 200 and time < 300:
		return datestr + ' 02:00:00'
	elif time >= 300 and time < 400:
		return datestr + ' 03:00:00'
	elif time >= 400 and time < 500:
		return datestr + ' 04:00:00'
	elif time >= 500 and time < 600:
		return datestr + ' 05:00:00'
	elif time >= 600 and time < 700:
		return datestr + ' 06:00:00'
	elif time >= 700 and time < 800:
		return datestr + ' 07:00:00'
	elif time >= 800 and time < 900:
		return datestr + ' 08:00:00'
	elif time >= 900 and time < 1000:
		return datestr + ' 09:00:00'
	elif time >= 1000 and time < 1100:
		return datestr + ' 10:00:00'
	elif time >= 1100 and time < 1200:
		return datestr + ' 11:00:00'
	elif time >= 1200 and time < 1300:
		return datestr + ' 12:00:00'
	elif time >= 1300 and time < 1400:
		return datestr + ' 13:00:00'
	elif time >= 1400 and time < 1500:
		return datestr + ' 14:00:00'
	elif time >= 1500 and time < 1600:
		return datestr + ' 15:00:00'
	elif time >= 1600 and time < 1700:
		return datestr + ' 16:00:00'
	elif time >= 1700 and time < 1800:
		return datestr + ' 17:00:00'
	elif time >= 1800 and time < 1900:
		return datestr + ' 18:00:00'
	elif time >= 1900 and time < 2000:
		return datestr + ' 19:00:00'
	elif time >= 2000 and time < 2100:
		return datestr + ' 20:00:00'
	elif time >= 2100 and time < 2200:
		return datestr + ' 21:00:00'
	elif time >= 2200 and time < 2300:
		return datestr + ' 22:00:00'
	else:
		return datestr + ' 23:00:00'


if __name__ == '__main__':
	sc = SparkContext()
	#rddw = sc.textFile('weather-2011-2017.csv')
	rddw2016 = sc.textFile('weather-2016.csv')
	#rddw2017 = sc.textFile('weather-201706-201712.csv')
	lines = rddw2016.mapPartitions(lambda x: reader(x))

	#rddw2017 = sc.textFile('weather-201706-201712.csv')

	hour_spd = lines.filter(lambda x : float(x[2]) != 999.9).map(lambda x: (hour(x[0], x[1]), (float(x[2]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: "%s,%0.1f" % (x[0], x[1][0] / x[1][1]))
	hour_spd.saveAsTextFile("hour_spd.txt")

	week_spd = lines.filter(lambda x : float(x[2]) != 999.9).map(lambda x: (week(x[0]), (float(x[2]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%0.1f" % (x[0], x[1][0] / x[1][1]))
	week_spd.saveAsTextFile("week_spd.txt")


	hour_visb = lines.filter(lambda x : int(x[3]) != 999999).map(lambda x: (hour(x[0], x[1]), (int(x[3]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: "%s,%0.1f" % (x[0], x[1][0] / x[1][1]))
	hour_visb.saveAsTextFile("hour_visb.txt")

	week_visb = lines.filter(lambda x : int(x[3]) != 999999).map(lambda x: (week(x[0]), (int(x[3]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%0.1f" % (x[0], x[1][0] / x[1][1]))
	week_visb.saveAsTextFile("week_visb.txt")


	hour_temp = lines.filter(lambda x : float(x[4]) != 999.9).map(lambda x: (hour(x[0], x[1]), (float(x[4]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: "%s,%0.1f" % (x[0], x[1][0] / x[1][1]))
	hour_temp.saveAsTextFile("hour_temp.txt")

	week_temp = lines.filter(lambda x : float(x[4]) != 999.9).map(lambda x: (week(x[0]), (float(x[4]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%0.1f" % (x[0], x[1][0] / x[1][1]))
	week_temp.saveAsTextFile("week_temp.txt")


	hour_prcp = lines.filter(lambda x : float(x[5]) != 999.9).map(lambda x: (hour(x[0], x[1]), (float(x[5]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: "%s,%0.1f" % (x[0], x[1][0] / x[1][1]))
	hour_prcp.saveAsTextFile("hour_prcp.txt")

	week_prcp = lines.filter(lambda x : float(x[5]) != 999.9).map(lambda x: (week(x[0]), (float(x[5]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%0.1f" % (x[0], x[1][0] / x[1][1]))
	week_prcp.saveAsTextFile("week_prcp.txt")


	hour_sd = lines.filter(lambda x : int(x[6]) != 9999).map(lambda x: (hour(x[0], x[1]), (int(x[6]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: "%s,%0.1f" % (x[0], x[1][0] / x[1][1]))
	hour_sd.saveAsTextFile("hour_sd.txt")

	week_sd = lines.filter(lambda x : int(x[6]) != 9999).map(lambda x: (week(x[0]), (int(x[6]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%0.1f" % (x[0], x[1][0] / x[1][1]))
	week_sd.saveAsTextFile("week_sd.txt")


	hour_sdw = lines.filter(lambda x : float(x[7]) != 99999.9).map(lambda x: (hour(x[0], x[1]), (float(x[7]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: "%s,%0.1f" % (x[0], x[1][0] / x[1][1]))
	hour_sdw.saveAsTextFile("hour_sdw.txt")

	week_sdw = lines.filter(lambda x : float(x[7]) != 99999.9).map(lambda x: (week(x[0]), (float(x[7]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%0.1f" % (x[0], x[1][0] / x[1][1]))
	week_sdw.saveAsTextFile("week_sdw.txt")

	#nothing for 2016/2017
	week_sa = lines.filter(lambda x : int(x[8]) != 999).map(lambda x: (week(x[0]), (int(x[8]), float(1)))) \
							.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).filter(lambda x: x[0] > 0) \
							.map(lambda x: "%d,%0.1f" % (x[0], x[1][0] / x[1][1]))
	week_sa.saveAsTextFile("week_sa.txt")	
