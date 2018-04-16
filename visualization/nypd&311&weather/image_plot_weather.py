import numpy as np
import matplotlib.pyplot as plt

fn1 = 'week_spd.txt'
fn2 = 'week_visb.txt'
fn3 = 'week_temp.txt'
fn4 = 'week_prcp.txt'
fn5 = 'week_sd.txt'
fn6 = 'week_sdw.txt'


def open_file(fn):
	f = open(fn)
	content = f.readlines()
	f.close()
	return content

def getlist(content):
	#52 weeks in a year
	number = 52
	#array = [0] * number
	array = np.zeros(number, dtype = 'float')
	for i in range (len(content)):
		key, value = content[i].strip('\n').split(',')
		index = int(key) - 1
		val = float(value)
		if index >= 0:
			array[index] = val
	return array


week_spd = getlist(open_file(fn1))
week_visb = getlist(open_file(fn2))
week_visb_adjust = np.divide(week_visb, 1000)
week_temp = getlist(open_file(fn3))
week_prcp = getlist(open_file(fn4))
week_sd = getlist(open_file(fn5))
week_sdw = getlist(open_file(fn6))
week_sdw_adjust = np.divide(week_sdw, 10)


plt.figure()
plt.title('Weather for 2016', fontsize=20, fontweight='bold')
plt.plot(week_spd, "b", label = 'Wind Speed Rate (meters per secont)')
plt.plot(week_visb_adjust, "c", label = 'Visibility (kilometers)')
plt.plot(week_temp, "m", label = 'Temperature (degrees Celsius)')
plt.plot(week_prcp, "r", label = 'Precipitation (millimeters)')
plt.plot(week_sd, "y", label = 'Depth of snow and ice (centimeters)')
plt.plot(week_sdw_adjust, "k", label = 'Snow Precipitation (centimeters)')
plt.xlabel("Week")
plt.legend()
plt.show()
