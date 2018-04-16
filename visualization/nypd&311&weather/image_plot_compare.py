import numpy as np
import matplotlib.pyplot as plt

fn0 = '311_month_count.txt'
fn1 = '311_week_count.txt'
fn2 = '311_day_count.txt'
fn3 = 'nypd_month_count.txt'
fn4 = 'nypd_week_count.txt'
fn5 = 'nypd_day_count.txt'
fn6 = '2016week_collision.txt'

def open_file(fn):
	f = open(fn)
	content = f.readlines()
	f.close()
	return content

def get_numpy_array(content):
	number = len(content)
	array = np.arange(number)
	for i in range (number):
		key, value = content[i].strip('\n').split(',')
		key.strip('\n')
		value.strip('\n')
		count = int(value)
		array[i] = count
	return array

def plotmonth(array1, label1, array2, label2):
	plt.figure()
	plt.title('Total Amount for 2016', fontsize=20, fontweight='bold')
	plt.plot(array1, "r", label = label1)
	plt.plot(array2, "b", label = label2)
	plt.xlim([0, 12])
	plt.xlabel("Month")
	plt.ylabel("Total Amount")
	plt.legend()
	plt.show()

def plotweek(array1, label1, array2, label2):
	plt.figure()
	plt.title('Total Amount for 2016', fontsize=20, fontweight='bold')
	plt.plot(array1, "r", label = label1)
	plt.plot(array2, "b", label = label2)
	plt.xlim([0, 52])
	plt.xlabel("Week")
	plt.ylabel("Total Amount")
	plt.legend()
	plt.show()

def plotday(array1, label1, array2, label2):
	plt.figure()
	plt.title('Total Amount for 2016', fontsize=20, fontweight='bold')
	plt.plot(array1, "r", label = label1)
	plt.plot(array2, "b", label = label2)
	plt.xlim([0, 366])
	plt.xlabel("Day")
	plt.ylabel("Total Amount")
	plt.legend()
	plt.show()

def plotweek3(array1, label1, array2, label2, array3, label3):
	plt.figure()
	plt.title('Total Amount for 2016', fontsize=20, fontweight='bold')
	plt.plot(array1, "r", label = label1)
	plt.plot(array2, "b", label = label2)
	plt.plot(array3, "c", label = label3)
	plt.xlim([0, 52])
	plt.xlabel("Week")
	plt.ylabel("Total Amount")
	plt.legend()
	plt.show()




if __name__ == '__main__':
	label311 = "311 Service Requests"
	labelnypd = "NYPD Complaint"
	labelcollision = "Collision"

	month311 = get_numpy_array(open_file(fn0))
	week311 = get_numpy_array(open_file(fn1))
	day311 = get_numpy_array(open_file(fn2))
	monthnypd = get_numpy_array(open_file(fn3))
	weeknypd = get_numpy_array(open_file(fn4))
	daynypd = get_numpy_array(open_file(fn5))
	weekcollison = get_numpy_array(open_file(fn6))

	#plotweek(week311, label311, weeknypd, labelnypd)
	#plotday(day311, label311, daynypd, labelnypd)
	#plotmonth(month311, label311, monthnypd, labelnypd)
	plotweek3(week311, label311, weeknypd, labelnypd, weekcollison, labelcollision)




