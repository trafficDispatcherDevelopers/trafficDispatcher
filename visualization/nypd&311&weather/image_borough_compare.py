import numpy as np
import matplotlib.pyplot as plt

fnb0 = '311_bronx_wc.txt'
fnb1 = '311_brooklyn_wc.txt'
fnb2 = '311_manhattan_wc.txt'
fnb3 = '311_queen_wc.txt'
fnb4 = '311_staten_wc.txt'
fnb5 = 'nypd_bronx_wc.txt'
fnb6 = 'nypd_brooklyn_wc.txt'
fnb7 = 'nypd_manhattan_wc.txt'
fnb8 = 'nypd_queen_wc.txt'
fnb9 = 'nypd_staten_wc.txt'

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
		index = int(key) - 1
		count = int(value)
		array[index] = count
	return array

def plotweekborough(titlestring, array1, label1, array2, label2, array3, label3, array4, label4, array5, label5):
	plt.figure()
	plt.title(titlestring, fontsize=20, fontweight='bold')
	plt.plot(array1, "b", label = label1)
	plt.plot(array2, "c", label = label2)
	plt.plot(array3, "m", label = label3)
	plt.plot(array4, "r", label = label4)
	plt.plot(array5, "y", label = label5)
	plt.xlim([0, 52])
	plt.xlabel("Week")
	plt.ylabel("Total Amount")
	plt.legend()
	plt.show()

if __name__ == '__main__':
	label1 = 'Bronx'
	label2 = 'Brooklyn'
	label3 = 'Manhattan'
	label4 = 'Queens'
	label5 = 'Staten Island'

	title311 = '311 Service Request 2016'
	titlenypd = 'NYPD Complaint 2016'

	bronx311 = get_numpy_array(open_file(fnb0))
	brooklyn311 = get_numpy_array(open_file(fnb1))
	manhattan311 = get_numpy_array(open_file(fnb2))
	queen311 = get_numpy_array(open_file(fnb3))
	staten311 = get_numpy_array(open_file(fnb4))
	bronxnypd = get_numpy_array(open_file(fnb5))
	brooklynnypd = get_numpy_array(open_file(fnb6))
	manhattannypd = get_numpy_array(open_file(fnb7))
	queennypd = get_numpy_array(open_file(fnb8))
	statennypd = get_numpy_array(open_file(fnb9))

	plotweekborough(title311, bronx311, label1, brooklyn311, label2, manhattan311, label3, queen311, label4, staten311, label5)
	plotweekborough(titlenypd, bronxnypd, label1, brooklynnypd, label2, manhattannypd, label3, queennypd, label4, statennypd, label5)