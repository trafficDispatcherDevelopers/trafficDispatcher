import matplotlib.pyplot as plt
import numpy as np
import math
import datetime

plt.style.use('ggplot')

file = open('neighbour_borough_date_time.txt', 'rb')

Y1 = [0] * 52
Y2 = [0] * 52
Y3 = [0] * 52
X = range(52)

for line in file:
    try:
        neigh, borough, date, time = line.strip('\n').split(', ')
        month, day, year = map(int, date.split('/'))
    except:
        continue
    #D = (datetime.date(year, month, day)-datetime.date(2016, 12, 31)).days
    if year == 2017:
        wy, ww, wd = datetime.date(year, month, day).isocalendar() # w=23-52
        if wd < 6:
        	Y1[ww-1] += 0.2
        else:
            Y2[ww-1] += 0.5


plt.plot(X, Y1, label = 'weekday')
plt.plot(X, Y2, label = 'weekend')
plt.xlim([23-1,52-1])
plt.legend(loc='best')
plt.xlabel("week (06/01/17-12/30/17)")
plt.ylabel("collision")
plt.show()
