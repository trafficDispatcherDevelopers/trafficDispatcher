"""
    Language: Python 2.7
    Author: Aining Wang
"""

import matplotlib.pyplot as plt
import numpy as np
import math
import datetime

plt.style.use('ggplot')

file = open('2017_hour_temp.txt', 'rb')
maxY = [-50] * (365 - 151)
minY = [50] * (365 - 151)
X = [i+152 for i in range(365 - 151)]

for line in file:

    key, temp = line.strip('\n').split(',')
    date, time = key.split(' ')
    temp = float(temp)
    year, month, day = int(date[:4]), int(date[4:6]), int(date[6:8])
    
    D = (datetime.date(year, month, day) - datetime.date(2016, 12, 31)).days
    maxY[D-152] = max(maxY[D-152], temp)
    minY[D-152] = min(minY[D-152], temp)


plt.plot(X, maxY, label = "max temp")
plt.plot(X, minY, label = "min temp")
plt.xlim([152, 356])
plt.ylim([-15,35])
plt.legend(loc='best')
plt.xlabel("day of 2017 (06/01/17-12/30/17)")
plt.ylabel("tempreture (c)")

plt.show()



file.close()
