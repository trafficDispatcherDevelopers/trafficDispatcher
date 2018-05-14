"""
    Language: Python 2.7
    Author: Aining Wang
"""

import matplotlib.pyplot as plt
import numpy as np
import math
import datetime

plt.style.use('ggplot')

file = open('2017_hour_prcp.txt', 'rb')

Y = [0] * (365 - 151)
X = [i+152 for i in range(365 - 151)]

for line in file:
    key, prcp = line.strip('\n').split(',')
    date, time = key.split(' ')
    year, month, day = int(date[:4]), int(date[4:6]), int(date[6:8])
    #print (datetime.date(year, month, day)-datetime.date(2016, 12, 31)).days
    # 152-365
    D = (datetime.date(year, month, day)-datetime.date(2016, 12, 31)).days
    Y[D-152] += float(prcp)

plt.plot(X, Y)
plt.xlim([152, 356])
plt.legend(loc='best')
plt.xlabel("day of 2017 (06/01/17-12/30/17)")
plt.ylabel("precipitation (mm)")
plt.show()

file.close()
