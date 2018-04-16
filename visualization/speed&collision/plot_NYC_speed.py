"""
    Language: Python 2.7
    Author: Aining Wang
"""

import matplotlib.pyplot as plt
import numpy as np
import math

plt.style.use('ggplot')

file = open('201712_borough_speedseries.txt', 'rb')
data_dict = dict()
T = np.arange(0., 7., 0.125)

for line in file:
    borough, speed = line.strip('\n').split('\t')
    speed = map(float, speed.split(', '))
    SPEED = list()
    for i in range(720/3):
        SPEED.append(max(speed[3*i:3*i+3]))
    plt.plot(T, SPEED[8*3 : 8*10 ], label = borough)
    plt.ylim([0, 60])

plt.legend(loc='best')
plt.xlabel("day (12/03/17-12/10/17)")
plt.ylabel("speed (mile/h)")

plt.show()



file.close()
