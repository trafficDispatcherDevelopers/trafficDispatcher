import numpy as np
import matplotlib.pyplot as plt

s311_total = 1722164
s311_bronx_wc = 308509
s311_brooklyn_wc = 524609
s311_manhattan_wc = 391325
s311_queen_wc = 410081
s311_staten_wc = 87640

nypd_total = 422655
nypd_bronx_wc = 95516
nypd_brooklyn_wc = 123031
nypd_manhattan_wc = 99776
nypd_queen_wc = 84058
nypd_staten_wc = 20274

def autolabel(rects):
    for rect in rects:
        h = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1*h, '%d'%int(h), ha='center', va='bottom')

n = 5
ind = np.arange(n)
width = 0.27

fig = plt.figure()
plt.title('Amount By Boroughs 2016', fontsize=20, fontweight='bold')
ax = fig.add_subplot(111)

s311val = [s311_bronx_wc, s311_brooklyn_wc, s311_manhattan_wc, s311_queen_wc, s311_staten_wc]
rects1 = ax.bar(ind, s311val, width, color = 'r')
nypdval = [nypd_bronx_wc, nypd_brooklyn_wc, nypd_manhattan_wc, nypd_queen_wc, nypd_staten_wc]
rects2 = ax.bar(ind + width, nypdval, width, color = 'c')

ax.set_xticks(ind + width)
ax.set_xticklabels(('Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island'))
ax.legend((rects1[0], rects2[0]), ('311 Service Requests', 'NYPD Complaint'))

autolabel(rects1)
autolabel(rects2)

plt.show()