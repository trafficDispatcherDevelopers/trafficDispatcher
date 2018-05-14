import datetime

file1 = open('neighbour_borough_date_time.txt', 'r')
file2 = open('2016week_collision', 'w')
D = dict()
for i in range(52):
    D[i+1] = 0


for line in file1:
    try:
        neigh, borough, date, time = line.strip('\n').split(', ')
        month, day, year = map(int, date.split('/'))
    except:
        continue
    wy, ww, wd = datetime.date(year, month, day).isocalendar()
    if wy == 2016:
        D[ww] += 1

for i in range(52):
    file2.write(str(i+1) + ', ' + str(D[i+1]) + '\n')



file1.close()
file2.close()
