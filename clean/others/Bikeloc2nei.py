#!/usr/bin/env python
import sys
import csv
import re
from shapely.geometry.polygon import Polygon
from shapely.geometry import Point

def getpolygondict():
    filename = 'neighborhood.txt'

    k = 0
    polygon_dict = {}
    file = open(filename)
    for line in file:
        line = line.strip('\n').split(' ')
        if len(line) == 1:
            if k == 0:
                nid = int(line[0])
                polygon_dict[nid] = []
            k = (k + 1) % 3
        if len(line) == 2:
            x = float(line[0])
            y = float(line[1])
            point = (x, y)
            polygon_dict[nid].append(point)
    file.close()

    for key in polygon_dict:
        polygon_dict[key] = Polygon(polygon_dict[key])

    return polygon_dict

def getpoint(x, y):
    return Point(x, y)

def location2neighbor(point, polygon_dict):
    for key in polygon_dict:
        if polygon_dict[key].contains(point):
            return key
    return -1

def datetimeformat(string):
	r = re.compile('.*-.*-.* .*:.*:.*')
	if r.match(string) is None:
		date, time = string.strip().split(' ', 1)
		month, day, year = date.split('/')
		timestamp = year + '-' + month + '-' + day + ' ' + time
		return timestamp
	else:
		return string

if __name__ == '__main__':
    polygon_dict = getpolygondict()
    
    file = open("BikeResult.txt", 'w')

    for entry in csv.reader(sys.stdin):
        dt = entry[0]
        longitude = float(entry[2])
        latidude = float(entry[1])
        point = getpoint(longitude, latidude)
        neiborid = location2neighbor(point, polygon_dict)
        datetime = datetimeformat(dt)

        result = '{0:s},{1:d}\n'.format(datetime, neiborid)
        file.write(result)

    file.close()
