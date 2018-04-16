from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

"""
when len(line.split(' ')) == 1:
    k = 0: <region-id>
    k = 1: <number-of-polygons> (this number is always 1)
    k = 2: <number-of-data-points>
"""
def read_input_file(input_file):
    k = 0
    polygon_dict = dict()
    file = open(input_file)
    for line in file:
        line = line.strip('\n').split(' ')
        if len(line) == 1:
            if k == 0:
                polygon_dict[line[0]] = list()
                id = line[0]
            k = (k + 1) % 3
        if len(line) == 2:
            polygon_dict[id].append((float(line[0]), float(line[1])))
    file.close()
    return polygon_dict


def get_point(x, y):
    return Point(x, y)


def get_polygon_dict(input_file):
    polygon_dict = read_input_file(input_file)
    for key in polygon_dict:
        polygon_dict[key] = Polygon(polygon_dict[key])
    return polygon_dict

def map_neighbour(point, polygon_dict):
    for key in polygon_dict:
        if polygon_dict[key].contains(point):
            return key
    return 'UNKNOWN'




"""
d = get_polygon_dict('neighborhood.txt')
p = get_point(-73.99423, 40.7719)
print(map_neighbour(p, d))
"""
