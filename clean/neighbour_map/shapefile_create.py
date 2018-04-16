import fiona
from shapely.geometry.polygon import Polygon
from shapely.geometry import mapping

def readcsv_neighbor(filename):
    csv_list = [0] * 260

    file = open(filename)
    for line in file:
        index, value = line.strip('\n').split(',')
        nid = int(index)
        count = int(value)
        csv_list[nid] = count
    file.close()

    return csv_list


def getneighbordict():
    filename = 'neighborhood-id-mapping.txt'

    neighborhood_dict = {}

    file = open(filename)
    for line in file:
        line = line.strip().split('\t')
        neighborname = line[0]
        neighborid = int(line[1])
        neighborhood_dict[neighborid] = neighborname
    file.close()

    return neighborhood_dict


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


def createshapefile(polygon_dict, namemap, list1, list2):
    schema = {'geometry': 'Polygon', 'properties': {'id': 'int', 'name': 'str', '311service': 'int', 'NYPDcompl': 'int'}}

    with fiona.open('neighbor_shp.shp', 'w', 'ESRI Shapefile', schema) as c:
        for i in range (len(polygon_dict)):
            poly = polygon_dict[i]
            name = namemap[i]
            value1 = list1[i]
            value2 = list2[i]
            c.write({'geometry': mapping(poly), 'properties': {'id': i, 'name': name, '311service': value1, 'NYPDcompl': value2}})

if __name__ == '__main__':
    namemap = getneighbordict()
    polygon = getpolygondict()
    list311 = readcsv_neighbor('311_neighbor_count.txt')
    listnypd = readcsv_neighbor('nypd_neighbor_count.txt')
    createshapefile(polygon, namemap, list311, listnypd)