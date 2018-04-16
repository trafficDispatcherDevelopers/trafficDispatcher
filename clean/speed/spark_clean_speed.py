import sys
from csv import reader
from pyspark import SparkContext
from pyspark.sql import Row
import datetime
sc = spark.sparkContext


def get_time_key(time):
    h = int(time[-11:-9])
    m = int(time[-8:-6])
    if time[-2:] == 'PM' and h != 12: key = 12 * 3
    else: key = 0
    key += h * 3 + int(m / 20)
    key = key % 72
    return key


value = sc.textFile("/user/aw3349/neighbour_datetime_speed_borough.txt", 1)
value = value.map(lambda x: x.split(', ')) \
    .filter(lambda x: len(x)>1) \
    .filter(lambda x: float(x[2]) > 0.0)
value1 = value.map(lambda x: dict({ 'neighbour':x[0], 'speed':x[2], 'location':x[3], 'month':(201700+int(x[1][:2])), 'day':int(x[1][3:5]), 'key':get_time_key(x[1]), 'time':x[1][-11:] }))\
    .filter(lambda x: x['month'] == 201712)


result = value1.map(lambda x: [x['neighbour'], x['day'], x['speed'], x['location'], x['key'], x['time']])
output = result.map(lambda x:"\t".join(map(str, x)))
output.saveAsTextFile("201712_neighbour_day_speed_location_key_time.txt")




speedRDD = value1.map(lambda x: Row(neighbour = x['neighbour'], day=x['day'], speed=x['speed'], location = x['location'], key = x['key'], time = x['time']))
speed = spark.createDataFrame(speedRDD)
speed.show()

sailors.select("*").write.save("sailorscsv.csv", format="csv")
