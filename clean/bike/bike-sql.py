from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
	spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
	files = ["201601-citibike-tripdata.csv", "201602-citibike-tripdata.csv", "201603-citibike-tripdata.csv", "201604-citibike-tripdata.csv", "201605-citibike-tripdata.csv", "201606-citibike-tripdata.csv", "201607-citibike-tripdata.csv", "201608-citibike-tripdata.csv", "201609-citibike-tripdata.csv", "201610-citibike-tripdata.csv", "201611-citibike-tripdata.csv", "201612-citibike-tripdata.csv"]
	df = reduce(lambda x,y: x.unionAll(y),[spark.read.format('com.databricks.spark.csv').load(f, header="true", inferSchema="true")for f in files])

	df1 = df.withColumnRenamed('start station latitude', 'latitude')
	dfb = df1.withColumnRenamed('start station longitude', 'longitude')
	dfb.createOrReplaceTempView("dfb")

	#t = spark.sql("select starttime, latitude, longitude from dfb where latitude like ''")
	#t = spark.sql("select starttime, latitude, longitude from dfb where longitude like ''")
	#t = spark.sql("select starttime, latitude, longitude from dfb where starttime like ''")

	result = spark.sql("select starttime, latitude, longitude from dfb")
	result.write.format("com.databricks.spark.csv").option("header", "false").save("bike2016.csv")