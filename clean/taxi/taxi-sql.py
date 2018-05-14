from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
	spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

	files = ["yellow_tripdata_2016-01.csv", "yellow_tripdata_2016-02.csv", "yellow_tripdata_2016-03.csv", "yellow_tripdata_2016-04.csv", "yellow_tripdata_2016-05.csv", "yellow_tripdata_2016-06.csv"]
	df = reduce(lambda x,y: x.unionAll(y),[spark.read.format('com.databricks.spark.csv').load(f, header="true", inferSchema="true")for f in files])

	df1 = df.withColumnRenamed('pickup_longitude', 'longitude')
	df2 = df1.withColumnRenamed('pickup_latitude', 'latitude')
	dft = df2.withColumnRenamed('tpep_pickup_datetime', 'datetime')
	dft.createOrReplaceTempView("dft")

	#t = spark.sql("select datetime, latitude, longitude from dft where latitude like ''")
	#t = spark.sql("select datetime, latitude, longitude from dft where longitude like ''")
	#t = spark.sql("select datetime, latitude, longitude from dft where datetime like ''")
	
	result = spark.sql("select datetime, latitude, longitude from dft")
	result.write.format("com.databricks.spark.csv").option("header", "false").save("taxi2016.csv")