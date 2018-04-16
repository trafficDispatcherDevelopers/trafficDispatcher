from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile("/user/bl2514/311Result.txt")
	parts = lines.map(lambda l: l.split(","))
	rdd311 = parts.map(lambda p: Row(nid=p[0], datetime=p[1], borough=p[2], count=int(p[3])))
	service311 = spark.createDataFrame(rdd311)

	spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
	df311.createOrReplaceTempView("df311")
	result = spark.sql("select nid, datetime, borough, count from df311 where nid not like 'UNKNOWN'")
	result.write.format("com.databricks.spark.csv").option("header", "true").save("311-Service-2016-Cleaned.csv")
	#no head
	result.select("*").write.save("311clean.csv", format="csv")