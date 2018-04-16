from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession

if __name__ == "__main__":
	sc = spark.SparkContext
	lines = sc.textFile("/user/bl2514/nypdResult.txt")
	parts = lines.map(lambda l: l.split(","))
	rddnypd = parts.map(lambda p: Row(nid=p[0], datetime=p[1], borough=p[2], count=int(p[3])))
	dfnypd = spark.createDataFrame(rddnypd)

	dfnypd.createOrReplaceTempView("dfnypd")
	result = spark.sql("select nid, datetime, borough, count from dfnypd where nid not like 'UNKNOWN'")
	result.write.format("com.databricks.spark.csv").option("header", "true").save("NYPD-2016-Cleaned.csv")
	#no head
	result.select("*").write.save("nypdclean.csv", format="csv")