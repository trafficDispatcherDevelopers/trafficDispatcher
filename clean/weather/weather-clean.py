from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

if __name__ == "__main__":
	spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

	dfw = spark.read.format("csv").options(header='true', inferschema='true', delimiter=',', ignoreLeadingWhiteSpace = 'true').load("weather-2011-2017.csv")
	#dfwn = dfw.withColumn("Date", dfw["Date"].cast(StringType()))

	dfwn1 = dfw.na.fill({'Spd': 999.9})
	dfwn2 = dfwn1.na.fill({'Visb': 999999})
	dfwn3 = dfwn2.na.fill({'Temp': 999.9})
	dfwn4 = dfwn3.na.fill({'Prcp': 999.9})
	dfwn5 = dfwn4.na.fill({'SD': 9999})
	dfwn6 = dfwn5.na.fill({'SDW': 99999.9})
	dfwn7 = dfwn6.na.fill({'SA': 999})

	#check
	#dfwn7.filter(col('Spd').isNull()).count()
	#dfwn7.filter(col('Visb').isNull()).count()
	#dfwn7.filter(col('Temp').isNull()).count()
	#dfwn7.filter(col('Prcp').isNull()).count()
	#dfwn7.filter(col('SD').isNull()).count()
	#dfwn7.filter(col('SDW').isNull()).count()
	#dfwn7.filter(col('SA').isNull()).count()

	dfwn7.write.format("com.databricks.spark.csv").option("header", "true").save("weather-2011-2017-clean.csv")

	#filter 2016
	dfwn20161 = dfwn7.filter(col('Date') > 20160000)
	dfwn20162 = dfwn20161.filter(col('Date') < 20170000)
	dfwn20162.select("*").write.save("weather-2016.csv", format="csv")

	#filter 2017.06-2017.12
	#dfwn = dfw.filter(col('Date') > 20170600)
	#dfwn1 = dfwn.filter(col('Date') < 20171300)

	#dfwn.createOrReplaceTempView("dfwn")
	#result = spark.sql("select * from dfwn where Date like '2016%'")
	#result.write.format("com.databricks.spark.csv").option("header", "true").save("weather-2016.csv")