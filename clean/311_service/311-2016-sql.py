from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

    dfc = spark.read.format("csv").options(header='true', inferschema='true', ).load("311_Service_Requests_from_2010_to_Present.csv")
    dfc1 = dfc.withColumnRenamed('Created Date', 'CreatedDate')
    dfc2 = dfc1.withColumnRenamed('Agency Name', 'AgencyName')
    dfcn = dfc2.withColumnRenamed('Complaint Type', 'ComplaintType')
    dfcn.createOrReplaceTempView("dfcn")

    boroughlist = spark.createDataFrame(["BRONX", "QUEENS", "BROOKLYN", "MANHATTAN", "STATEN ISLAND"], StringType())
    boroughlist.createOrReplaceTempView("boroughlist")

    result = spark.sql("select CreatedDate, Borough, Latitude, Longitude from dfcn where CreatedDate like '%2016%' and Borough in (select value from boroughlist) and Location not like ''")
    result.write.format("com.databricks.spark.csv").option("header", "false").save("311-Service-2016.csv")