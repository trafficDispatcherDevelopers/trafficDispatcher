from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

    dfp = spark.read.format("csv").options(header='true', inferschema='true').load("NYPD_Complaint_Data_Historic.csv")
    dfp.createOrReplaceTempView("dfp")

    boroughlist = spark.createDataFrame(["BRONX", "QUEENS", "BROOKLYN", "MANHATTAN", "STATEN ISLAND"], StringType())
    boroughlist.createOrReplaceTempView("boroughlist")

    result = spark.sql("select CMPLNT_FR_DT as Date, CMPLNT_FR_TM as Time, BORO_NM as Borough, Latitude, Longitude from dfp where CMPLNT_FR_DT like '%2016%' and BORO_NM in (select value from boroughlist) and Lat_Lon not like ''")
    result.write.format("com.databricks.spark.csv").option("header", "false").save("NYPD-2016.csv")