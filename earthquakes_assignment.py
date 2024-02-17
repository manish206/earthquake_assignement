from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col,concat, lit,to_timestamp
from pyspark.sql.types import StringType, FloatType, TimestampType
from math import radians, cos, sin, asin, sqrt
import folium
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder\
    .appName("earthquakes-assignment")\
    .master("local[*]")\
    .getOrCreate()


#Reading input csv file from specified path
df=spark.read.option("header","true")\
       .csv("C://Users//Manish.Gupta//Desktop//assignment//database.csv")

#selecting required columns and casting them
df1 = df.select(col("Date").cast(StringType()),
                col("Time").cast(StringType()),
                col("Latitude").cast(FloatType()),
                col("Longitude").cast(FloatType()),
                col("Type").cast(StringType()),
                col("Depth").cast(FloatType()),
                col("Magnitude").cast(FloatType())
                )
#creating new column TimeStamp by combinig Date and Time
df1=df1.withColumn("TimeStamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), 'MM/dd/yyyy HH:mm:ss'))\

#Filtering dataset with Magnitude greater than 5
df1=df1.where(col("Magnitude") > 5)

#calculating average depth and magnitude for each earthquake type
df1.groupby(col("Type")).agg(functions.avg("Depth").alias("avg_depth"), functions.avg("Magnitude").alias("avg_magnitude")).show()

df1.printSchema()
#UDF function to categorize the earthquakes into Lebvels
@functions.udf(returnType="string")
def measure_magnitude(mag):
    if(mag< 6):
        return "Low"
    elif(mag>=6 and mag<=7):
        return "Moderate"
    elif(mag > 7):
        return "High"

#UDF function to find the distance between two co-ordinates ( source reference location co-ordinate(10, 10))
@functions.udf(returnType="float")
def distance(lat2, lon2):
    lat1 = 10
    lon1 = 10
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2

    c = 2 * asin(sqrt(a))
    r = 6371
    return (c * r)


# measure_magnitude_udf=functions.udf(lambda x: measure_magnitude(x), StringType())

#creating Level and distance column by calling UDF function
df1=df1.withColumn("Level", measure_magnitude(col("Magnitude")))\
    .withColumn("distance" , distance(col("Latitude"), col("Longitude")))

df1.printSchema()
df1.show()

#writing output dataset
df1.write.option("header","true").mode('overwrite').csv("C://Users//Manish.Gupta//Desktop//assignment//out")

#collecting Latitude and Longitude coolumn to shown then on geospatial map
records=df1.select("Latitude","Longitude").collect()

#creating folium map
m=folium.Map()

for record in records:
    folium.Marker(location=(record["Latitude"], record["Longitude"])).add_to(m)

#saving resultant folium map
m.save("C://Users//Manish.Gupta//Desktop//assignment//out//earth_quake_folium.html")


