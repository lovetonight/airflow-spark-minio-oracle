from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_unixtime, col, to_timestamp, explode, lit
from pyspark.sql.functions import col, when
import sys


# Param
url = sys.argv[1]
table = sys.argv[2]
user = sys.argv[3]
password = sys.argv[4]
bucket = sys.argv[5]
dest_path = sys.argv[6]


spark = (
    SparkSession
    .builder
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

sc = spark.sparkContext
# Set the MinIO access key, secret key, endpoint, and other configurations
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "airflow2")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "airflow2")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://bucket:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl.disable.cache", "true")
sc._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print(f"s3a://{bucket}/{dest_path}")

json_df = spark.read.json(f"s3a://{bucket}/{dest_path}", multiLine=True)

# Dataframe
explode_df = (
    json_df
    .withColumn("weathers", explode(col("weather")))
    .drop("weather")
)

flatten_df = (
    explode_df
    .withColumn("weatherID", col("weathers.id"))
    .withColumn("weatherMain", col("weathers.main"))
    .withColumn("weatherDescription", col("weathers.description"))
    .withColumn("weatherIcon", col("weathers.icon"))
    .drop('weathers')
    .withColumn("could", col("clouds.all"))
    .drop('clouds')
    .withColumn("temp", col("main.temp"))
    .withColumn("feels_like", col("main.feels_like"))
    .withColumn("temp_min", col("main.temp_min"))
    .withColumn("temp_max", col("main.temp_max"))
    .withColumn("pressure", col("main.pressure"))
    .withColumn("sea_level", col("main.sea_level"))
    .withColumn("grnd_level", col("main.grnd_level"))
    .withColumn("humidity", col("main.humidity"))
    .withColumn("temp_kf", col("main.temp_kf"))
    .drop('main')
    .withColumn("windSpeed", col("wind.speed"))
    .withColumn("windDeg", col("wind.deg"))
    .withColumn("windGust", col("wind.gust"))
    .drop('wind')
    .withColumn("sysPod", col("sys.pod"))
    .drop('sys')
    # .withColumn("rain3h",when(col('rain').isNotNull(), col('rain.3h')).otherwise(None))
    .drop('rain')
    # .withColumn("cityID",lit(cityID))
)

flatten_df.show()


(
    flatten_df
    .write
    .format('jdbc')
    .options(
        url=url,
        dbtable=table,
        user=user,
        password=password,
        driver="oracle.jdbc.driver.OracleDriver"
    )
    .mode('append')
    .save()
)

