import logging
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, LongType,
)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "weather"
KAFKA_TOPIC = "weather_data"
KAFKA_BOOTSTRAP = "broker:29092"


def create_spark_connection():
    spark_conn = None
    try:
        spark_conn = (
            SparkSession.builder
            .appName("SparkWeatherStreaming")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .getOrCreate()
        )
        spark_conn.sparkContext.setLogLevel("ERROR")
        print("Spark session created.")
    except Exception as e:
        logging.error(f"Couldn't create Spark session: {e}")
    return spark_conn

import base64

def create_clickhouse_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
        record_id           UUID,
        city                String,
        country             String,
        latitude            Float64,
        longitude           Float64,
        weather_main        String,
        weather_description String,
        temp                Float64,
        feels_like          Float64,
        temp_min            Float64,
        temp_max            Float64,
        pressure            Int32,
        humidity            Int32,
        wind_speed          Float64,
        wind_deg            Int32,
        clouds              Int32,
        dt                  Int64
    ) ENGINE = MergeTree()
    ORDER BY (city, dt);
    """
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?query=" + urllib.parse.quote(ddl)
    auth = base64.b64encode(b"default:clickhouse_pass").decode()
    req = urllib.request.Request(
        url,
        method="POST",
        headers={
            "User-Agent": "spark-weather-pipeline/1.0",
            "Authorization": f"Basic {auth}",
        },
    )
    with urllib.request.urlopen(req) as resp:
        print(f"ClickHouse table '{CLICKHOUSE_TABLE}' ready (HTTP {resp.status}).")

def connect_to_kafka(spark_conn):
    try:
        spark_df = (
            spark_conn.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        print("Kafka dataframe created.")
        return spark_df
    except Exception as e:
        print(f"Kafka dataframe could not be created: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("record_id", StringType(), False),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("weather_main", StringType(), True),
        StructField("weather_description", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_deg", IntegerType(), True),
        StructField("clouds", IntegerType(), True),
        StructField("dt", LongType(), True),
    ])
    return (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )


def write_to_clickhouse(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.write
        .format("jdbc")
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", CLICKHOUSE_TABLE)
        .option("user", "default")
        .option("password", "clickhouse_pass")   # <-- was ""
        .mode("append")
        .save()
    )
    print(f"Batch {batch_id} written to ClickHouse.")

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is None:
        raise SystemExit(1)

    create_clickhouse_table()
    spark_df = connect_to_kafka(spark_conn)
    if spark_df is None:
        raise SystemExit(1)

    selection_df = create_selection_df_from_kafka(spark_df)

    query = (
        selection_df.writeStream
        .foreachBatch(write_to_clickhouse)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoint/weather")
        .trigger(processingTime="15 seconds")
        .start()
    )
    query.awaitTermination()