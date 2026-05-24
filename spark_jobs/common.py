import os

from pyspark.sql import SparkSession


POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/lab")
POSTGRES_USER = os.getenv("POSTGRES_USER", "lab")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lab")

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lab")

POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_PROPS = {
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def create_spark(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_postgres(spark, table):
    return spark.read.jdbc(POSTGRES_URL, table, properties=POSTGRES_PROPS)


def write_postgres(df, table, mode="overwrite"):
    (
        df.write.mode(mode)
        .option("truncate", "true")
        .jdbc(POSTGRES_URL, table, properties=POSTGRES_PROPS)
    )


def write_clickhouse(df, table):
    (
        df.write.mode("overwrite")
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .option("batchsize", "10000")
        .jdbc(CLICKHOUSE_URL, table, properties=CLICKHOUSE_PROPS)
    )
