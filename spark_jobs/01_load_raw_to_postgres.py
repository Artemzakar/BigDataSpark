import os

from pyspark.sql.functions import col, to_date

from common import create_spark, write_postgres


NUMERIC_COLUMNS = {
    "customer_age": "int",
    "product_price": "double",
    "product_quantity": "int",
    "sale_customer_id": "long",
    "sale_seller_id": "long",
    "sale_product_id": "long",
    "sale_quantity": "int",
    "sale_total_price": "double",
    "product_weight": "double",
    "product_rating": "double",
    "product_reviews": "int",
}


def main():
    spark = create_spark("lab2-load-raw-to-postgres")
    data_path = os.getenv("DATA_PATH", "/data/*.csv")

    raw = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(data_path)
    )

    df = raw.toDF(*[name.strip().lower() for name in raw.columns])
    for column_name, column_type in NUMERIC_COLUMNS.items():
        df = df.withColumn(column_name, col(column_name).cast(column_type))

    df = (
        df.withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy"))
        .withColumn("product_release_date", to_date(col("product_release_date"), "M/d/yyyy"))
        .withColumn("product_expiry_date", to_date(col("product_expiry_date"), "M/d/yyyy"))
    )

    write_postgres(df, "mock_data")
    print(f"Loaded rows to PostgreSQL mock_data: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
