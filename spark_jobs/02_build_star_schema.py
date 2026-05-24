from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    dayofmonth,
    month,
    quarter,
    sha2,
    year,
)

from common import create_spark, read_postgres, write_postgres


def key(*columns):
    return sha2(concat_ws("||", *[col(column).cast("string") for column in columns]), 256)


def main():
    spark = create_spark("lab2-build-star-schema")
    df = read_postgres(spark, "mock_data")

    base = (
        df.withColumn(
            "customer_key",
            key(
                "customer_email",
                "customer_first_name",
                "customer_last_name",
                "customer_country",
                "customer_pet_name",
            ),
        )
        .withColumn(
            "seller_key",
            key("seller_email", "seller_first_name", "seller_last_name", "seller_country"),
        )
        .withColumn(
            "product_key",
            key(
                "product_name",
                "product_category",
                "product_brand",
                "product_material",
                "product_color",
                "product_size",
                "supplier_email",
            ),
        )
        .withColumn(
            "store_key",
            key("store_name", "store_email", "store_phone", "store_city", "store_country"),
        )
        .withColumn(
            "supplier_key",
            key("supplier_name", "supplier_email", "supplier_phone", "supplier_country"),
        )
        .withColumn("date_key", date_format(col("sale_date"), "yyyyMMdd").cast("int"))
    )

    dim_customer = base.select(
        "customer_key",
        col("sale_customer_id").alias("source_customer_id"),
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
    ).dropDuplicates(["customer_key"])

    dim_seller = base.select(
        "seller_key",
        col("sale_seller_id").alias("source_seller_id"),
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
    ).dropDuplicates(["seller_key"])

    dim_supplier = base.select(
        "supplier_key",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ).dropDuplicates(["supplier_key"])

    dim_product = base.select(
        "product_key",
        "supplier_key",
        col("sale_product_id").alias("source_product_id"),
        "product_name",
        "product_category",
        "pet_category",
        "product_price",
        "product_quantity",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
    ).dropDuplicates(["product_key"])

    dim_store = base.select(
        "store_key",
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ).dropDuplicates(["store_key"])

    dim_date = base.select(
        "date_key",
        col("sale_date").alias("full_date"),
        year("sale_date").alias("year"),
        quarter("sale_date").alias("quarter"),
        month("sale_date").alias("month"),
        dayofmonth("sale_date").alias("day"),
    ).dropDuplicates(["date_key"])

    fact_sales = base.select(
        col("id").cast("long").alias("source_sale_id"),
        "customer_key",
        "seller_key",
        "product_key",
        "store_key",
        "supplier_key",
        "date_key",
        "sale_quantity",
        "sale_total_price",
    )

    tables = {
        "dim_customer": dim_customer,
        "dim_seller": dim_seller,
        "dim_supplier": dim_supplier,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
    }

    for table, table_df in tables.items():
        write_postgres(table_df, table)
        print(f"Wrote PostgreSQL {table}: {table_df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
