from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    corr,
    count,
    countDistinct,
    lag,
    lit,
    max,
    rank,
    round,
    sum,
)

from common import create_spark, read_postgres, write_clickhouse


def main():
    spark = create_spark("lab2-build-clickhouse-reports")

    fact = read_postgres(spark, "fact_sales")
    products = read_postgres(spark, "dim_product")
    customers = read_postgres(spark, "dim_customer")
    dates = read_postgres(spark, "dim_date")
    stores = read_postgres(spark, "dim_store")
    suppliers = read_postgres(spark, "dim_supplier")

    sales_products = (
        fact.join(products, "product_key")
        .groupBy("product_key", "product_name", "product_category")
        .agg(
            sum("sale_quantity").alias("total_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            count("*").alias("sales_count"),
            round(avg("product_rating"), 2).alias("avg_rating"),
            max("product_reviews").alias("reviews_count"),
        )
        .withColumn("product_sales_rank", rank().over(Window.orderBy(col("total_quantity").desc())))
        .withColumn(
            "category_revenue",
            round(sum("total_revenue").over(Window.partitionBy("product_category")), 2),
        )
    )

    sales_customers = (
        fact.join(customers, "customer_key")
        .groupBy(
            "customer_key",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            round(sum("sale_total_price"), 2).alias("total_purchases"),
            count("*").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_check"),
        )
        .withColumn("customer_revenue_rank", rank().over(Window.orderBy(col("total_purchases").desc())))
        .withColumn(
            "customers_in_country",
            count("*").over(Window.partitionBy("customer_country")),
        )
    )

    sales_time = (
        fact.join(dates, "date_key")
        .groupBy("year", "month")
        .agg(
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            count("*").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_order_amount"),
        )
        .withColumn("previous_month_revenue", lag("total_revenue").over(Window.orderBy("year", "month")))
        .withColumn("previous_month_revenue", coalesce(col("previous_month_revenue"), lit(0.0)))
        .withColumn(
            "revenue_delta",
            round(col("total_revenue") - col("previous_month_revenue"), 2),
        )
    )

    sales_stores = (
        fact.join(stores, "store_key")
        .groupBy("store_key", "store_name", "store_city", "store_country")
        .agg(
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            count("*").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_check"),
        )
        .withColumn("store_revenue_rank", rank().over(Window.orderBy(col("total_revenue").desc())))
        .withColumn(
            "city_country_revenue",
            round(sum("total_revenue").over(Window.partitionBy("store_city", "store_country")), 2),
        )
    )

    sales_suppliers = (
        fact.join(suppliers, "supplier_key")
        .join(products.select("product_key", "product_price"), "product_key")
        .groupBy("supplier_key", "supplier_name", "supplier_city", "supplier_country")
        .agg(
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            countDistinct("product_key").alias("products_count"),
            round(avg("product_price"), 2).alias("avg_product_price"),
        )
        .withColumn("supplier_revenue_rank", rank().over(Window.orderBy(col("total_revenue").desc())))
        .withColumn(
            "supplier_country_revenue",
            round(sum("total_revenue").over(Window.partitionBy("supplier_country")), 2),
        )
    )

    product_quality = (
        fact.join(products, "product_key")
        .groupBy("product_key", "product_name", "product_category")
        .agg(
            round(avg("product_rating"), 2).alias("avg_rating"),
            max("product_reviews").alias("reviews_count"),
            sum("sale_quantity").alias("total_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
        )
        .withColumn("highest_rating_rank", rank().over(Window.orderBy(col("avg_rating").desc())))
        .withColumn("lowest_rating_rank", rank().over(Window.orderBy(col("avg_rating").asc())))
        .withColumn("reviews_rank", rank().over(Window.orderBy(col("reviews_count").desc())))
    )

    correlation_value = product_quality.select(
        corr("avg_rating", "total_quantity").alias("rating_sales_correlation")
    ).first()["rating_sales_correlation"]
    product_quality = product_quality.withColumn(
        "rating_sales_correlation",
        lit(float(correlation_value) if correlation_value is not None else 0.0),
    )

    reports = {
        "report_product_sales": sales_products,
        "report_customer_sales": sales_customers,
        "report_time_sales": sales_time,
        "report_store_sales": sales_stores,
        "report_supplier_sales": sales_suppliers,
        "report_product_quality": product_quality,
    }

    for table, report_df in reports.items():
        write_clickhouse(report_df, table)
        print(f"Wrote ClickHouse {table}: {report_df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
