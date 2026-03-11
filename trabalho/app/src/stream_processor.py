from __future__ import annotations

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, expr, sum as _sum, count, avg, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def get_spark_session():
    builder = SparkSession.builder \
        .appName("BlackFridayStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.33,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    return builder.getOrCreate()

def get_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("seller_name", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("category", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("gross_amount", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("shipping_amount", DoubleType(), True),
        StructField("net_revenue", DoubleType(), True),
        StructField("cogs", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("target_gmv", DoubleType(), True),
        StructField("target_orders", IntegerType(), True)
    ])

def write_to_mysql(df, table_name, mode="append"):
    jdbc_url = "jdbc:mysql://mysql:3306/demo_db"
    db_properties = {
        "user": "demo_user",
        "password": "demo_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    try:
        df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=db_properties)
    except Exception as e:
        print(f"Error writing to MySQL table {table_name}: {e}")

def process_batch(df, epoch_id):
    print(f"Processing batch {epoch_id}...")
    
    # Cache the dataframe as we will use it multiple times
    df.cache()
    
    # 1. Write Raw Data to MinIO (Parquet)
    minio_bucket = os.getenv("MINIO_BUCKET", "lakehouse")
    s3_path = f"s3a://{minio_bucket}/data/orders"
    print(f"Writing raw batch to MinIO at {s3_path}")
    try:
        df.write.mode("append").parquet(s3_path)
    except Exception as e:
        print(f"Error writing to MinIO: {e}")
        
    # 2. Write Raw Data to MySQL (table 'data')
    print("Writing raw batch to MySQL table 'data'")
    write_to_mysql(df, "data")

    # 3. Calculate KPIs for Dashboard
    # We need to aggregate data to satisfy the dashboard requirements in README.md
    # Since this is a streaming batch, we are calculating metrics for THIS BATCH.
    # For a real-time dashboard, we usually upsert these into a database table that holds the current state.
    # However, standard Spark JDBC write is append or overwrite.
    # For simplicity in this assignment, we will append aggregated metrics to specific tables in MySQL.
    # The dashboard can then query these tables (e.g., sum of all records or latest snapshot).
    
    # Filter for valid sales (paid) for most metrics, or consider all depending on definition.
    # Usually GMV includes everything, Net Revenue excludes cancellations/returns.
    # Let's assume GMV is gross_amount of all orders placed (status != CANCELLED? or all?)
    # README says "Simulação Black Friday", usually GMV is Gross Merchandise Value of confirmed orders.
    # Let's filter out CANCELLED for sales metrics if we want "Realizado".
    # But for "Cancelamentos e devoluções" we need those.
    
    # Let's create a few aggregate views.
    
    # --- C-Level ---
    # GMV, Lucro Líquido, Ticket Médio
    # GMV = sum(gross_amount)
    # Lucro Líquido = sum(net_revenue - cogs) (Approx)
    # Ticket Médio = GMV / count(orders)
    
    c_level = df.groupBy().agg(
        _sum("gross_amount").alias("gmv"),
        _sum(expr("net_revenue - cogs")).alias("net_profit"),
        count("order_id").alias("total_orders"),
        avg("gross_amount").alias("avg_ticket"),
        current_timestamp().alias("calculation_time")
    )
    write_to_mysql(c_level, "kpi_c_level")

    # --- Vendas (Sales) ---
    # GMV por vendedor, loja
    sales_by_seller = df.groupBy("seller_name", "store_name").agg(
        _sum("gross_amount").alias("gmv"),
        count("order_id").alias("num_orders"),
        avg("gross_amount").alias("avg_ticket"),
        avg("discount_pct").alias("avg_discount"),
        current_timestamp().alias("calculation_time")
    )
    write_to_mysql(sales_by_seller, "kpi_sales_by_seller")
    
    # --- Controladoria ---
    # Margem bruta, Custo, Impacto de descontos, Cancelamentos, Lucro por categoria/canal
    # Margem Bruta = (Net Revenue - COGS) / Net Revenue
    # But here we just aggregate absolute values for the dashboard to calculate ratios.
    
    control_metrics = df.groupBy("category", "channel").agg(
        _sum("gross_amount").alias("gmv"),
        _sum("net_revenue").alias("net_revenue"),
        _sum("cogs").alias("total_cost"),
        _sum("discount_amount").alias("total_discount"),
        _sum(when(col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        _sum(when(col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
        current_timestamp().alias("calculation_time")
    )
    write_to_mysql(control_metrics, "kpi_controladoria")

    df.unpersist()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092,kafka-2:29093,kafka-3:29094")
    topic = os.getenv("KAFKA_TOPIC_ORDERS", "blackfriday_orders")

    print(f"Reading from Kafka topic {topic} at {kafka_bootstrap}")

    # Read from Kafka
    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    schema = get_schema()
    
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Transformations
    df_transformed = df_parsed \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time"))) \
        .withColumn("processing_time", current_timestamp())

    # Start Query
    query = df_transformed \
        .writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
