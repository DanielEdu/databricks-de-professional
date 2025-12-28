from pyspark.sql import functions as F
from datetime import datetime
from databricks.sdk.runtime import spark
import logging

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)
schema = "key STRING, offset STRING, partition STRING, timestamp STRING, topic STRING, value BINARY"

def process_bronze(topic: str, bad_rec_path:str, schema_value:str):

    checkpoint_path = f"/Volumes/bronze/checkpoint/{topic}"
    table_name      = f"bronze.bookstore.{topic}_brz"
    load_path       = "/Volumes/landing/kafka/data/"

    logger.info("🚀 Starting Bronze ingestion process")
    logger.info(f"📂 Source Topic: {topic}")
    logger.info(f"🧱 Target table: {table_name}")
    logger.info(f"📌 Checkpoint path: {checkpoint_path}")

    start_time = datetime.now()

    try:
        logger.info("🔄 Initializing Auto Loader stream")

        query_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(schema)
            .load(load_path)
            .filter(F.col("topic") == topic)
            .select(
                    F.col('value').cast("string").alias('value'),
                    F.struct(
                        F.col("partition").cast("string").alias("partition"),
                        F.col("offset").cast("string").alias("offset"),
                        F.col("topic").cast("string").alias("topic"),
                        F.col("timestamp").cast("string").alias("timestamp"),
                        ).alias('metadata'),
                    F.current_timestamp().alias('auditTime')
                )
            .withColumn('value_flat', F.from_json(F.col('value'), schema_value))   
            .select(
                F.col('value_flat.*'),
                F.col('metadata'),
                F.col('auditTime'),
                F.col('value')
            )
        )

        streaming_query = (
            query_df
                .writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint_path)
                .outputMode("append")
                .option("mergeSchema", True)
                .trigger(availableNow=True)
                .toTable(table_name)
        )
        logger.info("⏳ Waiting for streaming query to finish...")
        streaming_query.awaitTermination()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("✅ Bronze ingestion completed successfully")
        logger.info(f"⏱️ Total execution time: {duration:.2f} seconds")

    except Exception as e:
        logger.error("❌ Bronze ingestion failed")
        logger.exception(e)
        raise