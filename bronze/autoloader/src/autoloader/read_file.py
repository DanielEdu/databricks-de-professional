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


def process_bronze(target: str):

    source_path = "/Volumes/landing/country/data/"
    checkpoint_path = "/Volumes/landing/country/checkpoint"

    logger.info("🚀 Starting Bronze ingestion process")
    logger.info(f"📂 Source path: {source_path}")
    logger.info(f"🧱 Target table: {target}")
    logger.info(f"📌 Checkpoint path: {checkpoint_path}")

    start_time = datetime.now()

    schema = "code STRING, calling_code STRING, country STRING"

    try:
        logger.info("🔄 Initializing Auto Loader stream")

        query_df = (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .schema(schema)
                .load(source_path)
                .withColumn("ingesttime", F.current_timestamp())
        )

        logger.info("✍️ Writing stream to Delta table (append mode)")

        streaming_query = (
            query_df
                .writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint_path)
                .outputMode("append")
                .option("mergeSchema", True)
                .trigger(availableNow=True)
                .toTable(target)
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