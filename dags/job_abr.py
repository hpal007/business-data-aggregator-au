import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_date, year, month, coalesce, lit
from pyspark.sql.types import StringType, LongType
import logging

# Logger setup (could also be passed externally)
logger = logging.getLogger(__name__)


# A list of common Australian company suffixes to remove
AUSTRALIAN_SUFFIXES = [
    r"\s+PTY\s+LTD\b",
    r"\s+P\.\s*L\.\s*LTD\b",
    r"\s+LIMITED\b",
    r"\s+LTD\b",
    r"\s+CO\b",
    r"\s+CORP\b",
    r"\s+INC\b",
    r"\s+LLC\b",
    r"\s+COMPANY\b",
    r"\s+THE\b",
    r"\s+OF\b",
]
STOP_WORDS = ["the", "a", "and", "co"]


def clean_name(name):
    # 1. Lowercase
    name = str(name).lower()
    # 2. Remove punctuation (keep spaces)
    name = re.sub(r"[^\w\s]", "", name)
    # 3. Strip legal suffixes
    for suffix in AUSTRALIAN_SUFFIXES:
        name = re.sub(suffix, "", name, flags=re.IGNORECASE).strip()
    # 4. Remove stop words and multiple spaces
    name_tokens = [word for word in name.split() if word not in STOP_WORDS]
    return " ".join(name_tokens)


def process_abr_parquet_to_table(
    spark: SparkSession, parquet_dir: str, base_dir: str, abr_schema
):
    # Read parquet files
    df = spark.read.schema(abr_schema).parquet(f"{parquet_dir}/*.parquet")

    clean_name_udf = udf(clean_name, StringType())

    active_df = (
        df.filter(df["abn_status"] == "ACT")
        .filter(col("entity_name").isNotNull())
        .withColumn("clean_entity_name", clean_name_udf(col("entity_name")))
        .withColumn("abn_start_date", to_date("abn_start_date", "yyyyMMdd"))
        .withColumn("year", year("abn_start_date"))
        .withColumn("month", month("abn_start_date"))
        .withColumn("abn", df["abn"].cast(LongType()))
        .withColumn("entity_state", coalesce("entity_state", lit("unknown")))
        .orderBy("abn", "entity_name", "entity_state")
    )

    output_path = os.path.join(base_dir, "abr_tbl")
    active_df.write.partitionBy("year", "entity_state").mode("overwrite").parquet(
        output_path
    )

    return output_path
