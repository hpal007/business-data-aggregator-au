from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import to_date, year, month, coalesce, lit
import xml.etree.ElementTree as ET

# Global Spark session
_spark = None


def get_spark_session(app_name="ABR_Processor"):
    """Get or create a Spark session."""
    global _spark
    if _spark is None:
        _spark = SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()
    return _spark


# Define schema to be used across tasks
ABR_SCHEMA = StructType(
    [
        StructField("abn", StringType(), True),
        StructField("abn_status", StringType(), True),
        StructField("abn_start_date", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("entity_type_text", StringType(), True),
        StructField("entity_name", StringType(), True),
        StructField("entity_state", StringType(), True),
        StructField("entity_postcode", StringType(), True),
    ]
)


def stop_spark():
    """Stop the Spark session if it exists."""
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None


def safe_get(elem, path, attr=None):
    """Get text or attribute from XML element, return None if not found."""
    found = elem.find(path) if path else elem
    if found is None:
        return None
    return found.get(attr) if attr else found.text


def parse_abn_xml_iterative(file_path, batch_size=50000):
    """Parse large XML file iteratively in batches to avoid memory issues."""
    context = ET.iterparse(file_path, events=("end",))
    batch = []
    for event, elem in context:
        if elem.tag == "ABR":
            record = {
                "abn": safe_get(elem, "ABN"),
                "abn_status": safe_get(elem, "ABN", "status"),
                "abn_start_date": safe_get(elem, "ABN", "ABNStatusFromDate"),
                "entity_type": safe_get(elem, "EntityType/EntityTypeInd"),
                "entity_type_text": safe_get(elem, "EntityType/EntityTypeText"),
                "entity_name": safe_get(
                    elem, "MainEntity/NonIndividualName/NonIndividualNameText"
                ),
                "entity_state": safe_get(
                    elem, "MainEntity/BusinessAddress/AddressDetails/State"
                ),
                "entity_postcode": safe_get(
                    elem, "MainEntity/BusinessAddress/AddressDetails/Postcode"
                ),
            }
            batch.append(record)
            elem.clear()
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def process_active_records(df):
    """Process active ABR records with standard transformations."""
    return (
        df.filter(df["abn_status"] == "ACT")
        .withColumn("abn_start_date", to_date("abn_start_date", "yyyyMMdd"))
        .withColumn("year", year("abn_start_date"))
        .withColumn("month", month("abn_start_date"))
        .withColumn("abn", df["abn"].cast(LongType()))
        .withColumn("entity_state", coalesce("entity_state", lit("unknown")))
        .orderBy("abn", "entity_name", "entity_state")
    )
