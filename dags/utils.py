from pyspark.sql.types import StructType, StructField, StringType
import logging
import os
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


INDEX_NAME = "CC-MAIN-2025-13"
BASE_URL = "https://data.commoncrawl.org"

POSTGRES_JDBC_URL = "jdbc:postgresql://target_postgres:5432/target_db"
POSTGRES_PROPERTIES = {
    "user": "spark_user",
    "password": "spark_pass",
    "driver": "org.postgresql.Driver",
}

# https://data.gov.au/data/dataset/activity/abn-bulk-extract
URLS = [
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip",
]


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


class CheckpointManager:
    """Manages checkpoint state for DAG tasks."""

    def __init__(self, checkpoint_file, index_name):
        self.checkpoint_file = checkpoint_file
        self.index_name = index_name
        self.state = self._load_checkpoint()

    def _load_checkpoint(self):
        """Load checkpoint state from file."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}. Starting fresh.")
                return {
                    "index_name": self.index_name,
                    "tasks": {},
                    "created_at": datetime.now().isoformat(),
                }
        return {
            "index_name": self.index_name,
            "tasks": {},
            "created_at": datetime.now().isoformat(),
        }

    def _save_checkpoint(self):
        """Save checkpoint state to file."""
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.state, f, indent=2)
        logger.info(f"Checkpoint saved: {self.checkpoint_file}")

    def is_task_completed(self, task_name):
        """Check if a task has been completed."""
        return (
            self.state.get("tasks", {}).get(task_name, {}).get("status") == "completed"
        )

    def mark_task_completed(self, task_name, metadata=None):
        """Mark a task as completed."""
        if "tasks" not in self.state:
            self.state["tasks"] = {}
        self.state["tasks"][task_name] = {
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
            "metadata": metadata or {},
        }
        self._save_checkpoint()
        logger.info(f"✅ Task '{task_name}' marked as completed")

    def mark_task_failed(self, task_name, error_msg=None):
        """Mark a task as failed."""
        if "tasks" not in self.state:
            self.state["tasks"] = {}
        self.state["tasks"][task_name] = {
            "status": "failed",
            "failed_at": datetime.now().isoformat(),
            "error": error_msg,
        }
        self._save_checkpoint()
        logger.warning(f"❌ Task '{task_name}' marked as failed")

    def get_task_metadata(self, task_name):
        """Get metadata for a completed task."""
        return self.state.get("tasks", {}).get(task_name, {}).get("metadata", {})

    def reset_checkpoint(self):
        """Reset checkpoint for a fresh start."""
        self.state = {
            "index_name": self.index_name,
            "tasks": {},
            "created_at": datetime.now().isoformat(),
        }
        self._save_checkpoint()
        logger.info("✅ Checkpoint reset")
