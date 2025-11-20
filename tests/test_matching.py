import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from dags.job_matching import fuzzy_match_udf

class TestMatching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestMatching") \
            .master("local[1]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_fuzzy_match_udf(self):
        # Test exact match
        score = self.spark.sql("SELECT fuzzy_match_udf('Acme Corp', 'Acme Corp')").collect()[0][0]
        self.assertEqual(score, 100)

        # Test partial match
        self.spark.udf.register("fuzzy_match_udf", fuzzy_match_udf)
        score = self.spark.sql("SELECT fuzzy_match_udf('Acme Corp', 'Acme Corporation')").collect()[0][0]
        self.assertTrue(score > 80)

        # Test no match
        score = self.spark.sql("SELECT fuzzy_match_udf('Acme Corp', 'Banana Inc')").collect()[0][0]
        self.assertTrue(score < 50)

if __name__ == '__main__':
    unittest.main()
