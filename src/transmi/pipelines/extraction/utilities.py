import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, DateType


def _prepare_output_extraction_troncal(items: pd.DataFrame):
    """Prepare output of the node"""
    spark = SparkSession.builder.getOrCreate()
    sql_ctx = SQLContext(spark.sparkContext)
    if (items is None) or (len(items) == 0):
        schema = StructType(
            [
                StructField("day", DateType(), True),
            ]
        )
        empty_rdd = spark.sparkContext.emptyRDD()
        empty_df = spark.createDataFrame(empty_rdd, schema)
        return empty_df
    else:
        items_spark = sql_ctx.createDataFrame(items)
        return items_spark
