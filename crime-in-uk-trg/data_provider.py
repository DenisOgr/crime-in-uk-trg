import os.path
from typing import Union, Sequence
from logging_provider import get_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, Row
from pyspark.sql import functions as f

SOURCE_DIR = '/work/data/source'
TARGET_DIR = '/work/data/target'
TABLE_NAME = 'data'
logger = get_logger()

crimeSchema = StructType([
    StructField("crimeId", StringType(), False),
    StructField("month", DateType(), False),
    StructField("reportedBy", StringType(), False),
    StructField("fallsWithin", StringType(), False),
    StructField("longitude", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("location", DoubleType(), False),
    StructField("LSOACode", StringType(), False),
    StructField("LSOAName", StringType(), False),
    StructField("crimeType", StringType(), False),
    StructField("lastOutcome", StringType(), False),
    StructField("context", StringType(), False)
])
outcomesSchema = StructType([
    StructField("crimeId", StringType(), False),
    StructField("month", DateType(), False),
    StructField("reportedBy", StringType(), False),
    StructField("fallsWithin", StringType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("location", StringType(), False),
    StructField("LSOACode", StringType(), False),
    StructField("LSOAName", StringType(), False),
    StructField("lastOutcome", StringType(), False),
])


class TargetDirectoryExists(Exception):
    pass


def seed_from_csv(spark: SparkSession):
    """
    Read data from CSV, apply transformations and store to parquet files.
    Args:
        spark: Required. Spark application.

    """
    if os.path.isdir(TARGET_DIR):
        raise TargetDirectoryExists()
    logger.info("Starting the seeding data process... It can takes several minutes.")
    crimes = (spark.read.csv(path=f'{SOURCE_DIR}/*/*-street.csv', header=True, schema=crimeSchema)
              .filter(f.col('crimeId').isNotNull())
              .withColumn("districtName",
                          f.regexp_replace(f.regexp_extract(f.input_file_name(), '\\d{4}-\\d{2}-(.*)-street\\.csv$', 1),
                                           '-', ' '))
              .dropDuplicates(['crimeId']))
    outcomes = (
        spark.read.csv(path=f'{SOURCE_DIR}/*/*-outcomes.csv', header=True, schema=outcomesSchema)
            .filter(f.col('crimeId').isNotNull()).dropDuplicates(['crimeId']))

    merged = (crimes.join(outcomes, ['crimeId'], 'left')
        .select(
        crimes.crimeId,
        crimes.districtName, crimes.latitude,
        crimes.longitude, crimes.crimeType,
        f.when(outcomes.crimeId.isNull(), crimes.lastOutcome).otherwise(outcomes.lastOutcome).alias('lastOutcome')
    ))

    merged.write.parquet(TARGET_DIR)
    logger.info("The seeding data process finished with success.")


def create_view(spark: SparkSession):
    """
    Read parquet files and create temporary view.
    Args:
        spark: Required. Spark application.
    """
    data = spark.read.parquet(TARGET_DIR)
    data.createOrReplaceTempView(TABLE_NAME)


def get_data_from_query(spark: SparkSession, query: str, greedy: bool = True) -> Union[Sequence[Row], DataFrame]:
    """
    Args:
        spark: Required. Spark application.
        query: Required. Query to run.
        greedy: Optional. Type of returning data.

    Returns:
        Either list of Rows of Data frame.
    """
    logger.info(f"Run query: {query}")
    df = spark.sql(query)
    return df.collect() if greedy else df
