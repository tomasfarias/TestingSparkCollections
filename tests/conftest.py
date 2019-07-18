import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .appName('Test app') \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture(scope='session')
def test_df(spark):

    df = spark.read.csv('data.csv', header=True)

    yield df

    df.unpersist()
