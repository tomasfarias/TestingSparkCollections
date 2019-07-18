import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from spark_test import assert_dataframe_equal

from transforms import transform, transform_bugged, transform_wrong_order


@pytest.mark.parametrize('transform', (transform, transform_bugged, transform_wrong_order))
def test_transform(test_df, spark, transform):
    """Test transform returns adoptions per year"""
    schema = StructType([
        StructField('AdoptedYear', IntegerType()),
        StructField('Species', StringType()),
        StructField('count', LongType(), False)
    ])

    expected = spark.createDataFrame([
        (2017, 'Cat', 1),
        (2017, 'Dog', 1),
        (2018, 'Cat', 1),
        (2018, 'Dog', 2),
        (2019, 'Cat', 1),
        (2019, 'Dog', 2)
    ], schema=schema)

    result = transform(test_df)

    assert_dataframe_equal(expected, result)
