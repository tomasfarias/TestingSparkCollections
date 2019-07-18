import pytest
from spark_test import assert_dataframe_equal

from transforms.transform import transform


@pytest.mark.parametrize('transform', (transform, ))
def test_transform(test_df, spark, transform):
    """Test transform returns adoptions per year"""

    expected = spark.createDataFrame([
        (2017, 'Cat', 1),
        (2017, 'Dog', 1),
        (2018, 'Cat', 1),
        (2018, 'Dog', 2),
        (2019, 'Cat', 1),
        (2019, 'Dog', 2)
    ], schema=['AdoptedYear', 'Species', 'count'])

    result = transform(test_df)

    assert_dataframe_equal(expected, result)
