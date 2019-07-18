import datetime as dt

from pyspark.sql.functions import year, when, col


def transform_bugged(df):
    """Get year from date and count species adoptions per year"""

    df = df.withColumn('AdoptedYear', year(df['AdoptedDate']))
    df = df.groupBy(['AdoptedYear', 'Species']).count()

    df = df.withColumn(  # This shouldn't be here
        'count', when(col('Species') == 'Cat', df['count'] + 1).otherwise(df['count'])
    )

    return df.sort(['AdoptedYear', 'Species', 'count'])


def transform_wrong_order(df):
    """Get year from date and count species adoptions per year"""

    df = df.withColumn('AdoptedYear', year(df['AdoptedDate']))
    df = df.groupBy(['AdoptedYear', 'Species']).count()

    return df.sort(['Species', 'AdoptedYear', 'count'])
