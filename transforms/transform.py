import datetime as dt

from pyspark.sql.functions import year


def transform(df):
    """Get year from date and count species adoptions per year"""

    df = df.withColumn('AdoptedYear', year(df['AdoptedDate']))
    df = df.groupBy(['AdoptedYear', 'Species']).count()

    return df.sort(['AdoptedYear', 'Species', 'count'])
