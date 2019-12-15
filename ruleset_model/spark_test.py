from pyspark import RDD
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField,  IntegerType, TimestampType, FloatType
from pyspark.mllib.random import RandomRDDs
from pyspark.sql.functions import lit, sum, col, round



if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("spark_test")\
        .getOrCreate()
    print('PySpark initialized...')

    # Generate random data
    row_number = 10_000_000
    number_of_days = 10
    seconds_per_row = (number_of_days * 24 * 60 * 60) / row_number
    print('Default parallelism', spark.sparkContext.defaultParallelism)
    print('Seconds per row', seconds_per_row)

    price_diffs: RDD = RandomRDDs.uniformRDD(spark.sparkContext, row_number, 10)
    quantity = RandomRDDs.uniformRDD(spark.sparkContext, row_number, 10).map(lambda x: int(x * 100))
    trade_data = price_diffs.zip(quantity)

    schema = StructType([
        StructField("price", FloatType(), True),
        StructField("quantity", IntegerType(), False),
    ])
    print('Parallelize call...')
    trades = spark.createDataFrame(
        data = trade_data,
        schema = schema,
    ).repartition(10)
    trades = trades.withColumn('instrument', lit('GARAN'))
    trades = trades.withColumn('time', lit(seconds_per_row))
    trades = trades.withColumn(
        'time',
        sum('time').
        over(
            Window.orderBy(col('time').asc()).
            partitionBy('instrument').
            rowsBetween(Window.unboundedPreceding, 0)
        ).
        cast(TimestampType())
    )
    trades = trades.withColumn('date', trades['time'].cast('date'))
    trades = trades.withColumn(
        'price',
        sum('price').
        over(
            Window.orderBy(col('time').asc()).
            partitionBy('instrument').
            rowsBetween(Window.unboundedPreceding, 0)
        )
    )
    trades = trades.withColumn('price', round(trades['price'], 2))
    trades = trades.withColumn(
        'traded_volume',
        sum('quantity').
        over(
            Window.orderBy(col('time').asc()).
            partitionBy(['instrument', 'date']).
            rowsBetween(Window.unboundedPreceding, 0)
        )
    )
    print('Printing...')
    print(trades.limit(10).show())

    input()

    spark.stop()
