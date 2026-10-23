from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import os
JAR_PATH = os.path.join(os.path.dirname(__file__), "postgresql-42.7.3.jar")

JDBC_URL = "jdbc:postgresql://192.168.4.116:5432/stockpipe"
JDBC_PROPS = {
    "user": "stockpipe",
    "password": "stockpipe",
    "driver": "org.postgresql.Driver",
}


def run():
    spark = SparkSession.builder.appName("stockpipe").config("spark.driver.extraClassPath", JAR_PATH).getOrCreate()

    df = spark.read.jdbc(
        url=JDBC_URL, table="raw_daily_prices", properties=JDBC_PROPS
    )

    w = Window.partitionBy("stock_code").orderBy("trade_date")

    # SMA
    df2 = df.withColumn("sma_5", F.avg("close_price").over(w.rowsBetween(-4, 0)))
    df2 = df2.withColumn("sma_20", F.avg("close_price").over(w.rowsBetween(-19, 0)))
    df2 = df2.withColumn("sma_60", F.avg("close_price").over(w.rowsBetween(-59, 0)))

    # RSI
    df2 = df2.withColumn("prev_close", F.lag("close_price").over(w))
    df2 = df2.withColumn("change", F.col("close_price") - F.col("prev_close"))
    df2 = df2.withColumn("gain", F.when(F.col("change") > 0, F.col("change")).otherwise(0))
    df2 = df2.withColumn("loss", F.when(F.col("change") < 0, F.abs(F.col("change"))).otherwise(0))
    rsi_w = w.rowsBetween(-13, 0)
    df2 = df2.withColumn("avg_gain", F.avg("gain").over(rsi_w))
    df2 = df2.withColumn("avg_loss", F.avg("loss").over(rsi_w))
    df2 = df2.withColumn("rsi", 100 - (100 / (1 + F.col("avg_gain") / F.col("avg_loss"))))

    result = df2.select(
        "stock_code", "stock_name", "trade_date", "close_price",
        "sma_5", "sma_20", "sma_60", "rsi",
    )
    result.write.jdbc(
        url=JDBC_URL, table="spark_technical_indicators",
        mode="overwrite", properties=JDBC_PROPS,
    )

    spark.stop()


if __name__ == "__main__":
    run()
