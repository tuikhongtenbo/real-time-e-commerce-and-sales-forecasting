from pyspark.sql.functions import *

spark.conf.set(
    "fs.azure.account.key.<storage_account>.dfs.core.windows.net",
    "<Storage_Account_Access_Key>"
)

silver_path = "abfss://ecommerce@<storage_account>.dfs.core.windows.net/silver/"
gold_path = "abfss://ecommerce@<storage_account>.dfs.core.windows.net/gold/"

df_silver = (
    spark.readStream
    .format("delta")
    .load(silver_path)
)

df_gold = (
    df_silver
    .withWatermark("timestamp", "1 minute")
    .groupby(
        window("timestamp", "1 minute"),
        "region",
        "city_province"
    )
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("total_items")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "region",
        "city_province",
        "total_sales",
        "total_items"
    )
)

(
    df_gold.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", gold_path + "_checkpoint")
    .start(gold_path)
)