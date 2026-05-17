from pyspark.sql.functions import *

spark.conf.set(
    "fs.azure.account.key.ecommercestoragethang.dfs.core.windows.net",
    dbutils.secrets.get(scope="ecommerce-scope", key="storage-key")
)

bronze_path = "abfss://ecommerce@ecommercestoragethang.dfs.core.windows.net/bronze/"
silver_path = "abfss://ecommerce@ecommercestoragethang.dfs.core.windows.net/silver/"

df_bronze = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

df_clean = (
    df_bronze
    .withColumn("timestamp", to_timestamp("timestamp"))
    .withColumn("price", when(col("price").isNull(), 0.0).otherwise(col("price")))
    .withColumn("quantity", when(col("quantity").isNull(), 1).otherwise(col("quantity")))
    .withColumn("total_amount", col("price") * col("quantity"))
    .dropDuplicates(["order_id"])
    .filter(col("region").isNotNull())
    .filter(col("city_province").isNotNull())
)

(
    df_clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", silver_path + "_checkpoint")
    .start(silver_path)
)