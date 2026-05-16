from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    StructField('order_id', StringType()),
    StructField('timestamp', StringType()),
    StructField('customer_id', StringType()),
    StructField('product_id', StringType()),
    StructField('category', StringType()),
    StructField('price', DoubleType()),
    StructField('quantity', IntegerType()),
    StructField('total_amount', DoubleType()),
    StructField('payment_method', StringType()),
    StructField('region', StringType()),
    StructField('city_province', StringType()),
    StructField('latitude', DoubleType()),
    StructField('longitude', DoubleType()),
    StructField('delivery_status', StringType()),
])

event_hub_namespace = "<NAMESPACE_HOST_NAME>"
event_hub_name = "<EVENT_HUB_NAME>"
event_hub_conn_str = "<YOUR_EVENT_HUB_CONNECTION_STRING>"

eh_conf = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

df_raw = (
    spark.readStream
    .format('kafka')
    .options(**eh_conf)
    .load()
)

df_orders = (
    df_raw.selectExpr("Cast(value AS STRING) as json")
    .select(from_json("json", schema).alias("data"))
    .select("data.*")
)

spark.conf.set(
    "fs.azure.account.key.<storage_account>.dfs.core.windows.net",
    "<Storage_Account_Access_Key>"
)

bronze_path = "abfss://ecommerce@<storage_account>.dfs.core.windows.net/bronze/"

(
    df_orders.writeStream
    .format('delta')
    .outputMode("append")
    .option("checkpointLocation", bronze_path + "_checkpoint")
    .start(bronze_path)
)