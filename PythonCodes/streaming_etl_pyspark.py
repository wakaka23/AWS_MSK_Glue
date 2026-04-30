# ライブラリインポート
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job

# Glue Jobの初期化
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# スキーマ定義
value_schema = StructType([
	StructField("store_id", StringType()),
	StructField("amount", IntegerType()),
	StructField("timestamp", StringType()),
])

# MSK Serverlessからデータ読み込み
df_kafka = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "<BootStrapServersのURL>") \
  .option("subscribe", "test-topic") \
  .option("startingOffsets", "earliest") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
  .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
  .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
  .load()

# Transform処理関数を定義
def process_batch(data_frame, batch_id):
	if data_frame.count() > 0:
		data_frame.selectExpr(
			"CAST(key AS STRING) AS key",
			"CAST(value AS STRING) AS value_str",
			"topic",
			"partition",
			"offset",
			"timestamp AS kafka_timestamp",
		) \
		.withColumn("value", from_json(col("value_str"), value_schema)) \
		.selectExpr(
			"key",
			"value.store_id",
			"value.amount",
			"value.timestamp AS event_timestamp",
			"topic",
			"partition",
			"offset",
			"kafka_timestamp",
		) \
		.write \
		.mode("append") \
		.option("header", "true") \
		.parquet("s3://<出力先S3バケット>/output/")

# S3への書き込みストリーミング処理
query = df_kafka \
  .writeStream \
  .foreachBatch(process_batch) \
  .option("checkpointLocation", "s3://<出力先S3バケット>/checkpoint/") \
  .trigger(processingTime="30 seconds") \
  .start()

query.awaitTermination()
job.commit()