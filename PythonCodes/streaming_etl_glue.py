# ライブラリインポート
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# MSKからのデータ読み込み処理計画
df_kafka = glueContext.create_data_frame.from_catalog(
    database="data_analyze_database",
    table_name="data_analyze_table",
    additional_options={
        "startingOffsets": "earliest",
        "classification": "json",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "AWS_MSK_IAM",
        "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }
)

# マイクロバッチごとの処理（S3にParquet形式で書き出す）
def process_batch(data_frame, batchId):
    if data_frame.count() > 0:
        data_frame \
            .write \
            .mode("append") \
            .parquet("s3://<出力先バケット名>/output/")

# forEachBatch でデータストリーミング処理を開始
glueContext.forEachBatch(
    frame=df_kafka,
    batch_function=process_batch,
    options={
        "windowSize": "30 seconds",
        "checkpointLocation": "s3://<出力先バケット名>/checkpoint/"
    }
)

job.commit()