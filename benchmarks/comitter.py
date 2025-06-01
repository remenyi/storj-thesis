import os
import time
import random
import string
import warnings
from datetime import datetime
import sys

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
except ImportError:
    sys.exit(1)

try:
    import boto3
    import botocore
except ImportError:
    sys.exit(1)


MINIO_ENDPOINT = "http://your-minio-host:9001"
MINIO_ACCESS_KEY = "YOUR_MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "YOUR_MINIO_SECRET_KEY"
MINIO_REGION = 'us-east-1'

STORJ_ENDPOINT = "http://your-storj-host:80"
STORJ_ACCESS_KEY = "YOUR_STORJ_ACCESS_KEY"
STORJ_SECRET_KEY = "YOUR_STORJ_SECRET_KEY"
STORJ_REGION = 'us-east-1'

S3_CONFIGURATIONS = [
    {
        "name": "MinIO",
        "endpoint_url": MINIO_ENDPOINT,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "region": MINIO_REGION,
        "bucket_prefix": "pyspark-benchmark-minio-k8s"
    },
    {
        "name": "Storj",
        "endpoint_url": STORJ_ENDPOINT,
        "access_key": STORJ_ACCESS_KEY,
        "secret_key": STORJ_SECRET_KEY,
        "region": STORJ_REGION,
        "bucket_prefix": "pyspark-benchmark-storj-k8s"
    },
]

COMMITTERS_TO_TEST = ["directory", "magic", "partitioned", "file"]
NUM_ROWS = 10000000
NUM_PARTITIONS = 3
OUTPUT_FORMAT = "parquet"

K8S_MASTER_URL = "k8s://https://your-k8s-api-server"
SPARK_IMAGE = "remenyi/pyspark:v1.0.6"
SPARK_NAMESPACE = "spark"
SERVICE_ACCOUNT = "spark"
DRIVER_HOST_SVC = "your-driver-service-ip"
EXECUTOR_INSTANCES = 3
EXECUTOR_CORES = 1
EXECUTOR_MEMORY = "1g"
DRIVER_MEMORY = "1g"

HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"

warnings.filterwarnings("ignore", category=ResourceWarning)

def create_s3_client(config):
    try:
        client = boto3.client(
            's3',
            endpoint_url=config['endpoint_url'],
            aws_access_key_id=config['access_key'],
            aws_secret_access_key=config['secret_key'],
            region_name=config.get('region'),
            config=botocore.config.Config(signature_version='s3v4')
        )
        return client
    except Exception:
        return None

def create_s3_bucket(client, bucket_name, config):
    if not client: return False
    try:
        location_constraint = config.get('region') if config.get('region') != 'us-east-1' else None
        create_kwargs = {'Bucket': bucket_name}
        if location_constraint:
             create_kwargs['CreateBucketConfiguration'] = {'LocationConstraint': location_constraint}

        client.create_bucket(**create_kwargs)
        time.sleep(2)
        return True
    except botocore.exceptions.ClientError as e:
        err_code = e.response['Error']['Code']
        if err_code == 'BucketAlreadyOwnedByYou' or err_code == 'BucketAlreadyExists':
            return True
        elif err_code == 'InvalidLocationConstraint' and config.get('region') == 'us-east-1':
             try:
                  client.create_bucket(Bucket=bucket_name)
                  time.sleep(2)
                  return True
             except botocore.exceptions.ClientError as retry_e:
                  if retry_e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                     return True
                  return False
        else:
            return False
    except Exception:
         return False


def cleanup_s3_bucket(client, bucket_name):
    if not client:
        return
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        for page in pages:
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']}
                if objects_to_delete:
                    client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
            else: break

        client.delete_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError:
        pass
    except Exception:
         pass


def create_spark_session(committer_name, s3_config):

    hadoop_aws_jar = f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"
    aws_sdk_jar = f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}"
    hadoop_cloud_jar = f"org.apache.spark:spark-hadoop-cloud_2.12:{HADOOP_AWS_VERSION}"

    log4j_file_name = "/opt/spark/conf/log4j2.properties"
    log4j_file_name_executor = "/opt/spark/log4j2.properties"
    
    jvm_options = f"-Dlog4j.configuration=file:{log4j_file_name} -Dcom.amazonaws.sdk.enableDefaultMetrics"
    jvm_options_executor = f"-Dlog4j.configuration=file:{log4j_file_name_executor} -Dcom.amazonaws.sdk.enableDefaultMetrics"

    try:
        spark_builder = SparkSession.builder \
            .appName(f"S3A Bench K8s - {s3_config['name']} - {committer_name}") \
            .master(K8S_MASTER_URL) \
            .config("spark.driver.extraJavaOptions", jvm_options) \
            .config("spark.executor.extraJavaOptions", jvm_options_executor) \
            .config("spark.task.maxFailures", "1") \
            .config("spark.jars.packages", f"{hadoop_aws_jar},{aws_sdk_jar},{hadoop_cloud_jar}") \
            .config("spark.kubernetes.container.image", SPARK_IMAGE) \
            .config("spark.kubernetes.namespace", SPARK_NAMESPACE)\
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", SERVICE_ACCOUNT)\
            .config("spark.kubernetes.authenticate.serviceAccountName", SERVICE_ACCOUNT) \
            .config("spark.executor.instances", str(EXECUTOR_INSTANCES))\
            .config("spark.executor.cores", str(EXECUTOR_CORES)) \
            .config("spark.kubernetes.executor.request.cores", "200m") \
            .config("spark.kubernetes.executor.limit.cores", "500m") \
            .config("spark.executor.memory", EXECUTOR_MEMORY)  \
            .config("spark.kubernetes.container.image.pullPolicy", "Always") \
            .config("spark.driver.host", DRIVER_HOST_SVC) \
            .config("spark.driver.port", "2222")        \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.blockManager.port", "7777") \
            .config("spark.driver.memory", DRIVER_MEMORY)\
            .config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint_url']) \
            .config("spark.hadoop.fs.s3a.access.key", s3_config['access_key']) \
            .config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key']) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.committer.name", committer_name) \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
            .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
            .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol") \
            .config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter") \
            .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")

        if committer_name == "magic":
             spark_builder.config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        elif committer_name == "directory":
             spark_builder.config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")

        spark = spark_builder.getOrCreate()

        return spark

    except Exception:
        return None


def generate_data(spark, num_rows, num_partitions):
    if not spark: return None
    try:
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("random_string", StringType(), True),
            StructField("random_double", DoubleType(), True),
            StructField("group_id", IntegerType(), True)
        ])

        df = spark.range(0, num_rows, 1, spark.sparkContext.defaultParallelism) \
                  .withColumn("random_string", F.expr("uuid()")) \
                  .withColumn("random_double", F.rand() * 1000) \
                  .withColumn("group_id", (F.rand() * 10).cast(IntegerType())) \
                  .select("id", "random_string", "random_double", "group_id")

        return df.repartition(num_partitions)
    except Exception:
        return None

all_results = {}
overall_start_time = time.perf_counter()

for config in S3_CONFIGURATIONS:
    endpoint_name = config['name']
    endpoint_results = {}
    all_results[endpoint_name] = endpoint_results

    timestamp_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
    current_bucket_name = f"{config['bucket_prefix']}-{timestamp_suffix}"

    s3_client = create_s3_client(config)
    if not create_s3_bucket(s3_client, current_bucket_name, config):
        all_results[endpoint_name] = "BUCKET_SETUP_FAILED"
        continue

    for committer in COMMITTERS_TO_TEST:
        spark = None
        run_output_path = f"s3a://{current_bucket_name}/{OUTPUT_FORMAT}/{committer}"

        try:
            spark = create_spark_session(committer, config)
            if not spark: raise RuntimeError("Spark Session creation failed.")

            df = generate_data(spark, NUM_ROWS, NUM_PARTITIONS)
            if df is None: raise RuntimeError("Data generation failed.")

            write_start = time.perf_counter()

            df.write \
              .format(OUTPUT_FORMAT) \
              .mode("overwrite") \
              .save(run_output_path)

            write_duration = time.perf_counter() - write_start
            endpoint_results[committer] = write_duration

        except Exception:
            endpoint_results[committer] = "FAILED"
        finally:
            if spark:
                try:
                    spark.stop()
                except Exception:
                    pass
            spark = None

    cleanup_s3_bucket(s3_client, current_bucket_name)


overall_end_time = time.perf_counter()
