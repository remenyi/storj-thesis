import os
import time
import warnings
from datetime import datetime
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    LongType, BinaryType
)

MINIO_ENDPOINT = "https://your-minio-host"
MINIO_ACCESS_KEY = "YOUR_MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "YOUR_MINIO_SECRET_KEY"
MINIO_REGION = 'eu-north-1'
MINIO_INPUT_CSV_PATH = "s3a://your-minio-bucket/gt7data.csv"
MINIO_OUTPUT_BASE_PATH = "s3a://your-minio-bucket/"

STORJ_ENDPOINT = "http://your-storj-host:80"
STORJ_ACCESS_KEY = "YOUR_STORJ_ACCESS_KEY"
STORJ_SECRET_KEY = "YOUR_STORJ_SECRET_KEY"
STORJ_REGION = 'us-east-1'
STORJ_INPUT_CSV_PATH = "s3a://your-storj-bucket/gt7data.csv"
STORJ_OUTPUT_BASE_PATH = "s3a://your-storj-bucket/"

S3_CONFIGURATIONS = [
    {
        "name": "MinIO",
        "endpoint_url": MINIO_ENDPOINT,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "region": MINIO_REGION,
        "input_path": MINIO_INPUT_CSV_PATH,
        "output_base_path": MINIO_OUTPUT_BASE_PATH
    },
    {
        "name": "Storj",
        "endpoint_url": STORJ_ENDPOINT,
        "access_key": STORJ_ACCESS_KEY,
        "secret_key": STORJ_SECRET_KEY,
        "region": STORJ_REGION,
        "input_path": STORJ_INPUT_CSV_PATH,
        "output_base_path": STORJ_OUTPUT_BASE_PATH
    },
]

OUTPUT_PARTITIONS = 3
OUTPUT_FORMAT = "parquet"
TARGET_COMMITTER = "magic"

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

DATA_SCHEMA = StructType([
    StructField("magic", LongType(), False),
    StructField("position_x", DoubleType(), True),
    StructField("position_y", DoubleType(), True),
    StructField("position_z", DoubleType(), True),
    StructField("world_velocity_x", DoubleType(), True),
    StructField("world_velocity_y", DoubleType(), True),
    StructField("world_velocity_z", DoubleType(), True),
    StructField("rotation_x", DoubleType(), True),
    StructField("rotation_y", DoubleType(), True),
    StructField("rotation_z", DoubleType(), True),
    StructField("northorientation", DoubleType(), True),
    StructField("angularvelocity_x", DoubleType(), True),
    StructField("angularvelocity_y", DoubleType(), True),
    StructField("angularvelocity_z", DoubleType(), True),
    StructField("body_height", DoubleType(), True),
    StructField("rpm", DoubleType(), True),
    StructField("iv1", StringType(), True),
    StructField("iv2", StringType(), True),
    StructField("iv3", StringType(), True),
    StructField("iv4", StringType(), True),
    StructField("fuel_level", DoubleType(), True),
    StructField("fuel_capacity", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("boost", DoubleType(), True),
    StructField("oil_pressure_bar", DoubleType(), True),
    StructField("water_temperature", DoubleType(), True),
    StructField("oil_temperature", DoubleType(), True),
    StructField("tire_temp_FL", DoubleType(), True),
    StructField("tire_temp_FR", DoubleType(), True),
    StructField("tire_temp_RL", DoubleType(), True),
    StructField("tire_temp_RR", DoubleType(), True),
    StructField("pkt_id", IntegerType(), True),
    StructField("current_lap", IntegerType(), True),
    StructField("total_laps", IntegerType(), True),
    StructField("best_lap_time", IntegerType(), True),
    StructField("last_lap_time", IntegerType(), True),
    StructField("day_progression_ms", LongType(), True),
    StructField("pre_race_start_position", IntegerType(), True),
    StructField("pre_race_num_cars", IntegerType(), True),
    StructField("min_alert_rpm", IntegerType(), True),
    StructField("max_alert_rpm", IntegerType(), True),
    StructField("calculated_max_speed", IntegerType(), True),
    StructField("flags", IntegerType(), True),
    StructField("suggestedgear_gear", IntegerType(), True),
    StructField("throttle", IntegerType(), True),
    StructField("brake", IntegerType(), True),
    StructField("padding_byte1", StringType(), True),
    StructField("road_plane_x", DoubleType(), True),
    StructField("road_plane_y", DoubleType(), True),
    StructField("road_plane_z", DoubleType(), True),
    StructField("road_plane_dist", DoubleType(), True),
    StructField("tire_rps_FL", DoubleType(), True),
    StructField("tire_rps_FR", DoubleType(), True),
    StructField("tire_rps_RL", DoubleType(), True),
    StructField("tire_rps_RR", DoubleType(), True),
    StructField("tire_radius_FL", DoubleType(), True),
    StructField("tire_radius_FR", DoubleType(), True),
    StructField("tire_radius_RL", DoubleType(), True),
    StructField("tire_radius_RR", DoubleType(), True),
    StructField("susp_height_FL", DoubleType(), True),
    StructField("susp_height_FR", DoubleType(), True),
    StructField("susp_height_RL", DoubleType(), True),
    StructField("susp_height_RR", DoubleType(), True),
    StructField("unknown_single1", DoubleType(), True),
    StructField("unknown_single2", DoubleType(), True),
    StructField("unknown_single3", DoubleType(), True),
    StructField("unknown_single4", DoubleType(), True),
    StructField("unknown_single5", DoubleType(), True),
    StructField("unknown_single6", DoubleType(), True),
    StructField("unknown_single7", DoubleType(), True),
    StructField("unknown_single8", DoubleType(), True),
    StructField("clutch_pedal", DoubleType(), True),
    StructField("clutch_engagement", DoubleType(), True),
    StructField("rpm_clutch_gearbox", DoubleType(), True),
    StructField("transmission_top_speed", DoubleType(), True),
    StructField("gear_ratio1", DoubleType(), True),
    StructField("gear_ratio2", DoubleType(), True),
    StructField("gear_ratio3", DoubleType(), True),
    StructField("gear_ratio4", DoubleType(), True),
    StructField("gear_ratio5", DoubleType(), True),
    StructField("gear_ratio6", DoubleType(), True),
    StructField("gear_ratio7", DoubleType(), True),
    StructField("gear_ratio8", DoubleType(), True),
    StructField("car_code", IntegerType(), True),
    StructField("WheelRotationRadians", DoubleType(), True),
    StructField("FillerFloatFB", DoubleType(), True),
    StructField("Sway", DoubleType(), True),
    StructField("Heave", DoubleType(), True),
    StructField("Surge", DoubleType(), True)
])


def download_csv_to_dataframe(spark: SparkSession, input_path: str, schema: StructType) -> (DataFrame, float):
    start_time = time.perf_counter()
    df = None
    try:
        df = spark.read \
            .format("csv") \
            .schema(schema) \
            .option("header", "true") \
            .load(input_path)

        df.rdd.getNumPartitions()
        duration = time.perf_counter() - start_time
        return df, duration
    except Exception:
        raise


def convert_and_upload_parquet(spark: SparkSession, df: DataFrame, output_path: str, num_partitions: int) -> float:
    start_time = time.perf_counter()
    try:
        if num_partitions > 0:
             write_df = df.repartition(num_partitions)
        else:
             write_df = df

        write_df.write \
          .format(OUTPUT_FORMAT) \
          .mode("overwrite") \
          .save(output_path)

        duration = time.perf_counter() - start_time
        return duration
    except Exception:
        raise

def create_spark_session(s3_config):
    committer_name = TARGET_COMMITTER

    hadoop_aws_jar = f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"
    aws_sdk_jar = f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}"
    hadoop_cloud_jar = f"org.apache.spark:spark-hadoop-cloud_2.12:{HADOOP_AWS_VERSION}"

    log4j_file_name = "/opt/spark/conf/log4j2.properties"
    log4j_file_name_executor = "/opt/spark/log4j2.properties"

    jvm_options = f"-Dlog4j.configuration=file:{log4j_file_name} -Dcom.amazonaws.sdk.enableDefaultMetrics"
    jvm_options_executor = f"-Dlog4j.configuration=file:{log4j_file_name_executor} -Dcom.amazonaws.sdk.enableDefaultMetrics"

    try:
        spark_builder = SparkSession.builder \
            .appName(f"CSV2Parquet K8s - {s3_config['name']} - {committer_name}") \
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
            .config("spark.driver.port", "2222") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.blockManager.port", "6666") \
            .config("spark.driver.memory", DRIVER_MEMORY) \
            .config("spark.driver.request.cores", "200m") \
            .config("spark.driver.limit.cores", "500m") \
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
            .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
            .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")

        spark = spark_builder.getOrCreate()
        
        return spark

    except Exception:
        return None

all_results = {}
overall_start_time = time.perf_counter()

for config in S3_CONFIGURATIONS:
    endpoint_name = config['name']

    endpoint_result = None
    spark = None

    try:
        spark = create_spark_session(config)
        if not spark:
            raise RuntimeError()

        input_df, read_duration = download_csv_to_dataframe(spark, config['input_path'], DATA_SCHEMA)

        timestamp_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
        output_parquet_path = f"{config['output_base_path'].rstrip('/')}/{OUTPUT_FORMAT}_{timestamp_suffix}"

        write_duration = convert_and_upload_parquet(spark, input_df, output_parquet_path, OUTPUT_PARTITIONS)

        total_job_duration = read_duration + write_duration
        endpoint_result = write_duration

    except Exception:
        endpoint_result = "FAILED"
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                pass
        spark = None

    all_results[endpoint_name] = endpoint_result

overall_end_time = time.perf_counter()
