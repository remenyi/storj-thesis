import boto3
import time
import os
import io
import random
import string
import pandas as pd
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig # Needed for upload_fileobj Config
import sys

MINIO_ENDPOINT = "http://your-minio-host:9001"
MINIO_ACCESS_KEY = "YOUR_MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "YOUR_MINIO_SECRET_KEY"
MINIO_REGION = 'us-east-1'

STORJ_ENDPOINT = "http://your-storj-host:80"
STORJ_ACCESS_KEY = "YOUR_STORJ_ACCESS_KEY"
STORJ_SECRET_KEY = "YOUR_STORJ_SECRET_KEY"
STORJ_REGION = 'us-east-1'

NUM_ITERATIONS = 1
TEST_OBJECT_SIZE_MB = 300
BUCKET_PREFIX = "py-range-step-test-"
OBJECT_KEY = "range-test-object-large.dat"
OUTPUT_CSV_FILE = "range_test_results_1_250mb_5mb_step.csv"

MB = 1024 * 1024
START_RANGE_MB = 1
END_RANGE_MB = 250
STEP_RANGE_MB = 5

START_RANGE_BYTES = START_RANGE_MB * MB
STOP_RANGE_BYTES = END_RANGE_MB * MB + 1
STEP_RANGE_BYTES = STEP_RANGE_MB * MB

def generate_random_suffix(length=6):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def generate_random_data(size_in_bytes):
    chunk_size = 10 * 1024 * 1024
    data_buffer = io.BytesIO()
    generated = 0
    while generated < size_in_bytes:
        bytes_to_generate = min(chunk_size, size_in_bytes - generated)
        random_bytes = bytes([random.randint(0, 255) for _ in range(bytes_to_generate)])
        data_buffer.write(random_bytes)
        generated += bytes_to_generate
    data_buffer.seek(0)
    return data_buffer

def test_range_get(client, bucket_name, key_name, start_offset, length_bytes, object_size_bytes):
    range_header = f'bytes={start_offset}-{start_offset + length_bytes - 1}'
    start_time = time.perf_counter()
    response = client.get_object(Bucket=bucket_name, Key=key_name, Range=range_header)
    body = response['Body']
    content = body.read()
    body.close()
    elapsed_ms = (time.perf_counter() - start_time) * 1000

    read_bytes = len(content)
    expected_bytes = length_bytes
    if start_offset + length_bytes > object_size_bytes:
         expected_bytes = object_size_bytes - start_offset

    if read_bytes != expected_bytes:
        if read_bytes == 0 and expected_bytes > 0:
             return -1.0
        pass

    return elapsed_ms

if __name__ == "__main__":
    results_list = []
    run_id = generate_random_suffix()
    minio_bucket_name = f"{BUCKET_PREFIX}minio-{run_id}"
    storj_bucket_name = f"{BUCKET_PREFIX}storj-{run_id}"
    TEST_OBJECT_SIZE_BYTES = TEST_OBJECT_SIZE_MB * MB

    client_config = BotoConfig(connect_timeout=60, read_timeout=180, signature_version='s3v4')
    minio_client = None
    storj_client = None

    try:
        minio_client = boto3.client(
            's3', endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION, use_ssl=MINIO_ENDPOINT.startswith('https'),
            config=client_config.merge(BotoConfig(s3={'addressing_style': 'path'}))
        )
        minio_client.list_buckets()
    except Exception:
        pass

    try:
        storj_client = boto3.client(
            's3', endpoint_url=STORJ_ENDPOINT,
            aws_access_key_id=STORJ_ACCESS_KEY, aws_secret_access_key=STORJ_SECRET_KEY,
            region_name=STORJ_REGION, use_ssl=STORJ_ENDPOINT.startswith('https'),
            config=client_config
        )
        storj_client.list_buckets()
    except Exception:
        pass

    providers = {}
    if minio_client: providers["minio"] = {"client": minio_client, "bucket": minio_bucket_name}
    if storj_client: providers["storj"] = {"client": storj_client, "bucket": storj_bucket_name}

    if not providers:
        sys.exit(1)

    setup_ok = {"minio": False, "storj": False}
    test_data_stream = None

    try:
        test_data_stream = generate_random_data(TEST_OBJECT_SIZE_BYTES)

        for provider_name, config in providers.items():
            client = config["client"]
            bucket = config["bucket"]
            region = client.meta.region_name

            try:
                loc_constraint = {} if region == 'us-east-1' else {'LocationConstraint': region}
                if loc_constraint:
                    client.create_bucket(Bucket=bucket, CreateBucketConfiguration=loc_constraint)
                else:
                    client.create_bucket(Bucket=bucket)
                time.sleep(2 if provider_name == "minio" else 5)

                transfer_config = TransferConfig(multipart_chunksize=64 * 1024 * 1024, multipart_threshold=64 * 1024 * 1024)
                client.upload_fileobj(test_data_stream, bucket, OBJECT_KEY, Config=transfer_config)
                test_data_stream.seek(0)
                setup_ok[provider_name] = True

            except Exception:
                try: client.delete_bucket(Bucket=bucket)
                except: pass

        if not any(setup_ok.values()):
            raise RuntimeError()

        start_offset = 0

        for size_bytes in range(START_RANGE_BYTES, STOP_RANGE_BYTES, STEP_RANGE_BYTES):
            size_mb = size_bytes / MB
            if start_offset + size_bytes > TEST_OBJECT_SIZE_BYTES:
                break

            for provider_name, config in providers.items():
                if not setup_ok[provider_name]:
                    continue

                client = config["client"]
                bucket = config["bucket"]

                for i in range(NUM_ITERATIONS):
                    result_data = {
                         "provider": provider_name,
                         "operation": "range_get",
                         "range_size_mb": round(size_mb, 2),
                         "range_size_bytes": size_bytes,
                         "iteration": i+1,
                         "time_ms": float('nan'),
                         "error": None
                    }
                    try:
                        elapsed_ms = test_range_get(client, bucket, OBJECT_KEY, start_offset, size_bytes, TEST_OBJECT_SIZE_BYTES)
                        if elapsed_ms >= 0:
                             result_data["time_ms"] = elapsed_ms
                        else:
                             result_data["error"] = "Measurement Failed (zero bytes?)"

                    except ClientError as e:
                        error_code = e.response['Error']['Code']
                        result_data["error"] = error_code
                    except Exception as e:
                        result_data["error"] = str(e)

                    results_list.append(result_data)
                    time.sleep(0.1)

        if results_list:
            results_df = pd.DataFrame(results_list)
            try:
                results_df.to_csv(OUTPUT_CSV_FILE, index=False)
            except Exception:
                pass

    except Exception:
        pass

    finally:
        if test_data_stream:
            test_data_stream.close()

        for provider_name, config in providers.items():
            if setup_ok.get(provider_name):
                client = config["client"]
                bucket = config["bucket"]
                try:
                    try:
                        client.delete_object(Bucket=bucket, Key=OBJECT_KEY)
                    except ClientError as e_del_obj:
                         if e_del_obj.response['Error']['Code'] != 'NoSuchKey':
                              pass # Handle other errors silently
                    except Exception:
                        pass

                    client.delete_bucket(Bucket=bucket)

                except Exception:
                    pass
