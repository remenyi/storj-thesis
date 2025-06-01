import boto3
import os
import time
import math
import concurrent.futures
from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.s3.transfer import TransferConfig # Added for clarity with Config usage
import statistics
import sys

MINIO_ENDPOINT = "http://your-minio-host:9001"
MINIO_ACCESS_KEY = "YOUR_MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "YOUR_MINIO_SECRET_KEY"
MINIO_REGION = 'us-east-1'
MINIO_BUCKET = 'test'

STORJ_ENDPOINT = "http://your-storj-host:80"
STORJ_ACCESS_KEY = "YOUR_STORJ_ACCESS_KEY"
STORJ_SECRET_KEY = "YOUR_STORJ_SECRET_KEY"
STORJ_REGION = 'us-east-1'
STORJ_BUCKET = 'test'

NUM_REPETITIONS = 10
local_file_path = 'large_test_file.bin'
file_size_mib = 250
base_object_key = 'test_upload_object.bin'

chunk_size_mib_1 = 32
chunk_size_mib_2 = 64
MAX_PARALLEL_UPLOADS = 10

file_size_bytes = file_size_mib * 1024 * 1024
chunk_size_bytes_1 = chunk_size_mib_1 * 1024 * 1024
chunk_size_bytes_2 = chunk_size_mib_2 * 1024 * 1024

def create_test_file(filepath, size_bytes):
    try:
        with open(filepath, 'wb') as f:
             f.seek(size_bytes - 1)
             f.write(b"\0")
        return True
    except IOError:
        return False

def get_s3_client(endpoint_url, access_key, secret_key, region):
    try:
        s3_config = Config(signature_version='s3v4')
        client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=s3_config
        )
        client.list_buckets()
        return client
    except Exception:
        return None

def delete_s3_object(s3_client, bucket_name, object_key):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    except ClientError:
        pass
    except Exception:
        pass

def upload_single_part(s3_client, bucket_name, object_key, local_file_path):
    start_time = time.perf_counter()
    duration = None
    try:
        with open(local_file_path, 'rb') as f:
            s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=f)
        end_time = time.perf_counter()
        duration = end_time - start_time
        delete_s3_object(s3_client, bucket_name, object_key)
    except Exception:
        duration = None
    return duration

def _upload_part_task(s3_client, bucket_name, object_key, upload_id, part_number, chunk):
    try:
        response = s3_client.upload_part(
            Bucket=bucket_name, Key=object_key, PartNumber=part_number,
            UploadId=upload_id, Body=chunk
        )
        return {'PartNumber': part_number, 'ETag': response['ETag']}
    except Exception:
        raise

def upload_multipart_parallel(s3_client, bucket_name, object_key, local_file_path, chunk_size_bytes, max_workers):
    start_time = time.perf_counter()
    upload_id = None
    parts = []
    futures = []
    success = True
    upload_completed = False

    try:
        mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
        upload_id = mpu['UploadId']

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            part_number = 1
            with open(local_file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size_bytes)
                    if not chunk:
                        break
                    future = executor.submit(
                        _upload_part_task, s3_client, bucket_name, object_key,
                        upload_id, part_number, chunk
                    )
                    futures.append(future)
                    part_number += 1

            for future in concurrent.futures.as_completed(futures):
                try:
                    part_info = future.result()
                    parts.append(part_info)
                except Exception:
                    success = False

        if not success:
             raise Exception()

        parts.sort(key=lambda item: item['PartNumber'])
        s3_client.complete_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        upload_completed = True
        end_time = time.perf_counter()
        duration = end_time - start_time
        delete_s3_object(s3_client, bucket_name, object_key)
        return duration

    except Exception:
        if upload_id and not upload_completed:
             try:
                 s3_client.abort_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id)
             except ClientError:
                 pass
        return None

def calculate_speed(file_size_mib, duration_seconds):
    if duration_seconds is None or duration_seconds <= 0:
        return 0.0
    return file_size_mib / duration_seconds

def get_median_from_list(data_list):
    valid_data = [d for d in data_list if d is not None and d > 0]
    if not valid_data:
        return None
    return statistics.median(valid_data)

all_results = {}

if not create_test_file(local_file_path, file_size_bytes):
    sys.exit()

try:
    minio_client = get_s3_client(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_REGION)
    storj_client = get_s3_client(STORJ_ENDPOINT, STORJ_ACCESS_KEY, STORJ_SECRET_KEY, STORJ_REGION)

    endpoints_to_test = []
    if minio_client:
        endpoints_to_test.append({"name": "MinIO", "client": minio_client, "bucket": MINIO_BUCKET})
    if storj_client:
         endpoints_to_test.append({"name": "Storj", "client": storj_client, "bucket": STORJ_BUCKET})

    if not endpoints_to_test:
        sys.exit()

    for endpoint in endpoints_to_test:
        name = endpoint["name"]
        all_results[name] = {
            'single_part': [],
            f'multipart_{chunk_size_mib_1}mib': [],
            f'multipart_{chunk_size_mib_2}mib': []
        }

    for i in range(NUM_REPETITIONS):
        for endpoint in endpoints_to_test:
            name = endpoint["name"]
            client = endpoint["client"]
            bucket = endpoint["bucket"]

            key_single = f"run{i+1}_single_{base_object_key}"
            key_multi_1 = f"run{i+1}_multi_{chunk_size_mib_1}mib_{base_object_key}"
            key_multi_2 = f"run{i+1}_multi_{chunk_size_mib_2}mib_{base_object_key}"

            duration_single = upload_single_part(client, bucket, key_single, local_file_path)
            all_results[name]['single_part'].append(duration_single)

            duration_multi_1 = upload_multipart_parallel(client, bucket, key_multi_1, local_file_path, chunk_size_bytes_1, MAX_PARALLEL_UPLOADS)
            all_results[name][f'multipart_{chunk_size_mib_1}mib'].append(duration_multi_1)

            duration_multi_2 = upload_multipart_parallel(client, bucket, key_multi_2, local_file_path, chunk_size_bytes_2, MAX_PARALLEL_UPLOADS)
            all_results[name][f'multipart_{chunk_size_mib_2}mib'].append(duration_multi_2)

finally:
    if os.path.exists(local_file_path):
        try:
            os.remove(local_file_path)
        except OSError:
            pass
