import boto3
import os
import time
import math
import concurrent.futures
from botocore.exceptions import ClientError
from botocore.config import Config
import tempfile
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

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

FILE_SIZE_KB = 1024
MAX_PARALLELISM = 100
PARALLELISM_LEVELS = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

RUN_ID = int(time.time())
MINIO_BUCKET_NAME = f"minio-par-upload-dyn-{RUN_ID}"
STORJ_BUCKET_NAME = f"storj-par-upload-dyn-{RUN_ID}"

CLIENT_CONFIG = Config(connect_timeout=60, read_timeout=120)

def generate_random_data(size_in_bytes):
    return os.urandom(size_in_bytes)

def create_temp_file(directory, file_id, size_kb):
    file_path = os.path.join(directory, f"run_data_{file_id:04d}.dat")
    size_bytes = size_kb * 1024
    data = generate_random_data(size_bytes)
    try:
        with open(file_path, 'wb') as f:
            f.write(data)
        return file_path
    except IOError:
        return None

def upload_single_file(client, bucket_name, local_file_path, object_key):
    try:
        client.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=object_key)
        return True, object_key
    except Exception:
        return False, object_key

def run_parallel_upload_test(service_name, client, bucket_name, local_file_paths, num_workers):
    start_time = time.perf_counter()
    success_count = 0
    fail_count = 0

    object_keys = [f"run_{RUN_ID}/workers_{num_workers}/file_{i:04d}.dat"
                   for i, fp in enumerate(local_file_paths)]

    tasks = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        for i, file_path in enumerate(local_file_paths):
            tasks.append(executor.submit(upload_single_file, client, bucket_name, file_path, object_keys[i]))

        for future in as_completed(tasks):
            try:
                success, _ = future.result()
                if success:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception:
                 fail_count += 1

    end_time = time.perf_counter()
    duration = end_time - start_time
    return duration, success_count, fail_count

def cleanup_remote_objects(client, bucket_name):
    obj_count = 0
    deleted_count = 0
    error_count = 0
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)

        objects_to_delete = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})
                    obj_count +=1
                    if len(objects_to_delete) >= 1000:
                        del_response = client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete, 'Quiet': True})
                        deleted_count += len(objects_to_delete)
                        if 'Errors' in del_response and del_response['Errors']:
                             error_count += len(del_response['Errors'])
                             deleted_count -= error_count
                        objects_to_delete = []

        if objects_to_delete:
            del_response = client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete, 'Quiet': True})
            deleted_count += len(objects_to_delete)
            if 'Errors' in del_response and del_response['Errors']:
                 error_count += len(del_response['Errors'])
                 deleted_count -= error_count

    except client.exceptions.NoSuchBucket:
        pass
    except Exception:
        pass

def main():
    all_results = []
    minio_client = None
    storj_client = None

    try:
        minio_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY, region_name=MINIO_REGION, use_ssl=MINIO_ENDPOINT.startswith('https'), config=CLIENT_CONFIG)
    except Exception: pass

    try:
        storj_client = boto3.client('s3', endpoint_url=STORJ_ENDPOINT, aws_access_key_id=STORJ_ACCESS_KEY, aws_secret_access_key=STORJ_SECRET_KEY, region_name=STORJ_REGION, use_ssl=STORJ_ENDPOINT.startswith('https'), config=CLIENT_CONFIG)
    except Exception: pass

    if minio_client:
        try:
            loc = {} if MINIO_REGION == 'us-east-1' else {'LocationConstraint': MINIO_REGION}
            cfg = {'CreateBucketConfiguration': loc} if loc else {}
            minio_client.create_bucket(Bucket=MINIO_BUCKET_NAME, **cfg)
            time.sleep(1)
        except minio_client.exceptions.BucketAlreadyOwnedByYou: pass
        except Exception: minio_client = None

    if storj_client:
        try:
            loc = {} if STORJ_REGION == 'us-east-1' else {'LocationConstraint': STORJ_REGION}
            cfg = {'CreateBucketConfiguration': loc} if loc else {}
            storj_client.create_bucket(Bucket=STORJ_BUCKET_NAME, **cfg)
            time.sleep(1)
        except storj_client.exceptions.BucketAlreadyOwnedByYou: pass
        except Exception: storj_client = None

    valid_parallelism_levels = [p for p in PARALLELISM_LEVELS if 1 <= p <= MAX_PARALLELISM]

    for workers in valid_parallelism_levels:
        with tempfile.TemporaryDirectory() as temp_dir:
            current_run_files = []
            for i in range(workers):
                fp = create_temp_file(temp_dir, i, FILE_SIZE_KB)
                if fp:
                    current_run_files.append(fp)

            if len(current_run_files) != workers:
                continue

            if minio_client:
                duration, success, failed = run_parallel_upload_test(
                    "MinIO", minio_client, MINIO_BUCKET_NAME, current_run_files, workers
                )
                all_results.append({
                    "provider": "minio",
                    "workers": workers,
                    "duration_s": duration,
                    "success": success,
                    "failed": failed,
                    "files_tested": len(current_run_files),
                    "file_size_kb": FILE_SIZE_KB
                })
                time.sleep(0.5)

            if storj_client:
                duration, success, failed = run_parallel_upload_test(
                    "Storj", storj_client, STORJ_BUCKET_NAME, current_run_files, workers
                )
                all_results.append({
                    "provider": "storj",
                    "workers": workers,
                    "duration_s": duration,
                    "success": success,
                    "failed": failed,
                    "files_tested": len(current_run_files),
                    "file_size_kb": FILE_SIZE_KB
                })
                time.sleep(0.5)

    if minio_client:
        cleanup_remote_objects(minio_client, MINIO_BUCKET_NAME)
        try:
            minio_client.delete_bucket(Bucket=MINIO_BUCKET_NAME)
        except Exception: pass

    if storj_client:
        cleanup_remote_objects(storj_client, STORJ_BUCKET_NAME)
        try:
            storj_client.delete_bucket(Bucket=STORJ_BUCKET_NAME)
        except Exception: pass

if __name__ == "__main__":
    main()
