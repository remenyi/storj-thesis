import boto3
import time
import os
import random
import string
import pandas as pd
from botocore.client import Config
from boto3.s3.transfer import TransferConfig
import botocore.exceptions
import tempfile
import sys
import math

MINIO_ENDPOINT = "http://your-minio-host:9001"
MINIO_ACCESS_KEY = "YOUR_MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "YOUR_MINIO_SECRET_KEY"
MINIO_REGION = 'us-east-1'

STORJ_ENDPOINT = "http://your-storj-host:80"
STORJ_ACCESS_KEY = "YOUR_STORJ_ACCESS_KEY"
STORJ_SECRET_KEY = "YOUR_STORJ_SECRET_KEY"
STORJ_REGION = 'us-east-1'

FILE_SIZES_MB = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, \
                 80, 85, 90, 95, 100, 105, 110, 115, 120, 125, 130, 135, 140, 145, \
                 150, 155, 160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 210, 215, \
                 220, 225, 230, 240, 245, 250]

RUN_ID = int(time.time())
MINIO_BUCKET_NAME = f"minio-speed-test-{RUN_ID}"
STORJ_BUCKET_NAME = f"storj-speed-test-{RUN_ID}"

CLIENT_CONFIG = Config(connect_timeout=60, read_timeout=300)

CHUNK_SIZE = 64 * 1024 * 1024
transferConfig = TransferConfig(
    multipart_threshold=CHUNK_SIZE,
    multipart_chunksize=CHUNK_SIZE,
)

def generate_random_data(size_bytes):
    start_gen = time.perf_counter()
    data = os.urandom(size_bytes)
    end_gen = time.perf_counter()
    return data

def create_temp_file_with_data(directory, prefix, size_mb):
    file_path = os.path.join(directory, f"{prefix}_{size_mb}MB.tmp")
    size_bytes = size_mb * 1024 * 1024
    try:
        data = generate_random_data(size_bytes)
        with open(file_path, 'wb') as f:
            f.write(data)
        del data
        actual_size = os.path.getsize(file_path)
        if actual_size != size_bytes:
             pass
        return file_path, size_bytes
    except MemoryError:
        return None, 0
    except IOError as e:
        return None, 0
    except Exception as e:
        return None, 0

def calculate_speed_mbps(size_bytes, duration_s):
    if duration_s is None or duration_s <= 0:
        return 0.0
    return (size_bytes / (1024 * 1024)) / duration_s

def cleanup_local_file(file_path):
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
        except OSError as e:
            pass

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
    except client.exceptions.NoSuchBucket: pass
    except Exception as e: pass

def main():
    all_results = []
    minio_client = None
    storj_client = None

    try:
        minio_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY, region_name=MINIO_REGION, use_ssl=MINIO_ENDPOINT.startswith('https'), config=CLIENT_CONFIG)
    except Exception as e: pass

    try:
        storj_client = boto3.client('s3', endpoint_url=STORJ_ENDPOINT, aws_access_key_id=STORJ_ACCESS_KEY, aws_secret_access_key=STORJ_SECRET_KEY, region_name=STORJ_REGION, use_ssl=STORJ_ENDPOINT.startswith('https'), config=CLIENT_CONFIG)
    except Exception as e: pass

    providers = {}
    if minio_client: providers["minio"] = {"client": minio_client, "bucket": MINIO_BUCKET_NAME}
    if storj_client: providers["storj"] = {"client": storj_client, "bucket": STORJ_BUCKET_NAME}

    if not providers:
        sys.exit(1)

    for name, config in providers.items():
        client = config["client"]
        bucket = config["bucket"]
        region = MINIO_REGION if name == "minio" else STORJ_REGION
        try:
            loc = {} if region == 'us-east-1' else {'LocationConstraint': region}
            cfg = {'CreateBucketConfiguration': loc} if loc else {}
            client.create_bucket(Bucket=bucket, **cfg)
            time.sleep(1)
        except client.exceptions.BucketAlreadyOwnedByYou: pass
        except Exception as e:
            providers[name]["client"] = None


    with tempfile.TemporaryDirectory() as temp_dir:
        for size_mb in FILE_SIZES_MB:
            local_file_path, file_size_bytes = create_temp_file_with_data(temp_dir, f"testdata_{RUN_ID}", size_mb)

            if not local_file_path:
                continue

            for provider_name, config in providers.items():
                client = config["client"]
                bucket = config["bucket"]

                if not client:
                     continue

                object_key = f"test_files/data_{size_mb}MB.bin"
                upload_duration = None
                download_duration = None
                upload_speed = 0.0
                download_speed = 0.0
                upload_error = None
                download_error = None

                try:
                    start_upload = time.perf_counter()
                    client.upload_file(Filename=local_file_path, Bucket=bucket, Key=object_key, Config=transferConfig)
                    end_upload = time.perf_counter()
                    upload_duration = end_upload - start_upload
                    upload_speed = calculate_speed_mbps(file_size_bytes, upload_duration)
                except Exception as e:
                    upload_error = str(e)
                    upload_duration = -1.0

                all_results.append({
                    "provider": provider_name,
                    "size_mb": size_mb,
                    "operation": "upload",
                    "duration_s": upload_duration if upload_duration is not None else -1.0,
                    "speed_MBps": upload_speed,
                    "error": upload_error
                })

                if upload_duration is not None and upload_duration >= 0:
                    download_file_path = local_file_path + ".downloaded"
                    try:
                        start_download = time.perf_counter()
                        client.download_file(Bucket=bucket, Key=object_key, Filename=download_file_path, Config=transferConfig)
                        end_download = time.perf_counter()
                        download_duration = end_download - start_download

                        downloaded_size = os.path.getsize(download_file_path)
                        if downloaded_size != file_size_bytes:
                             pass

                        download_speed = calculate_speed_mbps(downloaded_size, download_duration)

                    except Exception as e:
                        download_error = str(e)
                        download_duration = -1.0
                    finally:
                        cleanup_local_file(download_file_path)

                    all_results.append({
                        "provider": provider_name,
                        "size_mb": size_mb,
                        "operation": "download",
                        "duration_s": download_duration if download_duration is not None else -1.0,
                        "speed_MBps": download_speed,
                        "error": download_error
                    })
                    
                    try:
                        client.delete_object(Bucket=bucket, Key=object_key)
                    except Exception as e:
                        pass

                else:
                    all_results.append({
                        "provider": provider_name,
                        "size_mb": size_mb,
                        "operation": "download",
                        "duration_s": -1.0,
                        "speed_MBps": 0.0,
                        "error": "Skipped due to upload failure"
                    })

            cleanup_local_file(local_file_path)

    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df = results_df.sort_values(by=["size_mb", "provider", "operation"])
    else:
        pass

    for name, config in providers.items():
        client = config["client"]
        bucket = config["bucket"]
        if client:
            cleanup_remote_objects(client, bucket)
            try:
                client.delete_bucket(Bucket=bucket)
            except Exception as e: pass

if __name__ == "__main__":
    main()
