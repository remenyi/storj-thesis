import boto3
import time
import os
import io
import random
import string
import pandas as pd
from botocore.client import Config
import logging
import hashlib
import base64

MINIO_ENDPOINT = "http://your-minio-host:9001"
MINIO_ACCESS_KEY = "YOUR_MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "YOUR_MINIO_SECRET_KEY"
MINIO_REGION = 'us-east-1'

STORJ_ENDPOINT = "http://your-storj-host:80"
STORJ_ACCESS_KEY = "YOUR_STORJ_ACCESS_KEY"
STORJ_SECRET_KEY = "YOUR_STORJ_SECRET_KEY"
STORJ_REGION = 'us-east-1'

NUM_ITERATIONS = 10
SMALL_FILE_SIZE_KB = 128
LARGE_FILE_SIZE_KB = 102400
NUM_LIST_OBJECTS = 20

def generate_random_data(size_in_bytes):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_in_bytes)).encode('utf-8')

def add_content_md5_header(**kwargs):
    if 'request' not in kwargs:
        return
    request = kwargs['request']
    if not hasattr(request, 'body') or not hasattr(request, 'headers'):
        return

    if request.body:
        body_bytes = request.body
        if isinstance(body_bytes, str):
            try:
                body_bytes = body_bytes.encode('utf-8')
                request.body = body_bytes
            except Exception:
                 return
        elif not isinstance(body_bytes, bytes):
             return

        try:
            md5_hash = hashlib.md5(body_bytes).digest()
            md5_base64 = base64.b64encode(md5_hash).decode('utf-8')
            request.headers['Content-MD5'] = md5_base64
        except Exception:
             pass

def test_copy_object(client, source_bucket, source_key, dest_bucket, dest_key):
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    start_time = time.time()
    client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
    return time.time() - start_time

def test_head_object(client, bucket_name, key_name):
    start_time = time.time()
    client.head_object(Bucket=bucket_name, Key=key_name)
    return time.time() - start_time

def test_list_objects(client, bucket_name, prefix):
    start_time = time.time()
    client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=NUM_LIST_OBJECTS + 5)
    return time.time() - start_time

def test_delete_object(client, bucket_name, key_name):
    start_time = time.time()
    client.delete_object(Bucket=bucket_name, Key=key_name)
    return time.time() - start_time

def test_delete_objects_batch(client, bucket_name, keys_to_delete):
    if not keys_to_delete:
        return 0.0

    objects_to_delete_payload = [{'Key': key} for key in keys_to_delete]
    delete_request_payload = {'Objects': objects_to_delete_payload, 'Quiet': True}

    start_time = time.time()
    response = client.delete_objects(Bucket=bucket_name, Delete=delete_request_payload)
    elapsed = time.time() - start_time

    if 'Errors' in response and response['Errors']:
        pass # Handle errors silently or log to file

    return elapsed

results_list = []
results_df = pd.DataFrame()

run_id = int(time.time())
minio_bucket_name = f"minio-jup-perf-full-{run_id}"
storj_bucket_name = f"storj-jup-perf-full-{run_id}"

client_config = Config(connect_timeout=90, read_timeout=180)

try:
    minio_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY, region_name=MINIO_REGION, use_ssl=MINIO_ENDPOINT.startswith('https'), config=client_config)
except Exception:
    minio_client = None

try:
    storj_client = boto3.client('s3', endpoint_url=STORJ_ENDPOINT, aws_access_key_id=STORJ_ACCESS_KEY, aws_secret_access_key=STORJ_SECRET_KEY, region_name=STORJ_REGION, use_ssl=STORJ_ENDPOINT.startswith('https'), config=client_config)
    try:
        storj_event_emitter = storj_client.meta.events
        storj_event_emitter.register_first('before-sign.s3.DeleteObjects', add_content_md5_header)
    except Exception:
        pass
except Exception:
    storj_client = None


if minio_client:
    try:
        loc_constraint = {} if MINIO_REGION == 'us-east-1' else {'LocationConstraint': MINIO_REGION}
        if loc_constraint:
            minio_client.create_bucket(Bucket=minio_bucket_name, CreateBucketConfiguration=loc_constraint)
        else:
            minio_client.create_bucket(Bucket=minio_bucket_name)
        time.sleep(2)
    except Exception:
        minio_client = None

if storj_client:
    try:
        loc_constraint = {} if STORJ_REGION == 'us-east-1' else {'LocationConstraint': STORJ_REGION}
        if loc_constraint:
            storj_client.create_bucket(Bucket=storj_bucket_name, CreateBucketConfiguration=loc_constraint)
        else:
            storj_client.create_bucket(Bucket=storj_bucket_name)
        time.sleep(2)
    except Exception:
        storj_client = None

if not (minio_client or storj_client):
     exit()

small_file_data = generate_random_data(SMALL_FILE_SIZE_KB * 1024)
large_file_data = generate_random_data(LARGE_FILE_SIZE_KB * 1024)
list_obj_data = b"list_data"

providers = {}
if minio_client:
    providers["minio"] = {"client": minio_client, "bucket": minio_bucket_name}
if storj_client:
    providers["storj"] = {"client": storj_client, "bucket": storj_bucket_name}

test_run_prefix = f"jup-perf-test-{run_id}/"

source_small_file_key = f"{test_run_prefix}source_small_file.bin"
source_large_file_key = f"{test_run_prefix}source_large_file.bin"

for provider_name, config in providers.items():
    client = config["client"]
    bucket = config["bucket"]

    try:
        client.put_object(Bucket=bucket, Key=source_small_file_key, Body=io.BytesIO(small_file_data))
        client.put_object(Bucket=bucket, Key=source_large_file_key, Body=io.BytesIO(large_file_data))
    except Exception:
        continue

    list_batch_delete_keys = []

    for i in range(NUM_ITERATIONS):

        list_batch_delete_keys = [f"{test_run_prefix}list_del_obj_iter{i}_{j:03d}.txt" for j in range(NUM_LIST_OBJECTS)]
        creation_successful = True
        for key in list_batch_delete_keys:
             try:
                 client.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(list_obj_data))
             except Exception:
                 creation_successful = False
                 break
        if not creation_successful:
             list_batch_delete_keys = []
        else:
             time.sleep(1)

        dest_small_file_key = f"{test_run_prefix}small_file_copy_iter{i}.bin"
        dest_large_file_key = f"{test_run_prefix}large_file_copy_iter{i}.bin"

        try:
            elapsed = test_head_object(client, bucket, source_small_file_key)
            results_list.append({"provider": provider_name, "operation": "head", "size_kb": SMALL_FILE_SIZE_KB, "iteration": i+1, "time_sec": elapsed})
        except Exception as e:
            results_list.append({"provider": provider_name, "operation": "head", "size_kb": SMALL_FILE_SIZE_KB, "iteration": i+1, "time_sec": -1.0, "error": str(e)})

        try:
            elapsed = test_head_object(client, bucket, source_large_file_key)
            results_list.append({"provider": provider_name, "operation": "head", "size_kb": LARGE_FILE_SIZE_KB, "iteration": i+1, "time_sec": elapsed})
        except Exception as e:
            results_list.append({"provider": provider_name, "operation": "head", "size_kb": LARGE_FILE_SIZE_KB, "iteration": i+1, "time_sec": -1.0, "error": str(e)})

        try:
            elapsed = test_copy_object(client, bucket, source_small_file_key, bucket, dest_small_file_key)
            results_list.append({"provider": provider_name, "operation": "copy", "size_kb": SMALL_FILE_SIZE_KB, "iteration": i+1, "time_sec": elapsed})
        except Exception as e:
            results_list.append({"provider": provider_name, "operation": "copy", "size_kb": SMALL_FILE_SIZE_KB, "iteration": i+1, "time_sec": -1.0, "error": str(e)})
            dest_small_file_key = None

        try:
            elapsed = test_copy_object(client, bucket, source_large_file_key, bucket, dest_large_file_key)
            results_list.append({"provider": provider_name, "operation": "copy", "size_kb": LARGE_FILE_SIZE_KB, "iteration": i+1, "time_sec": elapsed})
        except Exception as e:
            results_list.append({"provider": provider_name, "operation": "copy", "size_kb": LARGE_FILE_SIZE_KB, "iteration": i+1, "time_sec": -1.0, "error": str(e)})
            dest_large_file_key = None

        if list_batch_delete_keys:
            try:
                elapsed = test_list_objects(client, bucket, f"{test_run_prefix}list_del_obj_iter{i}_")
                results_list.append({"provider": provider_name, "operation": "list", "num_objects": len(list_batch_delete_keys), "iteration": i+1, "time_sec": elapsed})
            except Exception as e:
                results_list.append({"provider": provider_name, "operation": "list", "num_objects": len(list_batch_delete_keys), "iteration": i+1, "time_sec": -1.0, "error": str(e)})

        if dest_small_file_key:
            try:
                elapsed = test_delete_object(client, bucket, dest_small_file_key)
                results_list.append({"provider": provider_name, "operation": "delete_one", "size_kb": SMALL_FILE_SIZE_KB, "iteration": i+1, "time_sec": elapsed})
            except Exception:
                pass

        if dest_large_file_key:
            try:
                test_delete_object(client, bucket, dest_large_file_key)
            except Exception:
                pass

        if list_batch_delete_keys:
            try:
                elapsed = test_delete_objects_batch(client, bucket, list_batch_delete_keys)
                if elapsed >= 0:
                    results_list.append({"provider": provider_name, "operation": "delete_batch", "num_objects": len(list_batch_delete_keys), "iteration": i+1, "time_sec": elapsed})
                else:
                     results_list.append({"provider": provider_name, "operation": "delete_batch", "num_objects": len(list_batch_delete_keys), "iteration": i+1, "time_sec": -1.0, "error": "Batch delete function indicated an error"})

                list_batch_delete_keys = []

            except Exception as e:
                results_list.append({"provider": provider_name, "operation": "delete_batch", "num_objects": len(list_batch_delete_keys), "iteration": i+1, "time_sec": -1.0, "error": str(e)})


if results_list:
    results_df = pd.DataFrame(results_list)
    try:
        from IPython.display import display
        display(results_df)
    except ImportError:
        pass


if storj_client and 'storj_event_emitter' in locals():
     try:
         storj_event_emitter.unregister('before-sign.s3.DeleteObjects', add_content_md5_header)
     except Exception:
         pass


for provider_name, config in providers.items():
    client = config["client"]
    bucket = config["bucket"]

    all_object_keys = []
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket)
        for page in pages:
            if 'Contents' in page:
                all_object_keys.extend([obj['Key'] for obj in page['Contents']])
    except client.exceptions.NoSuchBucket:
         pass
    except Exception:
        pass

    if all_object_keys:
        deleted_count = 0
        error_count = 0
        for key in all_object_keys:
            try:
                client.delete_object(Bucket=bucket, Key=key)
                deleted_count += 1
            except Exception:
                error_count += 1

    try:
        client.delete_bucket(Bucket=bucket)
    except client.exceptions.NoSuchBucket:
         pass
    except Exception:
        pass
