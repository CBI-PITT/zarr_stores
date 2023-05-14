"""
Example of how to migrate a zarr store on s3 to local H5_Nested_Store.
This code copys key-by-key from s3 store directly into a local H5_Nested_Store
Each key is verified after it is written to disk
If an error occurs, it is logged in the object 'errors'

Requires: s3fs (pip install s3fs)

Assumes: 
Annonmous credentials to the s3 store
Parallel copy using every available core
Verifies that each chunk is recorded without error
"""

import s3fs
import zarr
from zarr_stores.h5_nested_store import H5_Nested_Store
import numpy as np
import os
import dask
from dask import delayed
import time
from multiprocessing.pool import ThreadPool


start = time.time()

remote_bucket = 's3://my-bucket/my-store-path'

local_store = '/location/of/local/store'


# Get remote store
s3 = s3fs.S3FileSystem(anon=True)
remote_store = s3fs.S3Map(root=remote_bucket, s3=s3, check=False)

# Get local store
# local_store = H5_Nested_Store(local_store)



def copy_zarr_chunks(chunk_list,remote_s3_bucket,local_store):
    
    # Get remote store
    s3 = s3fs.S3FileSystem(anon=True)
    remote_store = s3fs.S3Map(root=remote_s3_bucket, s3=s3, check=False)
    
    # Get local store
    local_store = H5_Nested_Store(local_store)
    
    error_keys = []
    max_tries = 3
    for rk in chunk_list:
        print(f'Copying {rk}')
        tries = 0
        while True:
            try:
                remote_data = remote_store[rk]
                local_store[rk] = remote_data
            except:
                print(f'Failed Read/Write: {rk}')
            
            try:
                if remote_data == local_store[rk]:
                    break
            except:
                print(f'Failed Verification: {rk}')
                pass
            
            tries += 1
            if tries == max_tries:
                error_keys.append(rk)
                break
    
    return error_keys




# Get all keys (this can take awhile for a large store)
print('Collecting keys from remote store')
keys = tuple(remote_store.keys())

# Identify location of arrays
arrays = [x for x in keys if '.zarray' in x]
array_roots = [os.path.split(x)[0] + '/' for x in keys if '.zarray' in x]

# Sort all keys associated with each shard
# Threads handle 1 shard (h5 file) only to avoid the need for locks

depth = 3
array_keys = {}
file_keys = []
for key in keys:
    is_array = any([key.startswith(x) for x in array_roots])
    if not is_array:
        file_keys.append(key)
        continue
    
    array_root = [x for x in array_roots if key.startswith(x)][0]
    # for array_root in array_roots:
    print(key)
    test_key = key.replace(array_root,'',1)
    # print(test_key)
    parts = test_key.split('/')
    
    if len(parts) >= depth:
        shard_key = parts[-depth]
        if shard_key not in array_keys:
            array_keys[shard_key] = []
        array_keys[shard_key].append(key)
    else:
        file_keys.append(key)

array_keys['files'] = file_keys

# Queue all shards for copy (keys are group according to their respective shards)
to_run = []
for key in array_keys:
    tmp = delayed(copy_zarr_chunks)(array_keys[key],remote_bucket,local_store)
    to_run.append(tmp)

# Compute copy of each shard
dask.config.set(pool=ThreadPool(os.cpu_count()))
errors = dask.compute(to_run)[0]
errors = [item for sublist in errors for item in sublist]

total_time = time.time() - start
print(f'Time in hours: {total_time/60/60}')











