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
from numcodecs import Blosc
from itertools import product

NEW_COMPRESSION = Blosc(cname='zstd', clevel=5, shuffle=Blosc.SHUFFLE)

start = time.time()

remote_bucket = 's3://my-bucket/my-store-path'
remote_bucket = 's3://aind-open-data/exaSPIM_609281_2022-11-03_13-49-18_stitched_2022-11-22_12-07-00/fused.zarr/'

local_store = '/location/of/local/store'
local_store = r'c:\code\test_recompress'
local_store = '/CBI_Hive/globus/pitt/bil/allen/exaSPIM_609281_2022-11-03_13-49-18_stitched_2022-11-22_12-07-00/recompress'

# Get remote store
s3 = s3fs.S3FileSystem(anon=True)
remote_store = s3fs.S3Map(root=remote_bucket, s3=s3, check=False)

# Get local store
# local_store = H5_Nested_Store(local_store)



def copy_array(remote_s3_bucket,local_location,chunks,copy_index):
    # Get remote store
    s3 = s3fs.S3FileSystem(anon=True)
    array_location_remote_store = s3fs.S3Map(root=remote_s3_bucket, s3=s3, check=False)
    remote_array = zarr.open(array_location_remote_store,'r')
    # Create local array
    array_location_local_store = H5_Nested_Store(local_location)
    
    errors = []
    for idx in copy_index:
        try:
            print(f'Reading {idx}')
            remote_data = remote_array[idx]
            print(f'Writing {idx}, {remote_data.shape}')
            local_array[idx] = remote_data
        except:
            errors.append(idx)
            continue
        
        print(f'Comparing {idx}')
        if np.ndarray.all(remote_data == local_array[idx]):
            print(True)
        else:
            errors.append(idx)
            
    return errors
    
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

file_keys = []
for key in keys:
    is_array = any([key.startswith(x) for x in array_roots])
    if not is_array:
        file_keys.append(key)
        continue

print('Copying group keys')
copy_zarr_chunks(file_keys,remote_bucket,local_store)

def chunk_generator(array_shape,chunk_shape):
    ''' Returns the number of chunks in each dim'''
    chunks = (x//y for x,y in zip(array_shape,chunk_shape))
    mod = (x%y for x,y in zip(array_shape,chunk_shape))
    chunks = [c if m==0 else c+1 for c,m in zip(chunks,mod)]
    max_chunks = tuple(chunks)
    chunks = [range(x+1) for x in chunks]
    chunks = [x for x in product(*chunks)]
    return tuple(chunks), max_chunks
    
def chunk_to_slice(chunk,array_shape,chunk_shape):
    ''' Returns slice for each chunk'''
    
    chunks_start = [x*y for x,y in zip(chunk,chunk_shape)]
    chunks_stop = [x+y if x+y<z else z for x,y,z in zip(chunks_start,chunk_shape,array_shape)]
    chunk_slice = [slice(x,y) for x,y in zip(chunks_start,chunks_stop)]
    return tuple(chunk_slice)
    
    
depth = 3
for array_root in array_roots:
    # Mount remote array
    array_location_remote = remote_bucket + array_root[:-1]
    array_location_remote_store = s3fs.S3Map(root=array_location_remote, s3=s3, check=False)
    remote_array = zarr.open(array_location_remote_store,'r')
    # Create local array
    array_location_local = os.path.join(local_store,array_root)
    array_location_local_store = H5_Nested_Store(array_location_local)
    local_array = zarr.zeros(remote_array.shape,store=array_location_local_store,dtype=remote_array.dtype,chunks=remote_array.chunks, compressor=NEW_COMPRESSION)
    if len(local_array.shape) >= depth:
        chunks, max_chunks = chunk_generator(remote_array.shape,remote_array.chunks)
        to_run = []
        for c in range(max_chunks[-depth]):
            to_process = [x for x in chunks if x[-depth] == c]
            to_process = [chunk_to_slice(chunk,local_array.shape,local_array.chunks) for chunk in to_process]
            print(f'Total chunks per shard = {to_process}')
            tmp = delayed(copy_array)(array_location_remote,array_location_local,local_array.chunks,to_process)
            to_run.append(tmp)
    
    
    to_run = dask.compute(to_run)[0]
        
    
    

total_time = time.time() - start
print(f'Time in hours: {total_time/60/60}')











