# zarr_stores

## This repository offers alternative zarr compatible storage classes

**Nota Bene:**  <u>These storage classes are not endorsed, developed or maintained by the Zarr Developers</u>. In addition, these storage classes have not been vetted or approved by the broader Zarr community. The storage classes deposited herein have been created to meet the specific needs of several ongoing projects. Every effort has been made to ensure that they are drop-in compatible with the current Zarr standards, but we cannot guarantee this. Use at your own risk.

#### Get involved:

If you identify a bug or have a suggestion please [open an issue.](https://github.com/CBI-PITT/zarr_stores/issues)

If you wish to submit a pull request, [do so here](https://github.com/CBI-PITT/zarr_stores/pulls).

If these storage classes have been helpful to you, please [let us know](mailto:alan.watson@pitt.edu).



##### <u>H5_Nested_Store:</u>

```python
## Import
from zarr_stores.h5_nested_store import H5_Nested_Store
```

###### Description:

The H5_Nested_Store enables zarr chunks to be sharded across HDF5 containers located at a single dimension.  By default, HDF5 files are located at dimension -3 (consolidate_depth=3), but this can be changed to any dimension. By default, all chunks are written directly to HDF5 files and this is <u>not</u> process or thread safe for write operations. If dask distributed is installed and the store is initialized with option distributed_lock=True H5_Nested_Store will use a local distributed cluster to manage locks for reads and writes which make the store both process and thread safe for highly parallel operations. There is no need for locking if the array is read only.

```python
from zarr_stores.h5_nested_store import H5_Nested_Store
path = '/my/store/path'
store = H5_Nested_Store(path)
zarray = zarr.zeros((10,10,10), chunks=(5,5,5), dtype='uint16', store=store)
zarray[:] = 4
```



The H5_Nested_Store is designed to be fully compatible with zarr.storage.NestedDirectoryStore.  A store created using zarr.storage.NestedDirectoryStore can be read and written to directly with H5_Nested_Store. Initializing a new H5_Nested_Store with option 'write_direct=False' will make it behave exactly like a zarr.storage.NestedDirectoryStore.

```python
'''
Example showing how H5_Nested_Store can mimic zarr.storage.NestedDirectoryStore
by using the write_direct=False when initializing the store
'''

import numpy as np
import zarr

# Make NestedDirectoryStore
ndspath = '/my/store/nds'
ndsstore = zarr.storage.NestedDirectoryStore(ndspath)
znds = zarr.zeros((10,10,10), chunks=(5,5,5), dtype='uint16', store=ndsstore)
znds[:] = 4

# Make H5_Nested_Store with 'write_direct=False' (effectively becomes NestedDirectoryStore)
from zarr_stores.h5_nested_store import H5_Nested_Store
hnspath = '/my/store/hns'
hnsstore = H5_Nested_Store(hnspath, write_direct=False)
zhns = zarr.zeros((10,10,10), chunks=(5,5,5), dtype='uint16', store=hnsstore)
zhns[:] = 4

# They are the same array
np.ndarray.all(znds[:] == zhns[:]) 
#True

# They have exactly the same file structure
import glob
import os
ndsglob =  glob.glob(os.path.join(ndspath,'**'), recursive=True)
ndsglob = [os.path.relpath(x,ndspath) for x in ndsglob]

hnsglob =  glob.glob(os.path.join(hnspath,'**'), recursive=True)
hnsglob = [os.path.relpath(x,hnspath) for x in hnsglob]

all([x==y for x,y in zip(ndsglob, hnsglob)])
#True

# The zarr.storage.NestedDirectoryStore can be read directly with H5_Nested_Store
hns_from_nds = H5_Nested_Store(ndspath)
z_hns_from_nds = zarr.open(hns_from_nds)
np.ndarray.all(znds[:] == z_hns_from_nds[:])
#True
```



###### Convert a NestedDirectoryStore into a sharded H5_Nested_Store:

At any point, a NestedDirectoryStore can be converted into a sharded H5_Nested_Store by calling a simple consolidate() function built into the storage class. Chunks are moved into HDF5 files 1 at a time and the original chunk is deleted after the move. There is very little storage overhead ~= (maximum_chunk_size * num_threads), making it possible to convert an extremely large NestedDirectoryStore to H5_Nested_Store in place when storage space is limited. By default, dask is used to do the conversion in parallel, but if dask is not available it will default to single threaded operation. This process is expected to be safely done in parallel, as each shard is managed by only 1 thread to exclude the possibility of conflicts.

```python
hns_from_nds.consolidate()
# Moving chunk files into /my/store/nds/0.h5
# Moving chunk files into /my/store/nds/1.h5
# Removing Empty Dir /my/store/nds/0/0
# Removing Empty Dir /my/store/nds/0/1
# Removing Empty Dir /my/store/nds/1/0
# Removing Empty Dir /my/store/nds/1/1
# Removing Empty Dir /my/store/nds/0
# Removing Empty Dir /my/store/nds/1

# All future chunks written to the array will be written directly to .h5 files, because it was mounted as write_direct=True (default).
```

KNOWN ISSUES with H5_Nested_Store:

The consolidate() method will not work for stores with nested groups of arrays. The consolidate() method should only be called on arrays that are on the root path of the H5_Nested_Store.

```python
hnspath = '/my/store/test_hns_with_groups'
hnsstore = H5_Nested_Store(hnspath, write_direct=False)
g = zarr.group(store=hnsstore, overwrite=True)
g.create_groups('test_group','test_group2')
test_group = g.test_group
array = test_group.create_dataset('array',shape=(10,10,10),chunks=(5,5,5),dtype='uint16')
array[:] = 4

# DO NOT - YOU WILL CORRUPT YOUR STORE - DO NOT
# hnsstore.consolidate()  # DO NOT

# DO
hns_root_array = '/my/store/test_hns_with_groups/test_group/array'
hns_root_array_store = H5_Nested_Store(hns_root_array)
hns_root_array_store.consolidate()

zarray = zarr.open(hns_root_array_store)
nd.ndarray.all(zarray[:] == 4)
# True
```



##### <u>Archived_Nested_Store:</u>

###### Description:

Similar to H5_Nested_Store, this storage class uses zip files to store shards.  The storage class is designed to operate in a similar manner as the H5_Nested_Store but is less well developed and documented and <u>is not recommended for production use</u>. There are several disadvantages to using zip files to store chunks: 1) there is an expectation of lower performance comparted to HDF5 and 2) chunks cannot be directly replaced inside of a zip file without appending data to the zip file - making it inefficient for storing arrays that require multiple writes. This storage class should be used for archival purposes. For example, only write once to the store or operate on a NestedDirectoryStore prior to calling the consolidate() method to convert to the sharded representation. In general, it is recommended to use H5_Nested_Store instead.
