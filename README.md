# zarr_stores

## This repository offers alternative zarr compatible storage classes

**Nota Bene:**  <u>These storage classes are not endorsed, developed or maintained by the Zarr Developers</u>



##### <u>H5_Nested_Store:</u>

```python
## Import
from zarr_stores.h5_nested_store import H5_Nested_Store
```

###### Description:

The H5_Nested_Store enables zarr chunks are sharded across HDF5 containers located at a single dimension.  By default, HDF5 files are located at dimension -3 (consolidate_depth=3), but this can be changed to any dimension. By default, all chunks are written directly to HDF5 files and this is <u>not</u> process or thread safe for write operations. If dask distributed is installed, distributed_lock=True will use a local distributed cluster to manage locks for reads and writes which make the store both process and thread safe for highly parallel operations. There is no need for locking if the array is read only.

```python
from zarr_stores.h5_nested_store import H5_Nested_Store
path = '/my/store/path'
store = H5_Nested_Store(path)
zarray = zarr.zeros((50,50,50), chunks=(5,5,5), dtype='uint16', store=store)
zarray[:] = 4
```



The H5_Nested_Store is designed to be fully compatible with zarr.storage.NestedDirectoryStore while enabling sharding of chunks across a single dimension.  A store created using zarr.storage.NestedDirectoryStore can be read using H5_Nested_Store and initializing a H5_Nested_Store with 'write_direct=True' will make it behave exactly as a zarr.storage.NestedDirectoryStore.

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
znds = zarr.zeros((50,50,50), chunks=(5,5,5), dtype='uint16', store=ndsstore)
znds[:] = 4

# Make H5_Nested_Store with 'write_direct=False' (effectively becomes NestedDirectoryStore)
from zarr_stores.h5_nested_store import H5_Nested_Store
hnspath = '/my/store/hns'
hnsstore = H5_Nested_Store(hnspath, write_direct=False)
zhns = zarr.zeros((50,50,50), chunks=(5,5,5), dtype='uint16', store=hnsstore)
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

