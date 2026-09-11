# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 10:29:42 2022

@author: awatson
"""

'''
A Zarr store that uses HDF5 as a containiner to shard chunks accross a single
axis.  The store is implemented similar to a directory store 
but on axis[-3] HDF5 files are written which contain
chunks cooresponding to the remainining axes.  If the shape of the 
the array is less than 3 axes, the shards will be accross axis0

Example:
    array.shape = (1,1,200,10000,10000)
    /root/of/array/.zarray
    #Sharded h5 container at axis[-3]
    /root/of/array/0/0/4.hf

    4.hf contents:
        key:value
        0.0:byte-string
        0.1:byte-string
        4.6:byte-string
        ...
        ...
'''


import os
from os import scandir
import errno
import h5py
import shutil
import time
import numpy as np
import uuid
import glob
import re

from numcodecs.abc import Codec
# ---- Zarr v3 compatibility layer ----
from zarr.abc.store import (
    Store,
    ByteRequest,
    RangeByteRequest,
    OffsetByteRequest,
    SuffixByteRequest,
)
from zarr.core.buffer import default_buffer_prototype

# Zarr v2 metadata keys (Zarr-Python 3 can still read these)
V2_ARRAY_META_KEY = ".zarray"
V3_META_KEY = "zarr.json"
V2_ZATTR_META_KEY = ".zattrs"
V2_GROUP_META_KEY = ".zgroup"

# NOTE:
# Zarr v2 chunk keys may contain '.' (e.g. "0.0") while metadata files are
# also dot-prefixed (e.g. ".zarray").  The old code used a broad
# "'.' not in key" test to decide whether something was metadata.
# That accidentally treated real v2 chunk keys as metadata and disabled
# write_direct for v2.  The helpers below replace that broad rule with
# exact metadata detection based on the basename.

def _normalize_storage_path(path: str | None) -> str:
    if path is None:
        return ""
    # Zarr uses POSIX-style paths inside stores
    p = str(path).lstrip("/").replace("\\", "/")
    return p.strip("/")

def _retry_call(func, args=(), exceptions=(PermissionError,), retries: int = 10, delay: float = 0.05):
    import time
    for i in range(retries):
        try:
            return func(*args)
        except exceptions:
            if i == retries - 1:
                raise
            time.sleep(delay)


from numcodecs.compat import (
    ensure_bytes,
    ensure_text,
    ensure_contiguous_ndarray,
    ensure_contiguous_ndarray_like
)

# from numcodecs.registry import codec_registry

# from threading import Lock, RLock
# from filelock import Timeout, FileLock, SoftFileLock



_prog_number = re.compile(r'^\d+$')

class H5_Nested_Store(Store):
    """Storage class using directories and files on a standard file system.
    Parameters
    ----------
    path : string
        Location of directory to use as the root of the storage hierarchy.
    normalize_keys : bool, optional
        If True, all store keys will be normalized to use lower case characters
        (e.g. 'foo' and 'FOO' will be treated as equivalent). This can be
        useful to avoid potential discrepancies between case-sensitive and
        case-insensitive file system. Default value is False.
    dimension_separator : {None,'/'}
        Separator placed between the dimensions of a chunk.
        '/' is the only valid separator. If None, '/' will default to '/'
        If any thing other an '/' or None then an error will be raised
    write_direct : bool
        If True chunks will be written directly to hdf5 file.
        If False store will behave like a NestedDirectoryStore,
        writing all chunks as individual files
    swmr : bool
        If True, swmr is used for writing h5 files
    container_ext : {str, '.' + str} NOT ''
        An extension is required for h5 files. This can be any string, but
        by default it is 'h5'
    distribuited_lock : bool
        If True, the store will attempt use a local dask distribuited cluster
        to coordinate distribuited locking when writing/reading file from
        h5 shards. If dask distribuited does is not installed, it will default
        to hdf5 locking implemented by h5py. In single threaded operations, this
        will not matter, but for parallel operations it may result in errors,
        freezing and potentially data loss.
        If write_direct is False, this will be forced to False
    consolidate : bool
        If True, the self.consoldate function will be called during __init__
    consolidate_depth : int
        Default 3: This determines the depth of sharding.on dimension according to
        array.shape[-consolidate_depth]
    consolidate_parallel : bool
        If True, a call to the self.consolidate function will be run in parallel
        managed by dask
    """

    def __init__(self, path, normalize_keys=False, dimension_separator='/',
                 write_direct=True, swmr=False, container_ext='h5', distribuited_lock=False,
                 consolidate=False, consolidate_depth=3, consolidate_parallel=True,
                 auto_verify_write=False, mode='a',
                #zarr_version=3
                 ):

        super().__init__(read_only=(mode=='r'))
        # guard conditions
        path = os.path.abspath(path)
        if os.path.exists(path) and not os.path.isdir(path):
            raise NotADirectoryError(path)

        self.path = os.path.normpath(path)
        self.normalize_keys = normalize_keys
        if dimension_separator is None:
            dimension_separator = "/"
        elif dimension_separator != "/":
            raise ValueError(
                "Archived_Nested_Store only supports '/' as dimension_separator")
        self._dimension_separator = dimension_separator
        self.swmr = swmr
        if container_ext[0] == '.':
            self.container_ext = container_ext
        else:
            self.container_ext = f'.{container_ext}'

        self._write_direct = write_direct
        if distribuited_lock and self._write_direct:
            try:
                from distributed import Lock, get_client, Semaphore
            except:
                import warnings
                warnings.warn("""Dask distribuited failed to import, check whether it is installed
                              Thread and Process safe locking is disabled, data
                              loss could occur in a parallel computing environment""")
                distribuited_lock = False
        else:
            distribuited_lock = False

        self.distribuited_lock = distribuited_lock

        self.auto_verify_write = auto_verify_write
        self.mode=mode
        #self.zarr_version = zarr_version
        self._setup_dist_lock()

        self._consolidate_depth = consolidate_depth
        self._consolidate = consolidate
        self._consolidate_parallel = consolidate_parallel
        if self._consolidate:
            self.consolidate()
            self._consolidate = False
        self.uuid = uuid.uuid1()

    @property
    def _arrays(self):

        if os.path.isfile(os.path.join(self.path,'.zarray')):
            yield self.path

        else:
            for root, folder, files in os.walk(self.path,topdown=False):

                for f in folder:
                    test_path = os.path.join(root,f,'.zarray')
                    if os.path.exists(test_path):
                        yield os.path.join(root,f)

    def _setup_dist_lock(self):
        self.distribuited = False
        self.dist_client = None
        if self._write_direct and self.distribuited_lock:
            from distributed import Lock, get_client, worker_client
            '''Try to get client multiple times before erroring'''
            for _ in range(10):
                self.dist_client = None
                try:
                    self.Lock = Lock
                    self.dist_client = worker_client(timeout="10s")
                    self.distribuited = True
                except:
                    self.dist_client = None
                    self.distribuited = False
                if self.dist_client is not None and self.distribuited:
                    break
            if self.dist_client is None or not self.distribuited:
                self.dist_client = None
                self.distribuited = False
                raise NotImplementedError('Distribuited lock could not be setup')
        else:
            self.dist_client = None
            self.distribuited = False

    def __del__(self):
        pass

    def __getstate__(self):
        return (self.path, self.normalize_keys, self._dimension_separator, self.swmr, self.container_ext,
                self._write_direct,self.distribuited,self.distribuited_lock, self._consolidate_depth,
                self.auto_verify_write)

    def __setstate__(self, state):
        (self.path, self.normalize_keys, self._dimension_separator, self.swmr, self.container_ext,
         self._write_direct, self.distribuited,self.distribuited_lock, self._consolidate_depth,
         self.auto_verify_write) = state

        self.uuid = uuid.uuid1()
        self._setup_dist_lock()

    def _normalize_key(self, key):
            return key.lower() if self.normalize_keys else key

    @staticmethod
    def _fromfile(fn):
        with open(fn, 'rb') as f:
            return f.read()

    def _tofile(self, a, fn):
        while True:
            with open(fn, mode='wb') as f:
                f.write(a)
            if self.auto_verify_write:
                if self._fromfile(fn) == bytes(a):
                    break
            else:
                break

    def _get_archive_key_name(self,path):
        key = ''
        for _ in range(self._consolidate_depth - 2):
            path, last = os.path.split(path)
            key = f'.{last}{key}'
        path, last = os.path.split(path)
        key = f'{last}{key}'

        archive = f'{path}{self.container_ext}'
        return archive, key

    # ---- Compatibility helpers added for Zarr v2/v3 behavior ----
    def _maybe_convert_v2_chunk_key(self, key):
        """
        Convert only v2 chunk keys from dot form to nested path form.
        v3 is unaffected because it doesn't use dot-separated chunk keys, 
        and metadata is unaffected because it must be left as ordinary files.

        Example:
            foo/0.1  -> foo/0/1

        Why this is needed:
        - zarr-python 3 exposes v2 chunk keys to the store in dotted form
        - this store is designed around '/' as the chunk separator internally
        - metadata such as .zarray/.zattrs/.zgroup must NOT be rewritten

        Without this conversion, direct-sharded v2 chunk paths like foo/0.1 end up
        producing an archive path outside the array directory structure.
        """
        # if self.zarr_version != 2 or self._is_metadata_key(key):

        # Directly return metadata keys without modification
        if self._is_metadata_key(key):
            return key
        parent, base = os.path.split(key)
        # Only convert keys that look like v2 chunk keys (contain '.' but don't start with it).
        if '.' not in base or base.startswith('.'):
            # This is v3 key, so we should return it unchanged.
            return key
        # Convert v2 chunk keys from dot-separated to slash-separated form.
        return os.path.join(parent, base.replace('.', os.path.sep)) if parent else base.replace('.', os.path.sep)

    def _is_metadata_key(self, key):
        """
        Return True only for actual metadata objects.

        Why this exists:
        - v2 metadata files are .zarray/.zattrs/.zgroup
        - v3 metadata file is zarr.json
        - v2 chunk keys may still contain '.' (e.g. 0.0)

        So we must not use "." in the whole key as a metadata test.
        """
        base = os.path.basename(key)
        return base in {V2_ARRAY_META_KEY, V2_ZATTR_META_KEY, V2_GROUP_META_KEY, V3_META_KEY}

    def _prune_empty_parents(self, start_path):
        """
        Remove empty directories after deletes.

        This fixes recreate-after-delete flows where the old implementation left
        empty nested chunk directories behind, which later confused Zarr when
        creating the same array/chunk path again.
        """
        current = os.path.dirname(start_path)
        root = os.path.abspath(self.path)
        while os.path.abspath(current).startswith(root) and os.path.abspath(current) != root:
            if not os.path.isdir(current):
                current = os.path.dirname(current)
                continue
            try:
                if len(os.listdir(current)) == 0:
                    os.rmdir(current)
                    current = os.path.dirname(current)
                    continue
            except OSError:
                pass
            break

    def _remove_empty_archive(self, archive):
        """
        Delete an HDF5 shard file once its last dataset has been removed.

        This keeps delete/recreate cycles clean and avoids leaving behind empty
        *.h5 container files.
        """
        if not os.path.isfile(archive):
            return
        with h5py.File(archive, 'r', libver='latest', locking=True) as f:
            is_empty = len(f.keys()) == 0
        if is_empty:
            os.remove(archive)
            self._prune_empty_parents(archive)

    def _fromh5(self,archive,key):
        with h5py.File(archive, 'r', libver='latest', locking=True) as f:
            if key in f:
                return f[key][()].tobytes()
        raise KeyError(key)

    def _toh5(self,archive,key,value):
        times = 0
        if isinstance(value,np.ndarray):
            value = value.tobytes()
        while True:
            try:
                with h5py.File(archive, 'a', libver='latest', locking=True) as f:
                    if key in f:
                        del f[key]
                    f.create_dataset(key, data=np.void(value))
            except OSError:
                pass

            if self.auto_verify_write:
                times += 1
                try:
                    if self._fromh5(archive,key) == value:
                        break
                    else:
                        print(f'Verification failed {times} time, retrying')
                except KeyError:
                    print(f'Verification failed {times} time, retrying')
            else:
                break

    def path_depth(self,path,compare_path=None):

        if compare_path is None:
            compare_path = self.path

        startinglevel = compare_path.count(os.sep)
        totallevel = path.count(os.sep)
        return totallevel - startinglevel

    def get_unique_archive_locations(self):
        unique_archive_locations = {}
        for a in self._arrays:
            for root, folder, files in os.walk(a,topdown=True):
                for f in files:
                    filepath = os.path.join(root,f)
                    if '.z' not in f \
                        and self.path_depth(filepath,a) > self._consolidate_depth:

                        archive,key = self._get_archive_key_name(filepath)
                        if archive not in unique_archive_locations:
                            unique_archive_locations[archive] = None
                            yield archive

    def _migrate_path_to_archive(self,archive,path_name):
        print('Moving chunk files into {}'.format(archive))
        with h5py.File(archive, 'a', libver='latest', locking=True) as h:
            for root, folder, files in os.walk(path_name, topdown=True):
                for f in files:
                    filepath = os.path.join(root,f)
                    _ ,key = self._get_archive_key_name(filepath)
                    with open(filepath,'rb') as fp:
                        if key in h:
                            print(f'Deleting preexisting {key}')
                            del h[key]
                        h.create_dataset(key, data=np.void(fp.read()))
                    os.remove(filepath)

    def consolidate(self):

        par = False
        try:
            import dask
            from dask.delayed import delayed
            par = True
            to_run = []
            append = to_run.append
        except:
            pass

        for unique in self.get_unique_archive_locations():
            path_name = os.path.splitext(unique)[0]
            archive = unique
            if par:
                print('Delaying {}'.format(unique))
                d = delayed(self._migrate_path_to_archive)(archive,path_name)
                append(d)
                del d
            else:
                self._migrate_path_to_archive(archive,path_name)

        if par:
            del append
            to_run = dask.compute(to_run)

        for a in self._arrays:
            for root, folder, files in os.walk(a,topdown=False):
                for f in folder:
                    filepath = os.path.join(root,f)
                    if os.path.exists(filepath) and len(os.listdir(filepath)) == 0:
                        print('Removing Empty Dir {}'.format(filepath))
                        shutil.rmtree(filepath)

    def __getitem__(self, key):
        # print(f"Getting key: {key}")
        key = self._normalize_key(key)
        # CHANGE:
        # For v2, convert dotted chunk keys like '0.1' into nested internal
        # paths like '0/1', but keep metadata keys unchanged.
        key = self._maybe_convert_v2_chunk_key(key)
        filepath = os.path.join(self.path, key)

        if os.path.isfile(filepath):
            try:
                return self._fromfile(filepath)
            except:
                pass

        archive, h_key = self._get_archive_key_name(filepath)

        if os.path.isfile(archive):
            try:
                if self._write_direct and self.mode != 'r':
                    return self._read_direct_to_h5(archive,h_key)
                else:
                    return self._fromh5(archive,h_key)
            except:
                pass

        raise KeyError(key)

    import time
    @staticmethod
    def _timedelta(start,delta=5):
        return time.time()-start >= delta

    def _write_direct_to_h5(self,file_path,value):
        archive, key = self._get_archive_key_name(file_path)
        os.makedirs(os.path.split(archive)[0], exist_ok=True)
        if self.distribuited:
            try:
                lock = self.Lock(name=archive)
                with lock:
                    self._toh5(archive, key, value)
            except Exception as e:
                print(e)
                pass
        else:
            self._toh5(archive, key, value)
        return

    def _read_direct_to_h5(self,archive,key):
        if self.distribuited:
            try:
                lock = self.Lock(name=archive)
                with lock:
                    return self._fromh5(archive, key)
            except:
                raise
        else:
            return self._fromh5(archive, key)

    def __setitem__(self, key, value):
        key = self._normalize_key(key)
        # CHANGE:
        # Internally normalize v2 chunk keys to use '/' while keeping metadata
        # paths untouched.
        key = self._maybe_convert_v2_chunk_key(key)
        value = ensure_contiguous_ndarray_like(value)
        file_path = os.path.join(self.path, key)

        # CHANGE:
        # Use exact metadata detection instead of "'.' not in key".
        # This keeps metadata as ordinary files, while still allowing v2 chunk
        # keys such as "0.0" to be stored directly inside HDF5 shards when
        # write_direct=True.
        if self._write_direct and not os.path.isfile(file_path) and not self._is_metadata_key(key):
            self._write_direct_to_h5(file_path, value)
            return

        if os.path.isdir(file_path):
            shutil.rmtree(file_path)

        dir_path, file_name = os.path.split(file_path)
        if os.path.isfile(dir_path):
            raise KeyError(key)
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise KeyError(key)

        temp_name = f'{file_name}.{uuid.uuid4().hex}.partial'
        temp_path = os.path.join(dir_path, temp_name)
        try:
            self._tofile(value, temp_path)
            _retry_call(os.replace, (temp_path, file_path), exceptions=(PermissionError,))
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def __delitem__(self, key):
        key = self._normalize_key(key)
        key = self._maybe_convert_v2_chunk_key(key)
        path = os.path.join(self.path, key)
        removed_anything = False

        # CHANGE:
        # Delete raw files/directories first and then prune empty parent dirs.
        # This fixes recreate-after-delete failures caused by leftover empty
        # chunk directories.
        if os.path.isfile(path):
            os.remove(path)
            removed_anything = True
            self._prune_empty_parents(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
            removed_anything = True
            self._prune_empty_parents(path)

        # CHANGE:
        # Also delete from HDF5 shards when present.  Missing keys should be a
        # no-op rather than an exception, because Zarr overwrite/delete flows may
        # attempt cleanup for paths that are already gone.
        archive, h_key = self._get_archive_key_name(path)
        if os.path.isfile(archive):
            if self.distribuited:
                lock = self.Lock(name=archive)
                with lock:
                    with h5py.File(archive, 'a', libver='latest', locking=True) as f:
                        if h_key in f:
                            del f[h_key]
                            removed_anything = True
            else:
                with h5py.File(archive, 'a', libver='latest', locking=True) as f:
                    if h_key in f:
                        del f[h_key]
                        removed_anything = True
            self._remove_empty_archive(archive)

        # CHANGE:
        # Be tolerant here.  Returning without KeyError makes delete idempotent,
        # which matches what Zarr expects during overwrite/recreate operations.
        return

    def __contains__(self, key):
        key = self._normalize_key(key)
        key = self._maybe_convert_v2_chunk_key(key)
        file_path = os.path.join(self.path, key)

        if os.path.isfile(file_path):
            return True

        archive, key = self._get_archive_key_name(file_path)

        if os.path.isfile(archive):
             return self._dset_in(archive,key)

        return False

    def _dset_in(self,archive,key):
        with h5py.File(archive, 'r', libver='latest') as f:
            return key in f

    def __eq__(self, other):
        return (
            isinstance(other, H5_Nested_Store) and
            self.path == other.path
        )

    def _get_zip_keys(self,archive):
        with h5py.File(archive, 'r', libver='latest') as f:
            yield tuple(f.keys())

    def keys(self):
        if os.path.exists(self.path):
            yield from self._keys_fast()

    def _keys_fast(self, walker=os.walk):
        for dirpath, _, filenames in walker(self.path):
            dirpath = os.path.relpath(dirpath, self.path)
            for f in filenames:
                basefile, ext = os.path.splitext(f)

                # CHANGE:
                # Always expand shard files into logical Zarr keys, even when the
                # shard is at the store root.  The old code yielded "c.h5" at the
                # root instead of chunk keys like "c/0/0", which broke listing for
                # v3 stores and confused key enumeration.
                if ext == self.container_ext:
                    names = self._get_zip_keys(os.path.join(self.path, dirpath, f) if dirpath != os.curdir else os.path.join(self.path, f))
                    names = (x.replace('.', os.path.sep) for x in tuple(names)[0])
                    if dirpath == os.curdir:
                        names = (os.path.sep.join((basefile, x)) for x in names)
                    else:
                        names = (os.path.sep.join((dirpath, basefile, x)) for x in names)
                    yield from names
                else:
                    if dirpath == os.curdir:
                        yield f
                    else:
                        yield os.path.sep.join((dirpath, f))

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return sum(1 for _ in self.keys())

    def dir_path(self, path=None):
        store_path = _normalize_storage_path(path)
        dir_path = self.path
        if store_path:
            dir_path = os.path.join(dir_path, store_path)
        return dir_path

    def listdir(self, path=None):
        return self._nested_listdir(path) if self._dimension_separator == "/" else \
            self._flat_listdir(path)

    def _flat_listdir(self, path=None):
        dir_path = self.dir_path(path)
        if os.path.isdir(dir_path):
            return sorted(os.listdir(dir_path))
        else:
            return []

    def _nested_listdir(self, path=None):
        children = self._flat_listdir(path=path)
        if V2_ARRAY_META_KEY in children:
            new_children = []
            root_path = self.dir_path(path)
            for entry in children:
                entry_path = os.path.join(root_path, entry)
                if _prog_number.match(entry) and os.path.isdir(entry_path):
                    for dir_path, _, file_names in os.walk(entry_path):
                        for file_name in file_names:
                            file_path = os.path.join(dir_path, file_name)
                            rel_path = file_path.split(root_path + os.path.sep)[1]
                            new_children.append(rel_path.replace(os.path.sep, '.'))
                else:
                    new_children.append(entry)
            return sorted(new_children)
        else:
            return children

    def rename(self, src_path, dst_path):
        store_src_path = _normalize_storage_path(src_path)
        store_dst_path = _normalize_storage_path(dst_path)

        dir_path = self.path

        src_path = os.path.join(dir_path, store_src_path)
        dst_path = os.path.join(dir_path, store_dst_path)

        os.renames(src_path, dst_path)

    def rmdir(self, path=None):
        store_path = _normalize_storage_path(path)
        dir_path = self.path
        if store_path:
            dir_path = os.path.join(dir_path, store_path)
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)

    def getsize(self, path=None):
        store_path = _normalize_storage_path(path)
        fs_path = self.path
        if store_path:
            fs_path = os.path.join(fs_path, store_path)
        if os.path.isfile(fs_path):
            return os.path.getsize(fs_path)
        elif os.path.isdir(fs_path):
            size = 0
            for child in scandir(fs_path):
                if child.is_file():
                    size += child.stat().st_size
            return size
        else:
            return 0

    def clear(self):
        shutil.rmtree(self.path)

    def atexit_rmtree(path,
                      isdir=os.path.isdir,
                      rmtree=shutil.rmtree):
        if isdir(path):
            rmtree(path)

    def atexit_rmglob(path,
                      glob=glob.glob,
                      isdir=os.path.isdir,
                      isfile=os.path.isfile,
                      remove=os.remove,
                      rmtree=shutil.rmtree):
        for p in glob(path):
            if isfile(p):
                remove(p)
            elif isdir(p):
                rmtree(p)

    @property
    def supports_writes(self) -> bool:
        return not self.read_only

    @property
    def supports_deletes(self) -> bool:
        return not self.read_only

    @property
    def supports_listing(self) -> bool:
        return True

    # CHANGE:
    # Zarr 3 expects stores to support creating a read-only clone of the store.
    # Without this, open_group(..., mode='r') fails even though plain reads work.
    def with_read_only(self, read_only: bool = False):
        mode = 'r' if read_only else self.mode
        return type(self)(
            path=self.path,
            normalize_keys=self.normalize_keys,
            dimension_separator=self._dimension_separator,
            write_direct=self._write_direct,
            swmr=self.swmr,
            container_ext=self.container_ext,
            distribuited_lock=self.distribuited_lock,
            consolidate=False,
            consolidate_depth=self._consolidate_depth,
            consolidate_parallel=self._consolidate_parallel,
            auto_verify_write=self.auto_verify_write,
            mode=mode,
        )

    async def get(self, key: str, prototype=None, byte_range: ByteRequest | None = None):
        if prototype is None:
            prototype = default_buffer_prototype()
        try:
            data = self.__getitem__(key)
        except KeyError:
            return None

        if byte_range is not None:
            if isinstance(byte_range, RangeByteRequest):
                data = data[byte_range.start:byte_range.end]
            elif isinstance(byte_range, OffsetByteRequest):
                data = data[byte_range.offset:]
            elif isinstance(byte_range, SuffixByteRequest):
                data = data[-byte_range.suffix:]

        return prototype.buffer.from_bytes(data)

    async def get_partial_values(self, prototype, key_ranges):
        if prototype is None:
            prototype = default_buffer_prototype()
        out = []
        for k, r in key_ranges:
            out.append(await self.get(k, prototype=prototype, byte_range=r))
        return out

    async def exists(self, key: str) -> bool:
        return self.__contains__(key)

    async def set(self, key: str, value) -> None:
        if self.read_only:
            raise PermissionError("Store is read-only")
        data = value.to_bytes() if hasattr(value, "to_bytes") else bytes(value)
        self.__setitem__(key, data)

    async def delete(self, key: str) -> None:
        if self.read_only:
            raise PermissionError("Store is read-only")
        self.__delitem__(key)

    def list(self):
        async def gen():
            for k in self.keys():
                yield k.replace(os.path.sep, "/")
        return gen()

    def list_prefix(self, prefix: str):
        prefix = prefix.lstrip("/")
        async def gen():
            for k in self.keys():
                k2 = k.replace(os.path.sep, "/")
                if k2.startswith(prefix):
                    yield k2
        return gen()

    def list_dir(self, prefix: str):
        prefix = prefix.strip("/")

        async def gen():
            seen = set()

            # Group and raw-chunk directories can be listed directly.  The old
            # implementation called list_prefix(), which expanded every HDF5
            # shard in the entire store just to discover immediate children.
            # On large OME-HAN datasets this turned a metadata lookup into a
            # full-store scan.
            base = self.dir_path(prefix)
            if os.path.isdir(base):
                for entry in scandir(base):
                    name = entry.name
                    if entry.is_file() and name.endswith(self.container_ext):
                        name = name[: -len(self.container_ext)]
                    if name and name not in seen:
                        seen.add(name)
                        yield name
                return

            # A logical prefix may point inside an HDF5 shard. Walk toward the
            # store root until its archive is found, then list only the matching
            # immediate dataset children from that one shard.
            candidate = base
            internal_parts = []
            root = os.path.abspath(self.path)
            while os.path.abspath(candidate).startswith(root):
                archive = candidate + self.container_ext
                if os.path.isfile(archive):
                    internal_prefix = "/".join(internal_parts)
                    with h5py.File(archive, "r", libver="latest", locking=True) as h5:
                        for key in h5.keys():
                            logical_key = key.replace(".", "/")
                            if internal_prefix:
                                marker = internal_prefix + "/"
                                if not logical_key.startswith(marker):
                                    continue
                                logical_key = logical_key[len(marker) :]
                            first = logical_key.split("/", 1)[0]
                            if first and first not in seen:
                                seen.add(first)
                                yield first
                    return
                if os.path.abspath(candidate) == root:
                    return
                candidate, last = os.path.split(candidate)
                internal_parts.insert(0, last)

        return gen()
