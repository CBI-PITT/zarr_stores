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

from zarr.errors import (
    MetadataError,
    BadCompressorError,
    ContainsArrayError,
    ContainsGroupError,
    FSPathExistNotDir,
    ReadOnlyError,
)

from numcodecs.abc import Codec
from numcodecs.compat import (
    ensure_bytes,
    ensure_text,
    ensure_contiguous_ndarray,
    ensure_contiguous_ndarray_like
)

# from numcodecs.registry import codec_registry

# from threading import Lock, RLock
# from filelock import Timeout, FileLock, SoftFileLock

from zarr.util import (buffer_size, json_loads, nolock, normalize_chunks,
                       normalize_dimension_separator,
                       normalize_dtype, normalize_fill_value, normalize_order,
                       normalize_shape, normalize_storage_path, retry_call)

from zarr._storage.absstore import ABSStore  # noqa: F401

from zarr._storage.store import Store, array_meta_key

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
        
    Examples
    --------
    Store a single array::
        >>> import zarr
        >>> store = zarr.DirectoryStore('data/array.zarr')
        >>> z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
        >>> z[...] = 42
    Each chunk of the array is stored as a separate file on the file system,
    i.e.::
        >>> import os
        >>> sorted(os.listdir('data/array.zarr'))
        ['.zarray', '0.0', '0.1', '1.0', '1.1']
    Store a group::
        >>> store = zarr.DirectoryStore('data/group.zarr')
        >>> root = zarr.group(store=store, overwrite=True)
        >>> foo = root.create_group('foo')
        >>> bar = foo.zeros('bar', shape=(10, 10), chunks=(5, 5))
        >>> bar[...] = 42
    When storing a group, levels in the group hierarchy will correspond to
    directories on the file system, i.e.::
        >>> sorted(os.listdir('data/group.zarr'))
        ['.zgroup', 'foo']
        >>> sorted(os.listdir('data/group.zarr/foo'))
        ['.zgroup', 'bar']
        >>> sorted(os.listdir('data/group.zarr/foo/bar'))
        ['.zarray', '0.0', '0.1', '1.0', '1.1']
    Notes
    -----
    Atomic writes are used, which means that data are first written to a
    temporary file, then moved into place when the write is successfully
    completed. Files are only held open while they are being read or written and are
    closed immediately afterwards, so there is no need to manually close any files.
    Safe to write in multiple threads or processes.
    """

    def __init__(self, path, normalize_keys=False, dimension_separator='/', 
                 write_direct=True, swmr=False, container_ext='h5', distribuited_lock=False,
                 consolidate=False, consolidate_depth=3, consolidate_parallel=True,
                 auto_verify_write=False, mode='a',
                 ):

        # guard conditions
        path = os.path.abspath(path)
        if os.path.exists(path) and not os.path.isdir(path):
            raise FSPathExistNotDir(path)

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
                    # if self.dist_client.status == 'running':
                    #     self.distribuited = True
                    # else:
                    #     self.distribuited = False
                # except ValueError:
                #     self.dist_client = Client()
                except:
                    # print('BROKE TRYING TO GET CLIENT')
                    # print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
                    # print(self.dist_client)
                    # print(self.distribuited)
                    # print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
                    # if self.dist_client is not None:
                    #     self.dist_client.close()
                    self.dist_client = None
                    self.distribuited = False
                if self.dist_client is not None and self.distribuited:
                    # print('Connected to distribuited client')
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
        # if self.distribuited and self.dist_client is not None:
        #     self.dist_client.close()


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
        """ Read data from a file
        Parameters
        ----------
        fn : str
            Filepath to open and read from.
        Notes
        -----
        Subclasses should overload this method to specify any custom
        file reading logic.
        """
        with open(fn, 'rb') as f:
            return f.read()

    def _tofile(self, a, fn):
        """ Write data to a file
        Parameters
        ----------
        a : array-like
            Data to write into the file.
        fn : str
            Filepath to open and write to.
        Notes
        -----
        Subclasses should overload this method to specify any custom
        file writing logic.
        """
        while True:
            with open(fn, mode='wb') as f:
                f.write(a)
            # Verify contents of file and repeat write if not correct
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

    # def _get_archive_key_name(self,path):
    #     path_parts = path.split('/')
    #     key = path_parts[-(self._consolidate_depth-1):]
    #     archive = path_parts[:-(self._consolidate_depth-1)]
    #     return os.path.join(archive) + f'.{self.container_ext}', '.'.join(key)

    def _fromh5(self,archive,key):
        # print('In _fromh5')
        with h5py.File(archive, 'r', libver='latest', locking=True) as f:
            # print('In file')
            if key in f:
                # print('Getting Data')
                # return f[key].tobytes()
                return f[key][()].tobytes()
        raise KeyError(key)

    def _toh5(self,archive,key,value):
        times = 0
        if isinstance(value,np.ndarray):
            value = value.tobytes()
        while True:
            # Attempt to catch OSError which sometimes occurs if the h5 file is already open for read only.
            try:
                with h5py.File(archive, 'a', libver='latest', locking=True) as f:
                    # f.swmr_mode = self.swmr
                    if key in f:
                        del f[key]
                    f.create_dataset(key, data=np.void(value))
                    # f.create_dataset(key, data=value)
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

        startinglevel = compare_path.count(os.sep) #Normalization happens at __init__
        totallevel = path.count(os.sep)
        # totallevel = os.path.normpath(path).count(os.sep)
        return totallevel - startinglevel

    #Generator to yield unique archive locations for existing raw chunk files
    def get_unique_archive_locations(self):
        unique_archive_locations = {}
        # past_first = False
        for a in self._arrays:
            for root, folder, files in os.walk(a,topdown=True):
                # if past_first:
                for f in files:
                    filepath = os.path.join(root,f)
                    # print(filepath)
                    # Filter out metadata files (.zarray) or any files
                    if '.z' not in f \
                        and self.path_depth(filepath,a) > self._consolidate_depth:

                        archive,key = self._get_archive_key_name(filepath)
                        if archive not in unique_archive_locations:
                            unique_archive_locations[archive] = None
                            yield archive

    def _migrate_path_to_archive(self,archive,path_name):
        '''
        Given a H5 name and path, migrate all files under the
        path into the H5 file
        '''

        print('Moving chunk files into {}'.format(archive))
        with h5py.File(archive, 'a', libver='latest', locking=True) as h:
            # h.swmr_mode = self.swmr
            for root, folder, files in os.walk(path_name, topdown=True):
                for f in files:
                    filepath = os.path.join(root,f)
                    _ ,key = self._get_archive_key_name(filepath)
                    # print('Copying {} to {}'.format(filepath,archive))
                    #Write RAW chunk into H5
                    with open(filepath,'rb') as fp:
                        # print(key)
                        if key in h:
                            print(f'Deleting preexisting {key}')
                            del h[key]
                        h.create_dataset(key, data=np.void(fp.read()))
                    #Delete RAW chunk
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

        #Clean empty directories
        for a in self._arrays:
            for root, folder, files in os.walk(a,topdown=False):
                for f in folder:
                    filepath = os.path.join(root,f)
                    if os.path.exists(filepath) and len(os.listdir(filepath)) == 0:
                        print('Removing Empty Dir {}'.format(filepath))
                        shutil.rmtree(filepath)


    def __getitem__(self, key):
        # print('In Get Item')
        key = self._normalize_key(key)
        filepath = os.path.join(self.path, key)

        #Attempt to read raw file first if it exists
        if os.path.isfile(filepath):
            try:
                return self._fromfile(filepath)
            except:
                pass

        # Assume file does not exist, determine the name of shard file (archive) and key
        archive, h_key = self._get_archive_key_name(filepath)

        #Attempt to read file from H5
        if os.path.isfile(archive):
            # print('In read archive')
            try:
                if self._write_direct and self.mode != 'r':
                    return self._read_direct_to_h5(archive,h_key)
                else:
                    return self._fromh5(archive,h_key)
            except:
                pass

        #KeyError if neither RAW file nor key found in H5
        # print('Raising Key Error')
        raise KeyError(key)

    import time
    @staticmethod
    def _timedelta(start,delta=5):
        return time.time()-start >= delta

    def _write_direct_to_h5(self,file_path,value):
        '''
        Directly write chunks to h5 file. If distribuited locking is
        enabled, use it; else no external locking.

        h5py locking is enabled by default, probably making it thread safe,
        but it may not be multiprocess safe if distribuited locking is disabled.
        '''
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
        '''
        Directly write chunks to h5 file. If distribuited locking is
        enabled, use it; else no external locking.

        h5py locking is enabled by default, making it thread safe, but is may not be
        multiprocess safe if distribuited locking is disabled.
        '''
        # archive, key = self._get_archive_key_name(file_path)
        # os.makedirs(os.path.split(archive)[0], exist_ok=True)
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

        # coerce to flat, contiguous array (ideally without copying)
        value = ensure_contiguous_ndarray_like(value)

        # destination path for key
        file_path = os.path.join(self.path, key)

        # Write direct (to h5 file) if it is enabled:
            # If '.' is in the file name assume it is a metadata file and skip
            # If file aready exists assume that a chunks exists in the form of 
            # a NestedDirectoryStore and skip write direct to overwrite file
        if self._write_direct and not '.' in key and not os.path.isfile(file_path):
            self._write_direct_to_h5(file_path, value)
            return


        ## Everything below here mimics a NestedDirectoryStore ##
        
        # ensure there is no directory in the way
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)

        # ensure containing directory exists
        dir_path, file_name = os.path.split(file_path)
        if os.path.isfile(dir_path):
            raise KeyError(key)
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise KeyError(key)

        # write to temporary file
        # note we're not using tempfile.NamedTemporaryFile to avoid restrictive file permissions
        temp_name = f'{file_name}.{uuid.uuid4().hex}.partial'
        temp_path = os.path.join(dir_path, temp_name)
        try:
            self._tofile(value, temp_path)

            # move temporary file into place;
            # make several attempts at writing the temporary file to get past
            # potential antivirus file locking issues
            retry_call(os.replace, (temp_path, file_path), exceptions=(PermissionError,))

        finally:
            # clean up if temp file still exists for whatever reason
            if os.path.exists(temp_path):  # pragma: no cover
                os.remove(temp_path)

    def __delitem__(self, key):
        key = self._normalize_key(key)
        path = os.path.join(self.path, key)
        
        #Delete the file if it exists
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            # include support for deleting directories, even though strictly
            # speaking these do not exist as keys in the store
            shutil.rmtree(path)
        
        # Delete the dset in h5 file if it exists
        # If a file and h5 dset exist, both will be deleted
        archive, h_key = self._get_archive_key_name(path)
        if os.path.isfile(archive):
            if self.distribuited:
                lock = self.Lock(name=archive)
                with lock:
                    with h5py.File(archive, 'a', libver='latest', locking=True) as f:
                        # f.swmr_mode = self.swmr
                        if h_key in f:
                            del f[h_key]
            else:
                with h5py.File(archive, 'a', libver='latest', locking=True) as f:
                    # f.swmr_mode = self.swmr
                    if h_key in f:
                        del f[h_key]
        else:
            raise KeyError(key)

    def __contains__(self, key):
        key = self._normalize_key(key)
        file_path = os.path.join(self.path, key)
        
        if os.path.isfile(file_path):
            return True
        
        archive, key = self._get_archive_key_name(file_path)
                
        if os.path.isfile(archive):
             return self._dset_in(archive,key)
        
        #If all other fail to return True
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
            if dirpath == os.curdir:
                for f in filenames:
                    yield f
            else:
                # dirpath = dirpath.replace("\\", "/")
                for f in filenames:
                    basefile, ext = os.path.splitext(f)
                    if ext == self.container_ext:
                        names = self._get_zip_keys(os.path.join(self.path,dirpath,f))
                        # Keys are stored in h5 with '.' separator, replace with appropriate separator
                        names = (x.replace('.',os.path.sep) for x in tuple(names)[0])
                        names = (os.path.sep.join((dirpath, basefile,x)) for x in names)
                        yield from names
                    # elif ext == '.tmp' and os.path.splitext(basefile)[-1] == self.container_ext:
                    #     basefile, ext = os.path.splitext(basefile)
                    #     names = self._get_zip_keys(f)
                    #     names = ("/".join((dirpath, basefile,x)) for x in names)
                    #     yield from names
                    else:
                        yield os.path.sep.join((dirpath, f))

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return sum(1 for _ in self.keys())

    def dir_path(self, path=None):
        store_path = normalize_storage_path(path)
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
        if array_meta_key in children:
            # special handling of directories containing an array to map nested chunk
            # keys back to standard chunk keys
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
        store_src_path = normalize_storage_path(src_path)
        store_dst_path = normalize_storage_path(dst_path)

        dir_path = self.path

        src_path = os.path.join(dir_path, store_src_path)
        dst_path = os.path.join(dir_path, store_dst_path)

        os.renames(src_path, dst_path)

    def rmdir(self, path=None):
        store_path = normalize_storage_path(path)
        dir_path = self.path
        if store_path:
            dir_path = os.path.join(dir_path, store_path)
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)

    def getsize(self, path=None):
        store_path = normalize_storage_path(path)
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
                      rmtree=shutil.rmtree):  # pragma: no cover
        """Ensure directory removal at interpreter exit."""
        if isdir(path):
            rmtree(path)
    
    
    # noinspection PyShadowingNames
    def atexit_rmglob(path,
                      glob=glob.glob,
                      isdir=os.path.isdir,
                      isfile=os.path.isfile,
                      remove=os.remove,
                      rmtree=shutil.rmtree):  # pragma: no cover
        """Ensure removal of multiple files at interpreter exit."""
        for p in glob(path):
            if isfile(p):
                remove(p)
            elif isdir(p):
                rmtree(p)

