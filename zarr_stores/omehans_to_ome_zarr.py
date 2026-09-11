"""Convert an OME-HAN store to a directory-based OME-Zarr v2 store.

OME-HAN stores use :class:`H5_Nested_Store` to put Zarr chunks inside HDF5
shards.  This module deliberately reads them through the Zarr API instead of
depending on that private on-disk layout.  The result is therefore an ordinary
Zarr v2 directory that can be opened without ``zarr_stores`` or HDF5.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import itertools
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

import numpy as np

from .h5_nested_store import H5_Nested_Store


ProgressCallback = Callable[[str, int, int], None]


@dataclass(frozen=True)
class ConversionResult:
    """Summary returned after a successful conversion."""

    source: Path
    destination: Path
    arrays: int
    chunks: int
    logical_bytes: int


def _v2_attributes(attributes: Any) -> dict[str, Any]:
    """Return attributes using the OME-NGFF/Zarr-v2 attribute layout.

    OME-Zarr written on Zarr v3 places NGFF attributes below an ``ome`` key.
    NGFF 0.4 on Zarr v2 stores those keys directly in ``.zattrs``.
    """

    result = copy.deepcopy(dict(attributes))
    ome = result.pop("ome", None)
    if ome is None:
        return result
    if not isinstance(ome, dict):
        raise ValueError("The OME attribute must be a mapping")
    for key, value in ome.items():
        if key in result and result[key] != value:
            raise ValueError(
                f"Cannot flatten OME metadata: conflicting {key!r} attribute"
            )
        result[key] = value
    return result


def _chunk_selections(
    shape: Sequence[int], chunks: Sequence[int]
) -> Iterator[tuple[slice, ...]]:
    if len(shape) == 0:
        yield ()
        return
    chunk_counts = [math.ceil(size / chunk) for size, chunk in zip(shape, chunks)]
    for coordinate in itertools.product(*(range(count) for count in chunk_counts)):
        yield tuple(
            slice(index * chunk, min((index + 1) * chunk, size))
            for index, chunk, size in zip(coordinate, chunks, shape)
        )


def _spatial_axes(multiscale: dict[str, Any], ndim: int) -> tuple[int, int, int]:
    axes = multiscale.get("axes", [])
    names = [axis.get("name") if isinstance(axis, dict) else axis for axis in axes]
    try:
        result = tuple(names.index(name) for name in ("z", "y", "x"))
    except ValueError:
        if ndim < 3:
            raise ValueError(
                "An octree array must have at least three dimensions"
            ) from None
        result = tuple(range(ndim - 3, ndim))
    if len(set(result)) != 3 or any(axis >= ndim for axis in result):
        raise ValueError("Invalid z/y/x axes in multiscales metadata")
    return result  # type: ignore[return-value]


def _octree_chunks(
    ndim: int, spatial_axes: Sequence[int], spatial_chunk_size: int
) -> tuple[int, ...]:
    spatial = set(spatial_axes)
    return tuple(spatial_chunk_size if axis in spatial else 1 for axis in range(ndim))


def _downsample_2x(
    data: np.ndarray, spatial_axes: Sequence[int], method: str
) -> np.ndarray:
    if method == "nearest":
        selection = [slice(None)] * data.ndim
        for axis in spatial_axes:
            selection[axis] = slice(0, None, 2)
        return data[tuple(selection)]
    if method != "mean":
        raise ValueError("downsample method must be 'mean' or 'nearest'")

    work_dtype = np.float32 if data.dtype.itemsize <= 4 else np.float64
    result = data.astype(work_dtype, copy=False)
    for axis in spatial_axes:
        even_selection = [slice(None)] * result.ndim
        odd_selection = [slice(None)] * result.ndim
        even_selection[axis] = slice(0, None, 2)
        odd_selection[axis] = slice(1, None, 2)
        reduced = result[tuple(even_selection)].copy()
        odd = result[tuple(odd_selection)]
        target = [slice(None)] * result.ndim
        target[axis] = slice(0, odd.shape[axis])
        reduced[tuple(target)] += odd
        counts = np.full(reduced.shape[axis], 2, dtype=work_dtype)
        if result.shape[axis] % 2:
            counts[-1] = 1
        count_shape = [1] * result.ndim
        count_shape[axis] = counts.size
        result = reduced / counts.reshape(count_shape)
    return result.astype(data.dtype, copy=False)


def _octree_datasets(
    multiscale: dict[str, Any],
    level_count: int,
    spatial_axes: Sequence[int],
    ndim: int,
) -> list[dict[str, Any]]:
    source_datasets = multiscale.get("datasets") or []
    base = copy.deepcopy(source_datasets[0]) if source_datasets else {}
    transformations = copy.deepcopy(base.get("coordinateTransformations", []))
    scale_transform = next(
        (item for item in transformations if item.get("type") == "scale"), None
    )
    if scale_transform is None:
        scale_transform = {"type": "scale", "scale": [1.0] * ndim}
        transformations.append(scale_transform)
    base_scale = list(scale_transform.get("scale", []))
    if len(base_scale) != ndim:
        raise ValueError("The level-0 scale transform must match the array dimensions")

    datasets = []
    for level in range(level_count):
        dataset = copy.deepcopy(base)
        dataset["path"] = str(level)
        level_transforms = copy.deepcopy(transformations)
        level_scale = next(
            item for item in level_transforms if item.get("type") == "scale"
        )
        level_scale["scale"] = [
            value * (2**level) if axis in spatial_axes else value
            for axis, value in enumerate(base_scale)
        ]
        dataset["coordinateTransformations"] = level_transforms
        datasets.append(dataset)
    return datasets


async def _copy_array(
    source: Any,
    destination_group: Any,
    name: str,
    *,
    path: str,
    compressor: Any,
    dimension_separator: str,
    verify: bool,
    progress: ProgressCallback | None,
    output_chunks: Sequence[int] | None = None,
) -> tuple[int, int, Any]:
    source_format = source.metadata.zarr_format
    create_kwargs: dict[str, Any] = {
        "shape": source.shape,
        "dtype": source.dtype,
        "chunks": tuple(output_chunks) if output_chunks is not None else source.chunks,
        "fill_value": source.metadata.fill_value,
        "attributes": _v2_attributes(source.attrs),
        "chunk_key_encoding": {"name": "v2", "separator": dimension_separator},
    }

    # Zarr-v2 numcodecs can be retained exactly.  Zarr-v3 codecs are not all
    # representable in v2, so "source" falls back to Zarr's v2 default codec.
    if source_format == 2:
        create_kwargs["filters"] = source.filters
        if compressor == "source":
            create_kwargs["compressor"] = source.compressor
    if compressor != "source":
        create_kwargs["compressor"] = compressor

    destination = await destination_group.create_array(name, **create_kwargs)
    copy_chunks = tuple(output_chunks) if output_chunks is not None else source.chunks
    total = math.prod(
        math.ceil(size / chunk) for size, chunk in zip(source.shape, copy_chunks)
    )
    if len(source.shape) == 0:
        total = 1
    logical_bytes = 0

    for completed, selection in enumerate(
        _chunk_selections(source.shape, copy_chunks), start=1
    ):
        data = await source.getitem(selection if selection else ())
        if selection:
            await destination.setitem(selection, data)
            written = await destination.getitem(selection) if verify else None
        else:
            await destination.setitem((), data)
            written = await destination.getitem(()) if verify else None
        logical_bytes += int(np.asarray(data).nbytes)
        if verify:
            np.testing.assert_array_equal(written, data)
        if progress is not None:
            progress(path, completed, total)

    return total, logical_bytes, destination


async def _downsample_level(
    source: Any,
    destination_group: Any,
    name: str,
    *,
    template: Any,
    spatial_axes: Sequence[int],
    chunks: Sequence[int],
    method: str,
    compressor: Any,
    dimension_separator: str,
    verify: bool,
    progress: ProgressCallback | None,
) -> tuple[int, int, Any]:
    output_shape = tuple(
        math.ceil(size / 2) if axis in spatial_axes else size
        for axis, size in enumerate(source.shape)
    )
    create_kwargs: dict[str, Any] = {
        "shape": output_shape,
        "dtype": source.dtype,
        "chunks": tuple(chunks),
        "fill_value": template.metadata.fill_value,
        "chunk_key_encoding": {"name": "v2", "separator": dimension_separator},
    }
    if template.metadata.zarr_format == 2:
        create_kwargs["filters"] = template.filters
        if compressor == "source":
            create_kwargs["compressor"] = template.compressor
    if compressor != "source":
        create_kwargs["compressor"] = compressor
    destination = await destination_group.create_array(name, **create_kwargs)

    total = math.prod(
        math.ceil(size / chunk) for size, chunk in zip(output_shape, chunks)
    )
    logical_bytes = 0
    for completed, output_selection in enumerate(
        _chunk_selections(output_shape, chunks), start=1
    ):
        input_selection = tuple(
            slice(part.start * 2, min(part.stop * 2, source.shape[axis]))
            if axis in spatial_axes
            else part
            for axis, part in enumerate(output_selection)
        )
        source_data = np.asarray(await source.getitem(input_selection))
        data = _downsample_2x(source_data, spatial_axes, method)
        await destination.setitem(output_selection, data)
        logical_bytes += int(data.nbytes)
        if verify:
            np.testing.assert_array_equal(
                await destination.getitem(output_selection), data
            )
        if progress is not None:
            progress(name, completed, total)
    return total, logical_bytes, destination


async def _copy_octree(
    source_group: Any,
    destination_group: Any,
    multiscale: dict[str, Any],
    *,
    prefix: str,
    spatial_chunk_size: int,
    compressor: Any,
    dimension_separator: str,
    verify: bool,
    progress: ProgressCallback | None,
    label_data: bool,
) -> tuple[int, int, int, dict[str, Any]]:
    datasets = multiscale.get("datasets") or []
    if not datasets:
        raise ValueError(f"multiscales at {prefix or '/'} has no datasets")
    source_level_zero = await source_group.getitem(datasets[0]["path"])
    spatial_axes = _spatial_axes(multiscale, source_level_zero.ndim)
    chunks = _octree_chunks(source_level_zero.ndim, spatial_axes, spatial_chunk_size)

    first_path = f"{prefix}/0" if prefix else "0"
    chunk_count, logical_bytes, previous = await _copy_array(
        source_level_zero,
        destination_group,
        "0",
        path=first_path,
        compressor=compressor,
        dimension_separator=dimension_separator,
        verify=verify,
        progress=progress,
        output_chunks=chunks,
    )
    array_count = 1
    while any(previous.shape[axis] > spatial_chunk_size for axis in spatial_axes):
        level = array_count
        path = f"{prefix}/{level}" if prefix else str(level)
        new_chunks, new_bytes, previous = await _downsample_level(
            previous,
            destination_group,
            str(level),
            template=source_level_zero,
            spatial_axes=spatial_axes,
            chunks=chunks,
            method="nearest" if label_data else "mean",
            compressor=compressor,
            dimension_separator=dimension_separator,
            verify=verify,
            progress=(
                (lambda _name, done, total, path=path: progress(path, done, total))
                if progress is not None
                else None
            ),
        )
        array_count += 1
        chunk_count += new_chunks
        logical_bytes += new_bytes

    converted_multiscale = copy.deepcopy(multiscale)
    converted_multiscale["datasets"] = _octree_datasets(
        multiscale, array_count, spatial_axes, source_level_zero.ndim
    )
    return array_count, chunk_count, logical_bytes, converted_multiscale


async def _copy_group(
    source: Any,
    destination: Any,
    *,
    prefix: str,
    compressor: Any,
    dimension_separator: str,
    verify: bool,
    progress: ProgressCallback | None,
    spatial_chunk_size: int,
) -> tuple[int, int, int]:
    attributes = _v2_attributes(source.attrs)
    array_count = chunk_count = logical_bytes = 0

    multiscales = attributes.get("multiscales", [])
    if len(multiscales) > 1:
        raise ValueError("Multiple multiscales entries in one group are not supported")
    pyramid_paths: set[str] = set()
    if multiscales:
        pyramid_paths = {item["path"] for item in multiscales[0].get("datasets", [])}
        if any("/" in path.strip("/") for path in pyramid_paths):
            raise ValueError("Nested multiscale dataset paths are not supported")
        label_data = "labels" in prefix.split("/") or "image-label" in attributes
        arrays, chunks, byte_count, converted = await _copy_octree(
            source,
            destination,
            multiscales[0],
            prefix=prefix,
            spatial_chunk_size=spatial_chunk_size,
            compressor=compressor,
            dimension_separator=dimension_separator,
            verify=verify,
            progress=progress,
            label_data=label_data,
        )
        attributes["multiscales"] = [converted]
        array_count += arrays
        chunk_count += chunks
        logical_bytes += byte_count

    await destination.update_attributes(attributes)

    async for name, source_array in source.arrays():
        if name in pyramid_paths:
            continue
        path = f"{prefix}/{name}" if prefix else name
        output_chunks = (
            _octree_chunks(
                source_array.ndim,
                range(source_array.ndim - 3, source_array.ndim),
                spatial_chunk_size,
            )
            if source_array.ndim >= 3
            else source_array.chunks
        )
        chunks, byte_count, _ = await _copy_array(
            source_array,
            destination,
            name,
            path=path,
            compressor=compressor,
            dimension_separator=dimension_separator,
            verify=verify,
            progress=progress,
            output_chunks=output_chunks,
        )
        array_count += 1
        chunk_count += chunks
        logical_bytes += byte_count

    async for name, source_group in source.groups():
        path = f"{prefix}/{name}" if prefix else name
        destination_group = await destination.create_group(name)
        arrays, chunks, byte_count = await _copy_group(
            source_group,
            destination_group,
            prefix=path,
            compressor=compressor,
            dimension_separator=dimension_separator,
            verify=verify,
            progress=progress,
            spatial_chunk_size=spatial_chunk_size,
        )
        array_count += arrays
        chunk_count += chunks
        logical_bytes += byte_count

    return array_count, chunk_count, logical_bytes


async def async_convert_omehans_to_ome_zarr(
    source: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    overwrite: bool = False,
    compressor: Any = "source",
    dimension_separator: str = "/",
    verify: bool = False,
    progress: ProgressCallback | None = None,
    spatial_chunk_size: int = 128,
) -> ConversionResult:
    """Asynchronously convert to a standard, directory-based OME-Zarr v2.

    All arrays, groups and JSON-compatible attributes are copied. Multiscale
    arrays are rebuilt from level 0 as a 2x2x2 octree. Processing is blockwise;
    a downsampling block spans at most twice the configured spatial chunk edge.
    If the source is Zarr v2, its compressor and filters are kept by default.
    For a Zarr v3 source, Zarr's default v2 compressor is used.

    Parameters
    ----------
    source, destination:
        Paths to the input ``.omehans`` and output ``.ome.zarr`` directories.
    overwrite:
        Permit replacement of an existing destination. The source is never
        modified.
    compressor:
        ``"source"`` (default), a v2-compatible numcodecs codec, or ``None``.
    dimension_separator:
        Chunk-key separator in the output, either ``"/"`` or ``"."``.
    verify:
        Read every output chunk back and compare it with the source chunk.
    progress:
        Optional callback receiving ``(array_path, completed, total)``.
    spatial_chunk_size:
        Output Z/Y/X chunk edge and the stopping size for the octree. The
        default is 128, producing 128x128x128 spatial chunks.
    """

    source_path = Path(source).expanduser().resolve()
    destination_path = Path(destination).expanduser().resolve()

    if source_path == destination_path:
        raise ValueError("Source and destination must be different paths")
    if not source_path.is_dir():
        raise FileNotFoundError(f"OME-HAN source directory not found: {source_path}")
    if destination_path.exists() and not overwrite:
        raise FileExistsError(
            f"Destination already exists: {destination_path}; pass overwrite=True"
        )
    if dimension_separator not in {"/", "."}:
        raise ValueError("dimension_separator must be '/' or '.'")
    if spatial_chunk_size < 1:
        raise ValueError("spatial_chunk_size must be positive")

    from zarr.api.asynchronous import open_group

    source_store = H5_Nested_Store(source_path, mode="r")
    source_root = await open_group(store=source_store, mode="r")
    destination_root = await open_group(
        store=str(destination_path), mode="w", zarr_format=2
    )

    arrays, chunks, logical_bytes = await _copy_group(
        source_root,
        destination_root,
        prefix="",
        compressor=compressor,
        dimension_separator=dimension_separator,
        verify=verify,
        progress=progress,
        spatial_chunk_size=spatial_chunk_size,
    )
    return ConversionResult(
        source=source_path,
        destination=destination_path,
        arrays=arrays,
        chunks=chunks,
        logical_bytes=logical_bytes,
    )


def convert_omehans_to_ome_zarr(
    source: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    overwrite: bool = False,
    compressor: Any = "source",
    dimension_separator: str = "/",
    verify: bool = False,
    progress: ProgressCallback | None = None,
    spatial_chunk_size: int = 128,
) -> ConversionResult:
    """Synchronous wrapper for :func:`async_convert_omehans_to_ome_zarr`.

    Applications already running an asyncio event loop should call the async
    function directly.
    """

    return asyncio.run(
        async_convert_omehans_to_ome_zarr(
            source,
            destination,
            overwrite=overwrite,
            compressor=compressor,
            dimension_separator=dimension_separator,
            verify=verify,
            progress=progress,
            spatial_chunk_size=spatial_chunk_size,
        )
    )


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="input .omehans directory")
    parser.add_argument("destination", help="output .ome.zarr directory")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--no-compression", action="store_true")
    args = parser.parse_args()

    def show_progress(path: str, completed: int, total: int) -> None:
        if completed == total or completed == 1:
            print(f"{path}: {completed}/{total} chunks", flush=True)

    result = convert_omehans_to_ome_zarr(
        args.source,
        args.destination,
        overwrite=args.overwrite,
        verify=args.verify,
        compressor=None if args.no_compression else "source",
        progress=show_progress,
    )
    print(
        f"Converted {result.arrays} arrays, {result.chunks} chunks "
        f"({result.logical_bytes} logical bytes) to {result.destination}"
    )


if __name__ == "__main__":
    _main()
