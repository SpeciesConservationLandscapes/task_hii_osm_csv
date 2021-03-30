from math import ceil
from pathlib import Path
from typing import Iterator, List, Tuple, Union

import numba as nb  # type: ignore
import rasterio  # type: ignore
from rasterio import transform  # type: ignore
from rasterio.profiles import Profile  # type: ignore
from rasterio.windows import Window  # type: ignore


class ImageStackMetadata:
    source_images: List[Union[str, Path]]
    profile: Profile
    x_read_offset: int = 0
    y_read_offset: int = 0
    read_windows: Union[Iterator[Window], List[Window]]
    write_windows: Union[Iterator[Window], List[Window]]


@nb.jit(nopython=True)
def values_check(array):
    for x in array.flat:
        if x:
            return True
    return False


def get_windows(  # noqa: C901
    width: int,
    height: int,
    size: int = 256,
    offset: Tuple[int, int] = (
        0,
        0,
    ),
) -> Iterator[Window]:
    num_windows_w = int(ceil(width * 1.0 / size))
    num_windows_h = int(ceil(height * 1.0 / size))
    offset_x = None
    offset_y = 0

    for _ in range(num_windows_h):
        offset_x = 0
        for _ in range(num_windows_w):
            if offset_x > width:
                size_w = width
            elif offset_x + size < width:
                size_w = size
            else:
                size_w = width - offset_x

            if offset_y > height:
                size_h = height
            elif offset_y + size < height:
                size_h = size
            else:
                size_h = height - offset_y
            yield Window(offset_x + offset[0], offset_y + offset[1], size_w, size_h)

            offset_x = offset_x + size
        offset_y = offset_y + size


def stack_images(  # noqa: C901
    image_stack_metadata: ImageStackMetadata,
    output_path: Union[str, Path],
) -> Union[None, Path]:

    image_paths = image_stack_metadata.source_images

    if len(image_paths) == 0:
        return None
    elif len(image_paths) == 1:
        return Path(image_paths[0])

    image0 = rasterio.open(image_paths[0], "r")

    profile = image_stack_metadata.profile

    profile["tiled"] = True
    profile["blockxsize"] = 1024
    profile["blockysize"] = 1024
    profile["nodata"] = 0
    profile["dtype"] = rasterio.uint8
    profile["count"] = len(image_paths)
    profile["compress"] = "deflate"
    profile["predictor"] = "2"
    profile["ZLEVEL"] = "9"
    profile["num_threads"] = "1"

    output_image = rasterio.open(output_path, "w", **profile)
    image_datasets = [image0]
    image_datasets.extend([rasterio.open(ip, "r") for ip in image_paths[1:]])

    windows = zip(image_stack_metadata.read_windows, image_stack_metadata.write_windows)
    for window, write_window in windows:
        for n, image_dataset in enumerate(image_datasets):
            chunk = image_dataset.read(window=window, indexes=1)
            if values_check(chunk) is False:
                continue
            output_image.write(chunk, window=write_window, indexes=n + 1)

    for image_dataset in image_datasets:
        image_dataset.close()
        image_dataset = None

    output_image.close()
    output_image = None
    for image_dataset in image_datasets:
        image_dataset.close()
        image_dataset = None

    return Path(output_path)


def split_image(
    image_paths: List[Union[str, Path]], num_splits: int, window_size: int = 256
) -> List[ImageStackMetadata]:
    with rasterio.open(image_paths[0], "r") as ds:
        profile = ds.profile
        if num_splits < 1:
            raise ValueError("num_splits less than 1")
        elif num_splits == 1:
            meta = ImageStackMetadata()
            meta.profile = profile
            meta.source_images = image_paths
            meta.x_read_offset = 0
            meta.y_read_offset = 0
            meta.read_windows = [Window(0, 0, profile["width"], profile["height"])]
            meta.write_windows = meta.read_windows
            return [meta]

        src_transform = ds.profile["transform"]
        width = profile["width"]
        height = profile["height"]
        split_img_width = abs(ceil(width * 1.0 / num_splits))
        metas = []
        for n in range(num_splits):
            meta = ImageStackMetadata()
            meta.source_images = image_paths
            meta.x_read_offset = split_img_width * n
            meta.y_read_offset = 0

            new_profile = profile.copy()

            # Update the x-min value of the new image transformer
            x, _ = transform.xy(src_transform, 0, meta.x_read_offset, offset="ul")
            tf = list(new_profile["transform"].to_gdal())
            tf[0] = x
            new_profile["transform"] = transform.Affine.from_gdal(*tf)

            if split_img_width * n > width:
                new_image_width = width - (split_img_width * (n - 1))
            else:
                new_image_width = split_img_width

            new_profile["width"] = new_image_width

            meta.read_windows = list(
                get_windows(
                    width=new_image_width,
                    height=height,
                    size=window_size,
                    offset=(
                        meta.x_read_offset,
                        meta.y_read_offset,
                    ),
                )
            )

            meta.write_windows = list(
                get_windows(width=new_image_width, height=height, size=window_size)
            )

            meta.profile = new_profile
            metas.append(meta)

    return metas
