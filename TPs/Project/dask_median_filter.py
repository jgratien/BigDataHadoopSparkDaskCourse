from itertools import product
from functools import partial
from pathlib import Path
from PIL import Image

import numpy as np
from dask.multiprocessing import get
import dask
from dask.delayed import Delayed
from matplotlib import pyplot as plt
from time import perf_counter as pf

PATH = Path().absolute() / "data"


def get_indexes(i, j, w, h):
    dimin = -1 if i > 0 else 0
    dimax = 1 if i < w - 1 else 0
    djmin = -1 if j > 0 else 0
    djmax = 1 if j < h - 1 else 0
    return product(range(dimin, dimax + 1), range(djmin, djmax + 1))


def median_filter(data):
    i, image = data
    w, h, d = image.shape
    new_image = np.zeros(image.shape, dtype=np.uint8)
    for iw in range(w):
        for jh in range(h):
            values = [image[iw + di, jh + dj] for di, dj in get_indexes(iw, jh, w, h)]
            new_image[iw, jh] = np.median(values, axis=0)
    return i, new_image


def crop(data, reference):
    i, part = data
    if reference == 1:
        return part
    else:
        if i == 0:
            return part[:, :-1]
        elif i + 1 == reference:
            return part[:, 1:]
        else:
            return part[:, 1:-1]


def cat(processed_data):
    return np.concatenate(processed_data, axis=1)


def organize(img, partitions=8):
    h, w, d = img.shape
    step = w // partitions

    ccrop = partial(crop, reference=partitions)

    data = [
        (ipart, img[:, max(0, ipart * step - 1) : min((ipart + 1) * step + 1, w)])
        for ipart in range(partitions)
    ]

    dsk = {f"median-filter-{i}": (median_filter, data[i]) for i in range(partitions)}
    dsk.update({f"crop-{i}": (ccrop, f"median-filter-{i}") for i in range(partitions)})
    dsk.update({f"cat": (cat, [f"crop-{i}" for i in range(partitions)])})
    return dsk


if __name__ == "__main__":
    img = np.array(Image.open(PATH / "lena_noisy.jpg"), dtype="uint8")
    # delayed_dsk = Delayed("cat", organize(img))
    # dask.visualize(delayed_dsk)
    partitions_list = [1, 2, 4, 8]
    for p in partitions_list:
        start = pf()
        dsk = organize(img, partitions=p)
        result = get(dsk, "cat")
        duration = pf() - start
        print(f"For {p} partitions, time : {round(duration, 3)} s")
    new_img = Image.fromarray(result)
    new_img.save(PATH / "lena_filter.jpg")
    print("Image saved.")

    # h, w, d = img.shape
    # partitions = 8
    # step = w // partitions
    #
    # ccrop = partial(crop, reference=partitions)
    #
    # data = [
    #     (ipart, img[:, max(0, ipart * step - 1) : min((ipart + 1) * step + 1, w)])
    #     for ipart in range(partitions)
    # ]
    # for i, frag in enumerate(data):
    #     plt.subplot(1, 8, i + 1)
    #     plt.imshow(frag[1])
    #     plt.axis("off")
    # plt.show()
