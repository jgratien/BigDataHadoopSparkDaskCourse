import pyspark
from pyspark import SparkContext
import imageio
import os
import numpy as np
from itertools import product
from functools import partial
from pathlib import Path

PATH = Path().absolute() / "data"


def get_indexes(i, j, w, h):
    dimin = -1 if i > 0 else 0
    dimax = 1 if i < w - 1 else 0
    djmin = -1 if j > 0 else 0
    djmax = 1 if j < h - 1 else 0
    return product(range(dimin, dimax + 1), range(djmin, djmax + 1))


def median_filter(image):
    w, h, d = image.shape
    new_image = np.zeros(image.shape, dtype=np.uint8)
    for iw in range(w):
        for jh in range(h):
            values = [image[iw + di, jh + dj] for di, dj in get_indexes(iw, jh, w, h)]
            new_image[iw, jh] = np.median(values, axis=0)
    return new_image


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


def main():
    img_buf = np.array(imageio.imread(str(PATH / "lena_noisy.jpg")), dtype="uint8")

    # SPLIT IMAGES IN NB_PARTITIONS PARTS
    partitions = 8
    h, w, d = img_buf.shape
    step = w // partitions

    data = [
        img_buf[:, max(0, ipart * step - 1) : min((ipart + 1) * step + 1, w)]
        for ipart in range(partitions)
    ]

    # CREATE SPARKCONTEXT
    sc = SparkContext()
    data_rdd = sc.parallelize(data, 8)

    # PARALLEL MEDIAN FILTER COMPUTATION
    result_rdd = data_rdd.map(median_filter)
    result_data = result_rdd.collect()

    # COMPUTE NEW IMAGE RESULTS FROM RESULT RDD
    processed_data = map(partial(crop, reference=partitions), enumerate(result_data))
    new_img_buf = np.concatenate(list(processed_data), axis=1)
    print(new_img_buf.shape)

    imageio.imwrite(str(PATH / "lena_filter.jpg"), new_img_buf)
    print("Image saved.")


if __name__ == "__main__":
    main()
