from os import path
import numpy as np
from dask_image import imread
from dask_image.ndmeasure import label
from dask import array as darr
import matplotlib.pyplot as plt

flowers_data = imread.imread("flower_images/*.png")

def grayscale(data):
    gray_image = ((data[..., 0] * 0.2125) +
              (data[..., 1] * 0.7154) +
              (data[..., 2] * 0.0721))
    return gray_image
data_gray = grayscale(flowers_data)

def max_filter(block):
    return block > (0.75 * block.max())

max_filtered = data_gray.map_blocks(max_filter)

maxed_data = max_filtered.map_blocks(lambda x: label(x)[0])

num_components = maxed_data.max(axis=(1,2)).compute().compute()
plt.hist(num_components)
plt.xlabel("num_components")
plt.ylabel("number of images")
plt.title("Images with respective number of main components")
plt.show()
