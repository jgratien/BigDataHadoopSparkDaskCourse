# Import libraries
import os
import dask
from dask import delayed
import dask_image.imread
import dask_image.ndfilters
import dask_image.ndmeasure
import dask.array as da
import numpy as np
import matplotlib.pyplot as plt

# Reading the images into a dask array
imgs_path = os.path.join('flower_images', '0*.png')
flowers_images = dask_image.imread.imread(imgs_path)
print("Initial shape : ", flowers_images.shape)

# To GrayScale :
## GrayScale funct :
def to_grayscale(rgb):
    grayscaled = ((rgb[..., 0] * 0.2125) +
              (rgb[..., 1] * 0.7154) +
              (rgb[..., 2] * 0.0721))
    return grayscaled

## Apply to all images :
flowers_imagesGS = to_grayscale(flowers_images)
print("After GrayScale : ", flowers_imagesGS.shape)

# Segmentation pipeline = filtering, intensifying and segmenting :
## functions :
def filter_img(image):
    return dask_image.ndfilters.gaussian_filter(da.from_array(image), sigma=[0.2, 0.2])
def segment_img(image):
    return dask_image.ndmeasure.label(image)

## Transform images :
print("Starting transformation ...\n")

flowers_seg = []
for flower in flowers_imagesGS:
   filtered = delayed(filter_img)(flower)
   intensified = delayed(lambda z : z > (0.5 * da.max(z)))(filtered)
   segmented = delayed(segment_img)(intensified)
   flowers_seg.append(segmented)
res = dask.compute(flowers_seg)

## Gather results :
flower_results = res[0]
print("=============== Done ===============")

# Plotting results :
plt.hist([flower_results[i][1] for i in range(210)], bins=20)
plt.savefig('flowers_hist.png')
