# Importing required libraries
import numpy as np
from dask_image import imread
from dask import array as darr
import matplotlib.pyplot as plt
##warnings.simplefilter("ignore", RuntimeWarning)
# Defined Constants
image = "lena_noisy.jpg"
x_block = 8
y_block = 8

noisy = imread.imread(image)
plt.imshow(noisy[0])
plt.show()

chunked = noisy[0].rechunk(chunks=(x_block, y_block, 3))
extended = darr.overlap.overlap(chunked, depth={0:1,1:1}, boundary='nearest')

from scipy.ndimage.filters import median_filter
def partial_median(block):
    return median_filter(block, footprint=np.ones((3,3,1)))

filtered = extended.map_blocks(partial_median)

result = darr.overlap.trim_internal(filtered, {0:1,1:1})
plt.imshow(result)
plt.show()
