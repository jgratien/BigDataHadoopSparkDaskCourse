import imageio
import numpy as np
from dask_image import imread
from dask import array as darr
from scipy.ndimage.filters import median_filter

# SCRIPT VARS
IMG = "lena_noisy.jpg"
X_BLOCKSIZE = 8
Y_BLOCKSIZE = 8

# Image Loading & Partitioning
noisy = imread.imread(IMG)
chunked = noisy[0].rechunk(chunks=(X_BLOCKSIZE, Y_BLOCKSIZE, 3))

# Ghost-Cells Creation according to partitioning
extended = darr.overlap.overlap(chunked, depth={0:1, 1:1}, boundary='reflect')

# Non-linear kernel application over parallel blocks
filtered = extended.map_blocks(
    lambda block: median_filter(
        block, footprint=np.ones((3,3,1))
    )
)

# Get rid of ghost cells post-filtering
result = darr.overlap.trim_internal(filtered, {0:1,1:1})

# Write out resulting image
imageio.imwrite("lena_filter_dask.jpg", result)