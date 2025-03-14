import os
import numpy as np
import imageio.v2 as imageio
import dask.array as da
from dask.diagnostics import ProgressBar

def read_img(path):
    """Reads an image and returns a NumPy array."""
    img = imageio.imread(path)
    return np.array(img, dtype='uint8')

def write_img(path, buf):
    """Writes a NumPy array as an image."""
    imageio.imwrite(path, buf)

def apply_median(channel):
    """Applies a 3x3 median filter to a single grayscale channel."""
    nx, ny = channel.shape
    filtered_channel = np.zeros_like(channel)

    # Pad edges to prevent border issues
    padded_channel = np.pad(channel, pad_width=1, mode='edge')

    for i in range(nx):
        for j in range(ny):
            neighborhood = padded_channel[i:i+3, j:j+3].flatten()
            filtered_channel[i, j] = np.median(neighborhood)

    return filtered_channel

def median_filter_3x3(image):
    """Applies a 3x3 median filter to a color image (RGB) or grayscale."""
    if len(image.shape) == 2:  # Grayscale
        return apply_median(image)
    
    # Apply median filter per channel for RGB images
    r_filtered = apply_median(image[:, :, 0])
    g_filtered = apply_median(image[:, :, 1])
    b_filtered = apply_median(image[:, :, 2])

    # Merge channels back
    return np.stack([r_filtered, g_filtered, b_filtered], axis=2)

def process_partition(partition):
    """Applies median filter to a partition of an image."""
    return median_filter_3x3(partition)

def main(data_dir='./', input_filename='lena_noisy.jpg', output_filename='lena_filter.jpg', nb_partitions=8):
    """Main function to apply a median filter to an image using Dask."""
    file = os.path.join(data_dir, input_filename)
    img_buf = read_img(file)
    nx, ny, _ = img_buf.shape  # Ensure RGB image

    # Convert the image buffer to a Dask array with appropriate chunk sizes
    img_dask = da.from_array(img_buf, chunks=(nx // nb_partitions, ny, 3))

    # Apply the custom median filter function to each chunk
    filtered_img_dask = img_dask.map_blocks(process_partition, dtype=img_buf.dtype)

    # Compute the result
    with ProgressBar():
        filtered_img = filtered_img_dask.compute()

    # Save output
    write_img(os.path.join(data_dir, output_filename), filtered_img)

if __name__ == '__main__':
    main()
