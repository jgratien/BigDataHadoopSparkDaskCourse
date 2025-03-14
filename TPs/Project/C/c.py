import os
import numpy as np
import imageio.v2 as imageio
from pyspark import SparkContext

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

def part_median_filter(local_data):
    """Applies median filter to a partition of an RGB image."""
    part_id, first, end, buf = local_data
    filtered_local_buf = median_filter_3x3(buf)
    return part_id, filtered_local_buf

def main(data_dir='./', input_filename='lena_noisy.jpg', output_filename='lena_filter.jpg', nb_partitions=8):
    """Main function to apply a median filter to an image using PySpark."""
    file = os.path.join(data_dir, input_filename)
    img_buf = read_img(file)
    nx, ny, _ = img_buf.shape  # Ensure RGB image

    # Partitioning the image (keep all channels)
    block_size = nx // nb_partitions
    data = []
    for ip in range(nb_partitions):
        first = ip * block_size
        end = (ip + 1) * block_size if ip < nb_partitions - 1 else nx
        data.append((ip, first, end, img_buf[first:end, :, :]))  # Keep RGB channels

    # Create SparkContext
    sc = SparkContext.getOrCreate()
    data_rdd = sc.parallelize(data, nb_partitions)

    # Parallel median filter computation
    result_rdd = data_rdd.map(part_median_filter)
    result_data = result_rdd.collect()

    # Sort and reconstruct image
    result_data.sort(key=lambda x: x[0])
    new_img_buf = np.vstack([part[1] for part in result_data])

    # Save output
    write_img(os.path.join(data_dir, output_filename), new_img_buf)

    # Stop SparkContext
    sc.stop()

if __name__ == '__main__':
    main()