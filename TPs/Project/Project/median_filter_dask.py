import numpy as np
from dask_image import imread
import imageio


# Partial Median Filter
def partialMedianFun(array, xStart, xEnd, yStart, yEnd, chunkShape):
    chunkResult = np.ndarray(chunkShape, dtype=np.uint8)
    for i in range(1,xEnd-xStart-1):
        
        for j in range(1, yEnd-yStart-1):
            selectedPixels = array[ i-1: i+2,j-1:j+2,:]
            median = [int(val) for val in np.median(selectedPixels, axis=(0,1))]
            chunkResult[i, j, :] = median
            
    return(chunkResult)

# Partial Medial Filter Wrapper
def partial_median(block, block_info=None):

    if not block_info is None :
        (xStart, xEnd) = block_info[None]['array-location'][0]
        (yStart, yEnd) = block_info[None]['array-location'][1]

        chunkShape = block_info[None]['chunk-shape']           
        
        return(partialMedianFun(block, xStart, xEnd, yStart, yEnd, chunkShape))
        
    return partialMedianFun(block, 0, 0, 0, 0, (8,8,3))


def main():
    #Loading Image
    file = "lena_noisy.jpg"
    noisy = imread.imread(file)

    # Explicit Partitionning
    blockDimX = 8
    blockDimY = 8
    blockDimZ = 3

    chunkedImage = noisy[0].rechunk(chunks=(blockDimX, blockDimY, blockDimZ))

    # Overlapped Mapping to Median Filter for parallel execution
    filteredResult = chunkedImage.map_overlap(partial_median, depth={0:1,1:1,2:0}, 
                                boundary={0: 'reflect', 1: 'reflect'},
                                trim=True)

    # Exporting jpg output
    imageio.imwrite("lena_filter_dask.jpg", filteredResult)

if __name__ == '__main__':
    main()