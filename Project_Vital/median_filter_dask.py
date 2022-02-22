import dask_image.imread
import dask_image.ndfilters
import dask_image.ndmeasure
from dask.distributed import Client
import dask.array as da
import imageio
import os
import numpy as np

def readImg(path):
    img = imageio.imread(path)
    im = np.array(img,dtype='uint8')
    #img = dask_image.imread.imread(path)
    nb_partitions = 8
    print("NB PARTITIONS : ",nb_partitions)
    chunk_size = [img.shape[1]//8, img.shape[1], img.shape[2]]
    img = da.from_array(im, chunks=chunk_size)
    return img

def grayscale(rgb):
    result = ((rgb[..., 0] * 0.2125) +
              (rgb[..., 1] * 0.7154) +
              (rgb[..., 2] * 0.0721))
    return result

def writeImg(path,buf):
    imageio.imwrite(path,buf)

def part_median_filter(local_img):
    nx=local_img.shape[0]
    ny=local_img.shape[1]
    
    ##########################################
    #
    # TODO COMPUTE MEDIAN FILTER
    #
    for i in range(nx):
        for j in range(ny):
            median_list = []
            
            if i != 0 and j != 0:
                median_list.append(local_img[i-1,j-1])
            if i != 0:
                median_list.append(local_img[i-1,j])
            if i != 0 and j != ny-1:
                median_list.append(local_img[i-1,j+1])
            if j != 0:
                median_list.append(local_img[i,j-1])
                
            median_list.append(local_img[i,j])
            
            if j != ny-1:
                median_list.append(local_img[i,j+1])
            if i != nx-1 and j != 0:
                median_list.append(local_img[i+1,j-1])
            if i != nx-1:
                median_list.append(local_img[i+1,j])
            if i != nx-1 and j != ny-1:
                median_list.append(local_img[i+1,j+1])
            
            if grey_bool:
                local_img[i, j] = np.median(median_list)
            else:
                local_img[i, j, :] = np.median(median_list, axis=0)
    ##########################################
    #
    # RETURN LOCAL IMAGE PART
    #
    return local_img

def main():
    data_dir = "data_project/"
    file = os.path.join(data_dir,'lena_noisy.jpg')
    img_buf=readImg(file)
    if grey_bool:
        img_buf=rgb2gray(img_buf)
    print('SHAPE',img_buf.shape)

    
    ###########################################################################
    #
    # SPLT IMAGES IN NB_PARTITIONS PARTS
    nb_partitions = 8
    print("NB PARTITIONS : ",nb_partitions)
    
    ###########################################################################
    #
    # PARALLEL MEDIAN FILTER COMPUTATION
    new_img_buf = img_buf.map_overlap(part_median_filter, depth=(1, img_buf.shape[1])).compute()

    ###########################################################################

    print('CREATE NEW PICTURE FILE')
    filter_file = os.path.join(data_dir,'lena_filter_dask.jpg')
    writeImg(filter_file,new_img_buf)

if __name__ == '__main__':
    grey_bool = False
    client = Client(n_workers=8)
    main()
    client.close()