import pyspark
from pyspark import SparkContext
import imageio
import os
import numpy as np


def readImg(path):
    img = imageio.imread(path)
    im = np.array(img,dtype='uint8')
    return im

def rgb2gray(rgb):
    r, g, b = rgb[:,:,0], rgb[:,:,1], rgb[:,:,2]
    gray = 0.2989 * r + 0.5870 * g + 0.1140 * b

    return gray

def writeImg(path,buf):
    imageio.imwrite(path,buf)

def part_median_filter(local_data):
    part_id = local_data[0]
    first   = local_data[1]
    end     = local_data[2]
    buf     = local_data[3]
    nx=buf.shape[0]
    ny=buf.shape[1]
    
    ########################################
    #
    # CREATE NEW BUF WITH MEDIAN FILTER SOLUTION
    #
    r = int(end-first)
    new_buf = np.zeros([r, ny],dtype='uint8') if grey_bool else np.zeros([r, ny, 3],dtype='uint8')

    ##########################################
    #
    # TODO COMPUTE MEDIAN FILTER
    #
    #print(part_id, first, end, buf.shape, nx, ny)
    for i in range(int(first), int(end)):
        for j in range(ny):
            median_list = []
            
            if i != 0 and j != 0:
                median_list.append(buf[i-1,j-1])
            if i != 0:
                median_list.append(buf[i-1,j])
            if i != 0 and j != ny-1:
                median_list.append(buf[i-1,j+1])
            if j != 0:
                median_list.append(buf[i,j-1])
                
            median_list.append(buf[i,j])
            
            if j != ny-1:
                median_list.append(buf[i,j+1])
            if i != end-1 and j != 0:
                median_list.append(buf[i+1,j-1])
            if i != end-1:
                median_list.append(buf[i+1,j])
            if i != end-1 and j != ny-1:
                median_list.append(buf[i+1,j+1])
            
            if grey_bool:
                new_buf[int(i-first), j] = np.median(median_list)
            else:
                new_buf[int(i-first), j, :] = np.median(median_list, axis=0)
    
    
    ##########################################
    #
    # RETURN LOCAL IMAGE PART
    #
    return part_id,new_buf

def main():
    data_dir = "data_project/"
    file = os.path.join(data_dir,'lena_noisy.jpg')
    img_buf=readImg(file)
    if grey_bool:
        img_buf=rgb2gray(img_buf)
    print('SHAPE',img_buf.shape)
    #print('IMG\n',img_buf)
    nx=img_buf.shape[0]
    ny=img_buf.shape[1]
    
    ###########################################################################
    #
    # SPLT IMAGES IN NB_PARTITIONS PARTS
    nb_partitions = 8
    print("NB PARTITIONS : ",nb_partitions)
    data=[]
    begin=0
    block_size=nx/nb_partitions
    for ip in range(nb_partitions):
        end=min(begin+block_size,nx)
        data.append([ip,begin,end,img_buf])
        begin=end
    
    
    ###########################################################################
    #
    # CREATE SPARKCONTEXT
    sc =SparkContext.getOrCreate();
    data_rdd = sc.parallelize(data,nb_partitions)
    
    ###########################################################################
    #
    # PARALLEL MEDIAN FILTER COMPUTATION
    result_rdd = data_rdd.map(part_median_filter)
    result_data = result_rdd.collect()

#     new_img_buf=np.zeros([nx,ny],dtype='uint8')
    ###########################################################################
    #
    # COMPUTE NEW IMAGE RESULTS FROM RESULT RDD
    # TODO
    new_img_buf = np.concatenate([part[1] for part in result_data])

    print('CREATE NEW PICTURE FILE')
    filter_file = os.path.join(data_dir,'lena_filter.jpg')
    writeImg(filter_file,new_img_buf)

if __name__ == '__main__':
    grey_bool = False
    main()
