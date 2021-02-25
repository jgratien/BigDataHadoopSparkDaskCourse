import pyspark
from pyspark import SparkContext
import imageio
import os
import numpy as np

def readImg(path):
    img = imageio.imread(path)
    im = np.array(img,dtype='uint8')
    return im

def writeImg(path,buf):
    imageio.imwrite(path,buf)

def part_median_filter(local_data):
    part_id = local_data[0]
    first   = int(local_data[1])
    end     = int(local_data[2])
    buf     = local_data[3]
    nx=buf.shape[0]
    ny=buf.shape[1]
    
    ########################################
    #
    # CREATE NEW BUF WITH MEDIAN FILTER SOLUTION
    #
    new_buf=np.empty(shape=(end-first,ny),dtype='uint8')
    
    ##########################################
    #
    # TODO COMPUTE MEDIAN FILTER
    #
    for x in range(end-first):
        for y in range(ny):
            new_buf[x,y] = np.median(buf[max(x+first-1,0):min(x+first+2,nx), max(y-1,0):min(y+2, ny)])
    
    ##########################################
    #
    # RETURN LOCAL IMAGE PART
    #
    return part_id,new_buf

def main():
    path = 'lena_noisy.jpg'
    img_buf=readImg(path)
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
    sc =SparkContext()
    data_rdd = sc.parallelize(data,nb_partitions)	
    
    ###########################################################################
    #
    # PARALLEL MEDIAN FILTER COMPUTATION
    result_rdd = data_rdd.map(part_median_filter)
    result_data = result_rdd.collect()
    print(result_data[0][1].shape)
    
    new_img_buf=np.empty(shape=(nx,ny),dtype='uint8')
    ###########################################################################
    #
    # COMPUTE NEW IMAGE RESULTS FROM RESULT RDD
    # TODO
    for index in range(nb_partitions):
        start = int(data[index][1])
        end = int(data[index][2])
        new_img_buf[start: end, :] = result_data[index][1]

    print('CREATE NEW PICTURE FILE')
    filter_path = 'lena_filter.jpg'
    writeImg(filter_path,new_img_buf)

if __name__ == '__main__':
    main()
