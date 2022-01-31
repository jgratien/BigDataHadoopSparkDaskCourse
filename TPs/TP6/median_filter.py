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
    first   = local_data[1]
    end     = local_data[2]
    buf     = local_data[3]
    nx=buf.shape[0]
    ny=buf.shape[1]
    
    ########################################
    #
    # CREATE NEW BUF WITH MEDIAN FILTER SOLUTION
    #
    new_buf=np.array([end-first,ny],dtype='uint8')
    
    ##########################################
    #
    # TODO COMPUTE MEDIAN FILTER
    #
    
    
    ##########################################
    #
    # RETURN LOCAL IMAGE PART
    #
    return part_id,new_buf

def main():
    data_dir = '/gext/jean-marc.gratien/BigDataHadoopSpark/TPs/data'
    file = os.path.join(data_dir,'lena_noisy.jpg')
    img_buf=readImg(file)
    print('SHAPE',img_buf.shape)
    print('IMG\n',img_buf)
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

    new_img_buf=np.array([nx,ny],dtype='uint8')
    ###########################################################################
    #
    # COMPUTE NEW IMAGE RESULTS FROM RESULT RDD
    # TODO
    print('CREATE NEW PICTURE FILE')
    filter_file = os.path.join(data_dir,'lena_filter.jpg')
    writeImg(filter_file,new_img_buf)

if __name__ == '__main__':
    main()
