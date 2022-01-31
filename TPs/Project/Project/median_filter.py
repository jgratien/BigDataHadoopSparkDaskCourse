import pyspark
from pyspark import SparkContext
import imageio
import os
import numpy as np
from scipy.ndimage import median_filter

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
    buf     = np.array(local_data[3])

    ########################################
    #
    # CREATE NEW BUF WITH MEDIAN FILTER SOLUTION
    #
    new_buf=np.array(buf[first:end,:,:],dtype='uint8')
    
    ##########################################
    #
    # COMPUTE MEDIAN FILTER
    #
    new_buf = np.array(median_filter(new_buf, size=(3,3,3)),dtype='uint8')
    
    ##########################################
    #
    # RETURN LOCAL IMAGE PART
    #
    return part_id,new_buf

import os
def main():
    # print("PATH :", os.getcwd())
    data_dir = '/home/aympab/repos/BigDataHadoopSparkDaskCourse/TPs/Project'
    file = os.path.join(data_dir,'lena_noisy.jpg')
    # print("FILE:",file)
    img_buf=readImg(file)
    print('SHAPE',img_buf.shape)
    # print('IMG\n',img_buf)
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
    result_data = np.array(result_rdd.collect())

    ###########################################################################
    #
    # COMPUTE NEW IMAGE RESULTS FROM RESULT RDD
    new_img_buf=np.concatenate(result_data[:,1][:])
    
    print('CREATE NEW PICTURE FILE')
    filter_file = os.path.join(data_dir,'lena_filter.jpg')
    writeImg(filter_file,new_img_buf)

    sc.stop()

if __name__ == '__main__':
    main()
