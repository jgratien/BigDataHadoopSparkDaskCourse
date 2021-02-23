from pyspark import SparkContext
import numpy as np
import os, imageio

def readImg(path):
    img = imageio.imread(path)
    im = np.array(img, dtype='uint8')
    return im

def writeImg(path, buf):
    imageio.imwrite(path, buf)

def part_median_filter(local_data):
    part_id = local_data[0]
    first   = local_data[1]
    end     = local_data[2]
    buf     = local_data[3]
    ny      = buf.shape[1]
    
    # CREATE NEW BUF WITH MEDIAN FILTER SOLUTION
    new_buf = np.array([end-first-1, ny-2], dtype='uint8')
    
    # TODO COMPUTE MEDIAN FILTER
    for i in range(first+1, end):
        for j in range(1, ny-1):
            median = np.median(
                (buf[i-1,j-1], buf[i-1,j], buf[i-1,j+1],
                buf[i,j-1], buf[i,j], buf[i,j+1],
                buf[i+1,j-1], buf[i+1,j], buf[i+1,j+1]),
                axis=0)
            imedian = np.array([int(k) for k in median])
            new_buf[i-first-1,j-1] = imedian
    
    # RETURN LOCAL IMAGE PART
    return part_id, new_buf

def main():
    # CREATE SPARKCONTEXT
    sc = SparkContext()
    sc.setLogLevel("ERROR")

    data_dir = '/gext/rami.kader/hpcai/HPDA/BigDataHadoopSparkDaskCourse/TPs/data'
    file = os.path.join(data_dir, 'lena_noisy.jpg')
    img_buf = readImg(file)
    print('SHAPE', img_buf.shape)
    print('IMG\n', img_buf)
    nx = img_buf.shape[0]
    ny = img_buf.shape[1]
    
    # SPLT IMAGES IN NB_PARTITIONS PARTS
    nb_partitions = 8
    print("NB PARTITIONS : ", nb_partitions)
    data = []
    begin = 0
    block_size = nx / nb_partitions
    for ip in range(nb_partitions):
        end = min(begin + block_size + 1, nx - 1)
        data.append((ip, begin, end, img_buf))
        begin = end - 1

    
    # PARALLEL MEDIAN FILTER COMPUTATION
    data_rdd = sc.parallelize(data, nb_partitions)	
    result_rdd = data_rdd.map(part_median_filter)
    result_data = result_rdd.collect()

    print('CREATING NEW PICTURE FILE')
    new_img_buf = np.array([nx, ny], dtype='uint8')
    new_img_buf[:,0] = img_buf[:,0]
    new_img_buf[:,-1] = img_buf[:,-1]
    new_img_buf[0,:] = img_buf[0,:]
    new_img_buf[-1,:] = img_buf[-1,:]
    
    # COMPUTE NEW IMAGE RESULTS FROM RESULT RDD
    result_data.sort(key=lambda x: x[0])
    parts = list(zip(*result_data))[1]
    new_img_buf[1:-1,1:-1] = np.concatenate(parts)

    filter_file = os.path.join(data_dir,'lena_filter.jpg')
    writeImg(filter_file, new_img_buf)

if __name__ == '__main__':
    main()
