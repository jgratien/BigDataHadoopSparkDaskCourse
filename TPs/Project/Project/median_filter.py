from pyspark import SparkContext, SparkConf
import imageio
import numpy as np

def readImg(path):
    img = imageio.imread(path)
    im = np.array(img, dtype='uint8')
    return im

def writeImg(path,buf):
    imageio.imwrite(path,buf)

def part_median_filter(local_data):
    part_id = local_data[0]
    first   = local_data[1]
    end     = local_data[2]
    buf     = local_data[3]
    ny = buf.shape[1]
    nx = buf.shape[0]
    # CREATE NEW BUF WITH MEDIAN FILTER SOLUTION
    new_buf=np.zeros((end-first-1 + 2*(end==(nx-1)),ny,3),dtype='uint8')

    # COMPUTE MEDIAN FILTER
    for i in range(first+int(first==0), end-1+2*int(end==(nx-1))):

        #Left limit (j = 0) : 6-sized median
        selectedPixels = buf[i-1:i+2,0:2,:]
        median = np.median(selectedPixels, axis=(0,1))
        new_buf[i-first-1, 0, :] = median
        #Right limit (j = ny-1) : 6-sized median
        selectedPixels = buf[i-1:i+2,ny-2:ny,:]
        median = np.median(selectedPixels, axis=(0,1))
        new_buf[i-first-1, ny-1, :] = median

        for j in range(1, ny-1):
            selectedPixels = buf[i-1:i+2,j-1:j+2,:]
            # Medians Along 3 RGB Channels components : 9-sized median
            median = np.median(selectedPixels, axis=(0,1))
            new_buf[i-first, j, :] = median
    
    if first==0:
        for j in range(1, ny-1):
            selectedPixels = buf[0:2,j-1:j+2,:]
            # Upper limit (i = 0) : 6-sized median
            median = np.median(selectedPixels, axis=(0,1))
            new_buf[0, j, :] = median

    # RETURN LOCAL IMAGE PART
    return part_id, new_buf

def main():
    # CREATE SPARKCONFIG & SPARKCONTEXT
    sconf = SparkConf()
    sconf.setAppName("medianParallelFilter")
    sconf.setMaster("local[1]")
    sconf.set('spark.executor.memory', '4g')
    sconf.set('spark.executor.cores', 8)

    sc = SparkContext(conf=sconf)

    # LOAD DATA
    file = "lena_noisy.jpg"
    img_buf=readImg(file)
    print('SHAPE',img_buf.shape)
    #print('IMG\n',img_buf)
    nx=img_buf.shape[0]
    ny=img_buf.shape[1]

    # SPLT IMAGES IN NB_PARTITIONS PARTS (ALONG IMAGE ROWS)
    nb_partitions = 8
    print("NB PARTITIONS : ",nb_partitions)
    data=[]
    begin=0
    block_size = nx//nb_partitions
    rest = nx % nb_partitions
    for ip in range(nb_partitions):
        block_size_local = block_size + int(rest > 0 and ip < rest)
        end = min(begin+block_size_local + 1, nx - 1)
        data.append((ip, begin, end, img_buf))
        begin = end - 1


    # PARALLEL MEDIAN FILTER COMPUTATION
    data_rdd = sc.parallelize(data, nb_partitions)
    result_rdd = data_rdd.map(part_median_filter)
    result_data = result_rdd.collect()

    print("CREATE NEW PICTURE FILE")
    new_img_buf = np.zeros((nx, ny, 3), dtype='uint8')

    # COMPUTE NEW IMAGE RESULTS FROM RESULT RDD
    result_data.sort(key=lambda x: x[0])
    partitions = list(zip(*result_data))[1]
    new_img_buf[0:nx,0:ny,:] = np.concatenate(partitions)
    # CORNERS (total of 4 pixels)
    new_img_buf[nx-1,ny-1,:] = img_buf[nx-1,ny-1,:]
    new_img_buf[0,ny-1,:] = img_buf[0,ny-1,:]
    new_img_buf[0,0,:] = img_buf[0,0,:]
    new_img_buf[nx-1,0,:] = img_buf[nx-1,0,:]

    # EXPORTING NEW IMAGE RESULT
    filter_file = 'lena_filter.jpg'
    writeImg(filter_file,new_img_buf)

    sc.stop()

if __name__ == '__main__':
    main()
