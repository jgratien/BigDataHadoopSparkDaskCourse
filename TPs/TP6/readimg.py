import imageio
import os
import numpy as np
def readImg(path):
    img = imageio.imread(path)
    im = np.array(img,dtype='uint8')
    return im

def writeImg(path,buf):
    imageio.imwrite(path,buf)

def main():
    data_dir = '/work/gratienj/ParallelProgrammingCourse/BigDataHadoopSpark/TPs/data'
    filename = os.path.join(data_dir,'lena_noisy.jpg')
    print('PATH',filename)
    img_buf=readImg(filename)
    print('SHAPE',img_buf.shape)
    print('IMG\n',img_buf)
    img_buf[:,:,0] = 255
    newfilename = os.path.join(data_dir,'modif_lena_noisy.jpg')
    writeImg(newfilename,img_buf)

if __name__ == '__main__':
    main()
