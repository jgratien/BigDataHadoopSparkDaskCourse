import dask as da
from dask.distributed import Client
from dask import delayed
import pandas as pd
import time
from time import sleep
import os

import matplotlib.pyplot as plt


def inc(x):
    sleep(1)
    return x + 1

def add(x, y):
    sleep(1)
    return x + y


def seq_eval():
    x = inc(1)
    y = inc(2)
    z = add(x, y)

def delayed_eval():
    x = delayed(inc)(1)
    y = delayed(inc)(2)
    z = delayed(add)(x, y)
    return z

def test_delayed():
    client = Client(n_workers=4)

    print("SEQ EVAL:")
    t0 = time.process_time()
    seq_eval()
    t1 = time.process_time() - t0
    print("TIME SEQ :", t1)

    print("DELAYED EVAL:")
    t0 = time.process_time()
    z = delayed_eval()
    t1 = time.process_time() - t0
    print("TIME LAZY :", t1)
    #exit(0)

    t0 = time.process_time()
    z.compute()
    t1 = time.process_time() - t0
    print("TIME COMPUTE :", t1)
    z.visualize()

    client.close()

def seq_out_core_eval(dset):
    # Compute sum of large array, one million numbers at a time
    sums = []
    for i in range(0, 1000000000, 1000000):
        chunk = dset[i: i + 1000000]  # pull out numpy array
        sums.append(chunk.sum())
    total = sum(sums)
    print("TOTAL : ",total)

def dask_out_core_eval(dset):
    import dask.array as da
    x = da.from_array(dset, chunks=(1000000,))
    result = x.sum()
    return result

def test_out_of_memory(data_dir):
    import h5py
    import os

    client = Client(n_workers=4, processes=False)
    f = h5py.File(os.path.join(data_dir,'data', 'random.hdf5'), mode='r')
    dset = f['/x']

    print("SEQ SUM EVAL")
    t0 = time.process_time()
    seq_out_core_eval(dset)
    t1 = time.process_time() - t0
    print("TIME SEQ :", t1)

    print("DASK SUM LAZY EVAL")
    t0 = time.process_time()
    result = dask_out_core_eval(dset)
    t1 = time.process_time() - t0
    print("TIME DASK :", t1)

    print("DASK SUM COMPUTE")
    t0 = time.process_time()
    total = result.compute()
    t1 = time.process_time() - t0
    print("TIME DASK2 :", t1)
    print("TOTAL : ",total)
    client.close()



def test_dask_array(data_dir):
    import dask.array as da
    import h5py
    from glob import glob
    import os

    client = Client(n_workers=4, processes=False)

    filenames = sorted(glob(os.path.join(data_dir,'data', 'weather-big', '*.hdf5')))
    dsets = [h5py.File(filename, mode='r')['/t2m'] for filename in filenames]
    print(dsets[0])

    #fig = plt.figure(figsize=(16, 8))
    #plt.imshow(dsets[0][::4, ::4], cmap='RdBu_r')
    #plt.show()

    arrays = [da.from_array(dset, chunks=(500, 500)) for dset in dsets]
    print(arrays)

    x = da.stack(arrays, axis=0)
    print('X : \n',x)


    print('COMPUTE MEAN X')
    result = x[0] - x.mean(axis=0)

    print('PLOT MEAN X')
    fig = plt.figure(figsize=(16, 8))
    plt.imshow(result, cmap='RdBu_r')
    plt.show()

    client.close()

def test_dataframe(data_dir):
    from dask.distributed import Client
    import dask.dataframe as dd
    from dask import compute
    import glob

    #from prep import accounts_csvs
    #accounts_csvs()

    client = Client(n_workers=4)

    filename = os.path.join(data_dir,'data', 'accounts.*.csv')
    print(filename)

    df = dd.read_csv(filename)
    df.head()
    # load and count number of rows
    print('LEN',len(df))

    df = dd.read_csv(os.path.join(data_dir,'data', 'nycflights', '*.csv'),
                     parse_dates={'Date': [0, 1, 2]})
    print(df)
    df.head()
    df = dd.read_csv(os.path.join(data_dir,'data', 'nycflights', '*.csv'),
                     parse_dates={'Date': [0, 1, 2]},
                     dtype={'TailNum': str,
                            'CRSElapsedTime': float,
                            'Cancelled': bool})
    df.tail()


    filenames = glob.glob(os.path.join(data_dir,'data', 'nycflights', '*.csv'))
    t0 = time.process_time()
    maxes = []
    for fn in filenames:
        df = pd.read_csv(fn)
        maxes.append(df.DepDelay.max())
    final_max = max(maxes)
    t1 = time.process_time() - t0
    print("MAX:",final_max)
    print("TIME COMPUTE MAX :", t1)

    t0 = time.process_time()
    df = dd.read_csv(os.path.join(data_dir, 'data', 'nycflights', '*.csv'),
                     parse_dates={'Date': [0, 1, 2]},
                     dtype={'TailNum': str,'CRSElapsedTime': float,'Cancelled': bool})
    print("DASK MAX:",df.DepDelay.max().compute())
    t1 = time.process_time() - t0
    print("TIME DASK COMPUTE MAX :", t1)
    df.DepDelay.max().visualize()

    help(df.CRSDepTime.map_partitions)
    client.close()

def test_map_partition(data_dir):
    from dask.distributed import Client
    import dask.dataframe as dd
    from dask import compute
    import glob


    client = Client(n_workers=4)

    df = dd.read_csv(os.path.join(data_dir,'data', 'nycflights', '*.csv'),
                     parse_dates={'Date': [0, 1, 2]},
                     dtype={'TailNum': str,
                            'CRSElapsedTime': float,
                            'Cancelled': bool})
    df.tail()

    help(df.CRSDepTime.map_partitions)
    client.close()

def load_array(ip,nx,ny):
    import numpy as np
    a = np.zeros((nx,ny),dtype=int)
    a[:,:] = ip
    return a

def test5():
    import numpy as np
    from dask.distributed import Client
    client = Client(n_workers=4)
    nx=10
    ny=5
    partitions = [0,1,2,3]
    arrays = [ delayed(load_array)(ip,nx,ny) for ip in partitions]
    results = da.compute(*arrays)
    for r in results:
        print('R:',r.shape)
    new_array = np.concatenate(results,axis=0)
    print('NEW SHAPE',new_array.shape)
    print("NEW ARRAY",new_array)

    client.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--test", help="no du test", default="0")
    args = parser.parse_args()
    data_dir = '/work/gratienj/ParallelProgrammingCourse/GIT/BigDataHadoopSparkDaskCourse/TPs/data/dask'
    test_id = int(args.test)
    if test_id == 0:
        test_delayed()
    if test_id == 1:
        test_out_of_memory(data_dir)
    if test_id == 2:
        test_dask_array(data_dir)
    if test_id == 3:
        test_dataframe(data_dir)
    if test_id == 4:
        test_map_partition(data_dir)
    if test_id == 5:
        test5()


