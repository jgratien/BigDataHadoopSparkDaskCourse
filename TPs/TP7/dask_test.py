from dask.distributed import LocalCluster, Client
import time
import dask.array as da

if __name__ == '__main__':
    cluster = LocalCluster()
    client = Client(cluster)

    x = da.random.random((100000, 100000), chunks=(2000, 2000))
    y = x + x.T
    z = y[::2, 5000:].mean(axis=1)

    result = z.compute()
    print(result)

    cluster.close()