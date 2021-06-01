import dask.array as da
x = da.ones((15, 15), chunks=(5, 5))
print(x.compute())
y = x + x.T
# y.compute()
y.visualize(filename='transpose.png')