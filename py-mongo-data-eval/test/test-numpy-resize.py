import numpy as np

def inplace_reshape(a):
    a.shape = (10000,-1)

def inplace_resize(a):
    a.resize((10000,3))

def reshaped_view(a):
    a = np.reshape(a, (10000,-1))

def resized_copy(a):
    a = np.resize(a, (10000,3))


a = np.random.random((30000,2))

print(len(a))
print(a.shape)
print(a.shape[1])


# %timeit inplace_reshape(a)
# # 383 ns ± 14.1 ns per loop (mean ± std. dev. of 7 runs, 1000000 loops each)
#
# %timeit inplace_resize(a)
# # 294 ns ± 20.8 ns per loop (mean ± std. dev. of 7 runs, 1000000 loops each)
#
# %timeit reshaped_view(a)
# # 1.5 µs ± 25.8 ns per loop (mean ± std. dev. of 7 runs, 1000000 loops each)
#
# %timeit resized_copy(a)
# # 21.5 µs ± 289 ns per loop (mean ± std. dev. of 7 runs, 10000 loops each)