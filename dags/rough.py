import numpy as np
import time

# Create a large array of random integers
size = 10_000_000
data = np.random.randint(0, 100, size)

# Scalar (row-by-row) processing
def scalar_sum(arr):
    total = 0
    for num in arr:
        total += num
    return total

# Vectorized processing
def vectorized_sum(arr):
    return np.sum(arr)

# Time the scalar method
start = time.time()
scalar_result = scalar_sum(data)
scalar_time = time.time() - start

# Time the vectorized method
start = time.time()
vectorized_result = vectorized_sum(data)
vectorized_time = time.time() - start

print(f"Scalar sum: {scalar_result}")
print(f"Vectorized sum: {vectorized_result}")
print(f"Scalar processing time: {scalar_time:.6f} seconds")
print(f"Vectorized processing time: {vectorized_time:.6f} seconds")
print(f"Speedup: {scalar_time / vectorized_time:.2f}x")