# %% [markdown]
# # MDLE - Spark Streaming
# ### Exercise 2.1
# ##### Authors: Pedro Duarte 97673, Pedro Monteiro 97484

# %% [markdown]
# Import Necessary Libraries

# %%
from collections import deque
from pyspark.sql import SparkSession

# %% [markdown]
# Declare Constants

# %%
N = 24  # Window size
K = 20  # Number of bits for counting query

# %% [markdown]
# Set up the Spark application with a streaming DataFrame that reads data from a socket

# %%
spark = SparkSession.builder.appName("StructuredNetworkBitStream").getOrCreate()
stream_data = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# %% [markdown]
# Define the DGIM class Implementation

# %%
class Implementation:
    def __init__(self, window_size):
        self.window_size = window_size
        self.bit_stream = deque(maxlen=window_size)  # create a deque to store the stream of bits
        self.buckets = []  # list to store the buckets
        self.real_count = 0  # real value count
        self.discarded_buckets = 0  # number of discarded buckets

    def update_stream(self, bit):
        self.bit_stream.appendleft(bit)  # add the new bit to the beginning of the deque
        if bit == 1:
            self.real_count += 1  # if the new bit is 1, increment the real count by 1
            self.buckets.append(1)  # append a new bucket with a value of 1 to the buckets list
        self.merge_buckets()  # regardless of the bit's value, we should check if there are any buckets to merge
        self.remove_discarded_buckets()  # regardless of the bit's value, we should check if there are any buckets to discard

    def merge_buckets(self):
        while len(self.buckets) >= 3:
            if self.buckets[-3] == self.buckets[-2]:  # check if the last three buckets have the same value
                self.buckets[-2] += self.buckets.pop(-2)  # if have the same value, merge
            else:
                break

    def remove_discarded_buckets(self):
        # check if there are buckets and if the size of the bit stream is greater than the window size
        if len(self.buckets) > 0 and len(self.bit_stream) == self.window_size:
            self.buckets.pop(0)
            self.discarded_buckets += 1
            
    def estimate_count(self): # calculate the estimation
        count = sum(self.buckets)
        for i in range(self.discarded_buckets):
            count += 2 ** i
        return count

# %% [markdown]
# Using the Implementation class to process a stream of bits and apply DGIM algorithm

# %%
dgim_impl = Implementation(N)

def calc(df, n):
    for row in df.collect():
        bit = int(row["value"])
        dgim_impl.update_stream(bit)

    estimated = dgim_impl.estimate_count()
    print("Real count is", dgim_impl.real_count)
    print("Estimated", estimated)
    print("Error", (dgim_impl.real_count-estimated)/dgim_impl.real_count if dgim_impl.real_count > 0 else 0)
    print('-'*30)

query = stream_data.writeStream.foreachBatch(calc).start()
query.awaitTermination()


