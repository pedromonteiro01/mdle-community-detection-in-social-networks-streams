# exercise2_2
# # MDLE - Spark Streaming
# ### Exercise 2.2
# ##### Authors: Pedro Duarte 97673, Pedro Monteiro 97484

# %% [markdown]
# Import Necessary Libraries

# %%
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from operator import itemgetter
from itertools import chain
from pyspark import SparkConf

# %% [markdown]
# If there's an active SparkContext, stop it before creating a new one

# %%
if 'sc' in locals():
    sc.stop()

# %% [markdown]
# Set up a Spark Streaming application

# %%
conf = SparkConf().setAppName("Exercise2-2").setMaster("local[*]") 
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, batchDuration=5)
ssc.checkpoint("dir")

# %% [markdown]
# Declare decay factor

# %%
C = 10 ** -6 # decay factor

# %% [markdown]
# Initialize a stream
# 
# Parameters:
# - stream (list): List representing the stream or None
# 
# Returns:
# - list: Initialized stream as a list

# %%
def init_stream(stream):
    return [[], []] if stream is None else stream

# %% [markdown]
# Retrieves the distinct elements from a stream
# 
# Parameters:
# - stream: List representing the stream
# 
# Returns:
# - set: Set containing the distinct elements from the stream

# %%
def get_distinct_elements(stream):
    return set(stream)

# %% [markdown]
# Calculates the weight for a specific element in a stream
# 
# Parameters:
# - elem: Element for which the weight is calculated
# - stream: List representing the stream
# 
# Returns:
# - int: Weight of the element in the stream

# %%
def calculate_weight_for_element(elem, stream):
    count = 1 if stream[0] == elem else 0 # initialize count to 1 if first element matches 'elem'
    for inc_elem_idx in range(len(stream)):
        if inc_elem_idx == 0:
            count = count * (1 - C) # decaying factor applied to the initial count
            continue
        bit = 1 if elem == stream[inc_elem_idx] else 0 # set 'bit' to 1 if current element matches 'elem', otherwise 0
        count = count * (1 - C) + bit # update count with decaying factor and bit value
    return count

# %% [markdown]
# Updates two lists, 'values' and 'weights', based on a given element and its corresponding count
# 
# Parameters:
# - elem: Wlement to be added or updated
# - count: Count associated with the element
# - values (list): List of elements
# - weights (list): List of corresponding weights
# 
# Returns:
# - tuple: Tuple containing the updated 'values' and 'weights' lists.

# %%
def update_sample_lists(elem, count, values, weights):
    if elem not in values: # check if 'elem' is not already present in 'values'
        values.append(elem)
        weights.append(round(count))
    else:
        idx = values.index(elem) # find the index of 'elem' in 'values'
        weights[idx] += round(count) # # add 'count' to the corresponding weight in 'weights'
    return values, weights

# %% [markdown]
# Processes an incoming stream of elements and updates a sample window based on distinct elements and their weights
# 
# Parameters:
# - incoming_stream: List representing the incoming stream of elements
# - previous_stream: List representing the previous state of the stream
# 
# Returns:
# - list: List containing sample values and their corresponding weights

# %%
def decaying_window(incoming_stream, previous_stream):
    previous_stream = init_stream(previous_stream) # get 'previous_stream' using 'init_stream' function
    distinct_elems = get_distinct_elements(incoming_stream) # get distinct elements from 'incoming_stream'
    sample_values = [] # store sample values
    sample_weights = [] # store sample weights

    for elem in distinct_elems:
        count = calculate_weight_for_element(elem, incoming_stream) # calculate element weight
        sample_values, sample_weights = update_sample_lists(elem, count, sample_values, sample_weights) # update lists
    
    return [sample_values, sample_weights]

# %% [markdown]
# transform_to_pairs function:
# Transforms a pair of lists into pairs of corresponding elements
# 
# Parameters:
# - x: pair of lists
# 
# Returns:
# - zip: Sip object containing pairs of corresponding elements
# 
# get_sorted_counts function:
# Takes an RDD and returns a sorted RDD based on the counts of the elements
# 
# Parameters:
# - rdd: An RDD
# 
# Returns:
# - RDD: Sorted RDD based on the counts of the elements

# %%
def transform_to_pairs(x):
    return zip(x[0], x[1])

def get_sorted_counts(rdd):
    counts_dict = rdd.map(lambda x: x[1]) \
            .flatMap(transform_to_pairs)
    
    ordered_dict = counts_dict.sortBy(itemgetter(1), ascending = False)

    return ordered_dict

# %% [markdown]
# Create a socket connection to a server using the socketTextStream method 

# %%
try:
    lines = ssc.socketTextStream("localhost", 9998) # connect to localhost server
except ConnectionRefusedError: # handle connection refused error
    print("Connection was refused by the server.")

# %% [markdown]
# Using Spark Streaming to process data from a socket stream

# %%
pairs = lines.map(lambda line: line.split(" ")[1].split(",")) # get pairs of values extracted from the lines
pre_sampled_data = pairs.map(lambda mention: (0, mention[1])) # prepare data for stateful processing
sampled_data = pre_sampled_data.updateStateByKey(decaying_window) # update state by key  and compute sample values and weights based on the incoming data
ordered_counts = sampled_data.transform(get_sorted_counts) # get a sorted RDD based on the counts
ordered_counts.pprint(5) # print first 5 element

ssc.start()
ssc.awaitTermination()


