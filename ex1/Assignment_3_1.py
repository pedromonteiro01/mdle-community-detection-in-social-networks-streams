# Assignment_3_1
# # MDLE - Community Detection in Social Networks Streams
# ### Exercise 1
# ##### Authors: Pedro Duarte 97673, Pedro Monteiro 97484

# %% [markdown]
# Import Necessary Libraries

# %%
import numpy as np
import pandas as pd
import networkx as nx
import scipy

from sklearn.cluster import SpectralClustering, KMeans
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.clustering import PowerIterationClustering

import matplotlib.pyplot as plt
import matplotlib.colors as pltcolors

# %% [markdown]
# Compute the sorted eigendecomposition of the Laplacian matrix of a graph.
# 
# Parameters:
# - G: NetworkX graph object
#     Graph according to the eigendecomposition
# - k: int
#     Number of eigenvalues and eigenvectors
# 
# Returns:
# - eigenvalues: numpy array
#     Eigenvalues sorted in ascending order
# - eigenvectors: numpy array
#     Eigenvectors sorted based on eigenvalues

# %%
def get_sorted_eigendecomposition(G, k):

  # compute Laplacian matrix, eigenvalues and eigenvectors
  laplacian_matrix = nx.normalized_laplacian_matrix(G).toarray()
  eigenvalues, eigenvectors = scipy.sparse.linalg.eigs(laplacian_matrix, k)

  # check if the eigenvalues or eigenvectors have complex values
  if np.iscomplexobj(eigenvalues) or np.iscomplexobj(eigenvectors):
    # convert to real values
    eigenvalues = np.real(eigenvalues)
    eigenvectors = np.real(eigenvectors)
  
  # sort eigenvalues and eigenvectors in ascending order
  sorted_indices = np.argsort(eigenvalues)
  eigenvalues = eigenvalues[sorted_indices]
  eigenvectors = eigenvectors[:, sorted_indices]

  return eigenvalues, eigenvectors

# %% [markdown]
# Determine the ideal number of clusters based on the eigenvalues of a graph Laplacian.
# 
# Parameters:
# - eigenvalues: numpy array
#     Eigenvalues sorted in ascending order
# 
# Returns:
# - n_clusters: int
#     Ideal number of clusters based on the eigenvalue spectrum

# %%
def ideal_clusters_n(eigenvalues):
  eigengap = np.diff(eigenvalues) # difference between consecutive eigenvalues

  eigengap_peaks = np.where(np.diff(eigengap) < 0)[0] # indices where the difference between consecutive eigengap values becomes negative
  return eigengap_peaks[0] + 1

# %% [markdown]
# Preview a clustering result by drawing nodes with different colors based on their cluster labels
# 
# Parameters:
# - G: NetworkX graph object
#     Graph to be visualized
# - pos: dictionary
#     Node positions
# - cluster_labels: list
#     List of cluster labels for each node in the graph

# %%
def preview_clustering(G, pos, cluster_labels):
  # Draw the nodes with different colors based on their cluster labels
  next_color = [.6, .4, .3]
  for cluster_label in range(20):
      # generate a new color for each cluster label
      next_color[(cluster_label*5)%3] = (next_color[(cluster_label)%3] + .28*cluster_label) % 1
      # get current cluster label nodes
      nodes_in_cluster = [node for node, label in zip(G.nodes(), cluster_labels) if label == cluster_label]
      nx.draw_networkx_nodes(G, pos, nodelist=nodes_in_cluster, node_color=pltcolors.to_hex(next_color), node_size=10)

  plt.title('Spectral Clustering')
  plt.show()

# %% [markdown]
# Perform K-means clustering on a subset of eigenvectors to partition the data into clusters
# 
# Parameters:
# - eigenvectors: numpy array
#     Eigenvectors sorted based on eigenvalues
# - n_clusters: int
#     Desired number of clusters
# 
# Returns:
# - cluster_labels: numpy array
#     Cluster labels assigned to each data point

# %%
def kmeans_cluster_partitioning(eigenvectors, n_clusters):
  cluster_vectors = eigenvectors[:, :n_clusters] # get a subset of eigenvectors for clustering

  kmeans = KMeans(n_clusters=n_clusters)
  return kmeans.fit_predict(cluster_vectors)

# %% [markdown]
# Perform spectral clustering on a graph using the scikit-learn library
# 
# Parameters:
# - G: NetworkX graph object
#     Graph to be clustered
# - n_clusters: int
#     Desired number of clusters.
# 
# Returns:
# - cluster_labels: numpy array
#     Cluster labels assigned to each node in the graph

# %%
def scikit_cluster_partitioning(G, n_clusters):
  spectral_clustering = SpectralClustering(n_clusters, eigen_solver='amg', affinity='precomputed', n_jobs=4)
  return spectral_clustering.fit_predict(nx.adjacency_matrix(G, weight=None).toarray())

# %% [markdown]
# Perform cluster partitioning using Spark MLlib's PowerIterationClustering algorithm
# 
# Parameters:
# - G: NetworkX graph object
#     Graph to be partitioned
# - n_clusters: int
#     Desired number of clusters
# 
# Returns:
# - cluster_labels: list
#     Cluster labels assigned to each node in the graph

# %%
def spark_cluster_partitioning(G, n_clusters):
  # spark constants
  APP_NAME = 'assignment1'
  MASTER = 'local[*]'

  # create the SparkSession and SparkContext
  spark = SparkSession.builder.appName("SpectralClustering").getOrCreate()
  conf = SparkConf().setAppName(APP_NAME).setMaster(MASTER)
  sc = SparkContext.getOrCreate(conf=conf)
  
  # convert graph edges to the required format for PowerIterationClustering
  edges = [(int(edge[0]), int(edge[1]), 1.0) for edge in G.edges()]
  rdd = sc.parallelize(edges)
  
  model = PowerIterationClustering.train(rdd, n_clusters, 10, 'degree') # train the PowerIterationClustering model
  cluster_labels = model.assignments().map(lambda x: x.cluster).collect()

  spark.stop()

  return cluster_labels


# %% [markdown]
# ## High Energy Physics - Phenomenology collaboration network

# %% [markdown]
# Compute the sorted eigendecomposition of the Laplacian matrix for the Phenomenology Collaboration Network and determine the desired number of clusters

# %%
G = nx.read_edgelist("ca-HepPh.txt.gz") # read graph

pos = nx.random_layout(G) # generate random node positions

optimal_num_clusters = 20
# compute the sorted eigendecomposition of the graph Laplacian
eigenvalues, eigenvectors = get_sorted_eigendecomposition(G, optimal_num_clusters)

optimal_num_clusters

# %% [markdown]
# Perform K-means clustering on a subset of eigenvectors obtained from the eigendecomposition of a graph's Laplacian matrix

# %%
cluster_labels = kmeans_cluster_partitioning(eigenvectors, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels) # preview the result

# %% [markdown]
# Perform spectral clustering on a graph using the scikit-learn library's SpectralClustering algorithm

# %%
cluster_labels = scikit_cluster_partitioning(G, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)

# %% [markdown]
# Perform cluster partitioning on a graph using Spark MLlib's PowerIterationClustering algorithm

# %%
cluster_labels = spark_cluster_partitioning(G, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)

# %% [markdown]
# ## Social circles: Facebook

# %% [markdown]
# Compute the sorted eigendecomposition of the Laplacian matrix for the Facebook Network and determine the desired number of clusters

# %%
G = nx.read_edgelist("facebook_combined.txt.gz")

pos = nx.random_layout(G)

optimal_num_clusters = 20
eigenvalues, eigenvectors = get_sorted_eigendecomposition(G, optimal_num_clusters)

optimal_num_clusters

# %% [markdown]
# Perform K-means clustering on a subset of eigenvectors obtained from the eigendecomposition of a graph's Laplacian matrix

# %%
cluster_labels = kmeans_cluster_partitioning(eigenvectors, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)

# %% [markdown]
# Perform spectral clustering on a graph using the scikit-learn library's SpectralClustering algorithm

# %%
cluster_labels = scikit_cluster_partitioning(G, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)

# %% [markdown]
# Perform cluster partitioning on a graph using Spark MLlib's PowerIterationClustering algorithm

# %%
cluster_labels = spark_cluster_partitioning(G, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)

# %% [markdown]
# ## Human protein-protein interaction network

# %% [markdown]
# Compute the sorted eigendecomposition of the Laplacian matrix for the Human protein-protein interaction network and determine the desired number of clusters

# %%
data = pd.read_csv("PP-Pathways_ppi.csv.gz", header=None)

G = nx.from_pandas_edgelist(data, source=0, target=1)

pos = nx.random_layout(G)

optimal_num_clusters = 20
eigenvalues, eigenvectors = get_sorted_eigendecomposition(G, optimal_num_clusters)

optimal_num_clusters

# %% [markdown]
# Perform K-means clustering on a subset of eigenvectors obtained from the eigendecomposition of a graph's Laplacian matrix

# %%
cluster_labels = kmeans_cluster_partitioning(eigenvectors, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)

# %% [markdown]
# Perform spectral clustering on a graph using the scikit-learn library's SpectralClustering algorithm

# %%
cluster_labels = scikit_cluster_partitioning(G, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)

# %% [markdown]
# Perform cluster partitioning on a graph using Spark MLlib's PowerIterationClustering algorithm

# %%
cluster_labels = spark_cluster_partitioning(G, optimal_num_clusters)
preview_clustering(G, pos, cluster_labels)


