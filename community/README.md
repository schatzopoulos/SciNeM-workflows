# Community Detection Algorithms

## LPA (GraphFrames)

Label Propagation Algorithm (LPA), the standard community detection algorithm, as implemented by the [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#label-propagation-algorithm-lpa) Apache Spark package. 
Each node in the network is initially assigned to its own community, while at every superstep, nodes send their community affiliation to all neighbors and update their state to the mode community affiliation of incoming messages.
It is computationally inexpesive, although convergence is not guaranteed and trivial solutions may arise.

*Parameters: iterations*

## LPA
An enhanced implementation of LPA [1] considering weighted edges and a stopping criterion on the vertices for the propagation of labels. 

*Parameters: iterations, stop criterion*

## OLPA
A variant of LPA that detects communities that overlap. 
Different from the abovementioned enhanced LPA, each vertex has multiple labels. 
The labels received by its neighbors must pass a certain user-defined threshold to be included to the community affiliations of the vertex. 
Similar to the aforementioned enhanced LPA there is a stopping criterion imposed on the vertices based on how many times the vertex received the same community affiliations. 
In every iteration, a vertex sends its community affiliations weighted by the edge weight to a neighbor vertex through an edge.

*Parameters: iterations, stop criterion, threshold*

## PIC
The Power Iteration Clustering (PIC) [2], a scalable graph clustering algorithm proposed by Lin and Cohen. 
It finds a very low-dimensional embedding of a dataset using truncated power iteration on a normalized pairwise similarity matrix of the data. 
This embedding turns out to be an effective cluster indicator. 
GraphX has an implementation of PIC where it computes a pseudo-eigenvector of the normalized affinity matrix of the graph via power iteration and uses it to cluster vertices. 
The number of desired clusters is a user-defined parameter.

*Parameters: iterations, number of communities*

## HPIC 
Hierarchical Power Iteration Clustering (HPIC) is used for hierarchical community detection following the “bottom-up” approach. 
Our method consists of consecutive executions of the PIC algorithm. 
The number of clusters produced in each iteration is multiplied by a user-defined parameter ratio and the resulting Double number is rounded up and used as the number of clusters in the next iteration of PIC. After each execution of PIC, we create a new DataFrame, which will be used as input in the next iteration, and contains edges that correspond to the resulting clusters as source and destination vertices and the sum of the outer-cluster edges’ edge-weights as new edge-weights. The results of each execution are stored in a DataFrame as a new column. Each column corresponds to a different level in the hierarchy.

*Parameters: iterations, number of communities, ratio*

### Parameters description

| Parameter Name  | Description |
| ------------- | ------------- |
| maxSteps (iterations) | the maximum number of iterations to be performed by community detection algorithms |
| threshold |	a double number used in each iteration of Pregel in OLPA to determine for each vertex which of the incoming communities from its neighbors will be included in the community affiliations of a vertex |
| stopCriterion |	the number of times that a vertex can have the same community affiliation(s) before it stops been included in the remaining supersteps of the LPA (OLPA). |
| nOfCommunities |	the number of communities given to perform the PIC algorithm or the initial number of communities given to perform the HPIC algorithm. |
| ratio |	 a double number which reduces the number of communities on each level of HPIC algorithm |

### References
[1] Raghavan, Usha Nandini, Réka Albert, and Soundar Kumara. "Near linear time algorithm to detect community structures in large-scale networks." Physical review E 76.3 (2007): 036106.

[2] Lin, Frank, and William W. Cohen. "Power iteration clustering." ICML. 2010.
