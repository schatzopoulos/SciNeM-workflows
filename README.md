<p align="center">
  <img src="https://github.com/schatzopoulos/SciNeM/blob/master/src/main/webapp/content/images/scinem_logo_purple.png?raw=true" width="400px"/>
  <h1 align="center">Data Science Tool for Heterogeneous Network Mining <br/>(SciNeM-Workflows)</h1>
  <br />
</p>

This repository contains the workflows needed by the [SciNeM](https://github.com/schatzopoulos/SciNeM) application. 
SciNeM is an open-source tool that offers a wide range of functionalities for exploring and analysing HINs and utilises Apache Spark for scaling out through parallel
and distributed computation. Currently, it supports the following operations: 
* Entity Ranking (based on [HRank](https://link.springer.com/article/10.1007/s10115-016-0916-1))
* Similarity Join & Search (based on the BucketedRandomProjectionLSH of [MLlib](https://spark.apache.org/docs/latest/ml-features))
* Community Detection
* Path Searching

## How to cite
```
@inproceedings{chatzopoulos2021scinem,
  title={SciNeM: A Scalable Data Science Tool for Heterogeneous Network Mining.},
  author={Chatzopoulos, Serafeim and Vergoulis, Thanasis and Deligiannis, Panagiotis and Skoutas, Dimitrios and Dalamagas, Theodore and Tryfonopoulos, Christos},
  booktitle={EDBT},
  pages={654--657},
  year={2021}
}
```

## Input files 

Input files can be one of the following types:

* **File containing node attributes.** These are tab-separated files containing all node attributes. The first line is the header that contains all attribute names;
the first column should be an integer identifier, denoted as "id". 
These files should be named with the first letter of the entity they are representing. For example, the file that contains the attributes for node type `Author` should be named `A.csv`. An example of a file containing node attributes is the following: 

```
id	name    surname
0	Makoto  Satoh
1	Ryo Muramatsu
...
```

* **File containing relations.** These are tab-separated files needed to construct the relations among nodes. These files contain two columns, the source and target identidiers respectively and should be sorted based on the first column and named with the extension `.csv`. They do not contain a header and they should be named according to the source and target node types. For example, the file with the relations between node types `Author` and `Paper` should be named `AP.csv`. An example of file containing node relations is the following: 

```
src	dst
0	1
0	2
0	3
0	5
1	0
...
```
Note that a sample compressed file that contains all the required files for a subset of the DBLP dataset can be found [here](http://scinem.imsi.athenarc.gr/upload).

## Usage
It is vital that you provide the appropriate values in the [config file](https://github.com/schatzopoulos/SciNeM-workflows/blob/master/config.properties). 
Then perform the following command:
```
source config.properties
```

In order to perform an analysis, you need to navigate inside the root directory of the cloned repository and execute the following [analysis.sh](https://github.com/schatzopoulos/SciNeM-workflows/blob/master/analysis/analysis.sh) bash script with the appropriate configuration file as a parameter: 

```
/bin/bash ./analysis/analysis.sh <local_absoulte_path>/config.json
```

This configuration file is in JSON format and includes all apporopriate paramreters; 
a sample can be found [here](https://github.com/schatzopoulos/SciNeM-workflows/blob/master/analysis/sample_config.json).
Its main parameters are described below:

| Parameter Name  | Description |
| ------------- | ------------- |
| indir |	hdfs path of the folder containing node attribute files |
| irdir |	hdfs path of the folder containing relation files |
| indir_local |	local path of the folder containing node attribute files |
| hdfs_out_dir | hdfs path of base folder to save the results after analysis |
| local_out_dir |	local path of the base folde to save the final results  |
| analyses | an array with the analyses to be performed; currently supported analyses types are `Ranking`, `Community Detection`, `Transformation`, `Path Searching`, `Similarity Join` and `Similarity Search` |
| queries | an array of json objects containing keys for `metapath` to be used, `joinpath` (used in similarity join anaylysis) and `constraints` to be applied on node types |
| hin_out |	hdfs path to save the homogeneous network after HIN transformation |
| join_hin_out | hdfs path to save the homogeneous network after HIN transformation for similarity analyses	|
| ranking_out |	hdfs path to save ranking output |
| communities_out |	hdfs path to save community detection output |
| communities_details |	local path that indicates the file to store encoded output of community detection (needed for visualisation) |
| sim_search_out |	hdfs path to save similarity search output|
| sim_join_out |	hdfs path to save similarity join output |
| path_searching_out | hdfs path to save the output of path searching |
| final_ranking_out |	local path to save the final ranking output (containing the `select_field` column apart from the entity id)  |
| final_communities_out |	local path to save the final community detection output (containing the `select_field` column apart from the entity id)  |
| final_sim_search_out | 	local path to save the final similarity search output (containing the `select_field` column apart from the entity id)  |	
| final_sim_join_out |	local path to save the final similarity join output (containing the `select_field` column apart from the entity id)  |
| final_ranking_community_out |	local path to save the final ranking output combined with community detection results |
| final_community_ranking_out |	local path to save the final community detection output combined with ranking results |
| dataset |	the dataset name to be used |
| primary_entity | the name of the primary entity in the metapath (i.e. first and last entity)	|
| select_field | the field to append in the final results of each analysis (e.g. the `name` of column of entity `Author`)|
| pr_alpha | the alpha parameter of the Pagerank algorithm |
| pr_tol |	the tolerance parameter of the Pagerank algorithm |
| edgesThreshold |	threshold on the number of occurences of each edge on the transformed homogeneous network (edges with less frequency are not considered in the analyses)|
| target_id |	the target entity id of the similarity search analysis |
| searchK |	the `k` most similar entities to be retrieved in similarity analyses |
| t |	number of hash tables to be used for LSH in similarity analyses |
| sim_min_values |	threshold on the number of occurences of each edge to be considerd for similarity analyses (edges with less frequency are not considered in similarity analyses)|
| inputCSVDelimiter |	the delimiter of input files |
| community_algorithm |	the community detection algorithm to be used; one of `LPA (GraphFrames)`, `LPA`, `OLPA`, `PIC`, `HPIC`|
| transformation_algorithm | the algorithm to be used for HIN transformation; one of `MatrixMutl` or `Pregel` |
| maxSteps | the maximum number of iterations to be performed by community detection algorithms |
| threshold |	a double number used in each iteration of Pregel in OLPA to determine for each vertex which of the incoming communities from its neighbors will be included in the community affiliations of a vertex |
| stopCriterion |	the number of times that a vertex can have the same community affiliation(s) before it stops been included in the remaining supersteps of the LPA(OLPA). |
| nOfCommunities |	the number of communities given to perform the PIC algorithm or the initial number of communities given to perform the HPIC algorithm. |
| ratio |	 a double number which reduces the number of communities on each level of HPIC algorithm |
| path_searching_pairs | a file in the hdfs that encodes needed pairs for path searching |
| path_searching_length | the length of the paths to be retrieved |

