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
0	1
0	2
0	3
0	5
1	0
...
```

## Usage
All analyses can be performed using the [analysis.sh](https://github.com/schatzopoulos/SciNeM-workflows/blob/master/analysis/analysis.sh) bash script that requires a configuration file in json format. The parameters of this configuration files are described below: [TODO]

A sample configuration file can be found [here](https://github.com/schatzopoulos/SciNeM-workflows/blob/master/analysis/sample_config.json).
