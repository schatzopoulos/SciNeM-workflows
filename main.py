from pyspark import *
from pyspark.sql import *
import sys
import json
from SciNeMCore.Graph import Graph
import time
from pyspark.sql.functions import col
import SciNeMCore.utils as utils

if len(sys.argv) != 2:
	print("Usage: spark-submit main.py config.json", file=sys.stderr)
	sys.exit(-1)

spark = SparkSession.builder.appName('SciNeM').getOrCreate()

# supress Spark INFO messages
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

config_file = sys.argv[1]

with open(config_file) as fd:
		config = json.load(fd)
		nodes_dir = config["indir"]
		relations_dir = config["irdir"]
		analyses = config["analyses"]
		alpha = float(config["pr_alpha"]) if ("pr_alpha" in config) else None
		tol = float(config["pr_tol"]) if ("pr_tol" in config) else None
		edgesThreshold = int(config["edgesThreshold"])
		hin_out = config["hin_out"]
		join_hin_out = config["join_hin_out"]
		ranking_out = config["ranking_out"]
		communities_out = config["communities_out"]
		metapath = config["query"]["metapath"]
		joinpath = config["query"]["joinpath"]
		constraints = config["query"]["constraints"]
		community_detection_iter = int(config["maxSteps"])
		community_detection_algorithm = config["community_algorithm"]
		transformation_algorithm = config["transformation_algorithm"] if ("transformation_algorithm" in config) else "MatrixMult"
verbose = True

if ("Ranking" in analyses or "Community Detection" in analyses) or ("Transformation" in analyses and transformation_algorithm == "MatrixMult"):

  # In ranking and community detection a homegeneous graph is needed
  graph = Graph()
  res_hin = graph.transform(spark, metapath, nodes_dir, relations_dir, constraints, verbose)

  # apply filter in case of ranking and community detection
  res_hin.filter(col("numberOfPaths") >= edgesThreshold)
  res_hin.filter(col("src") != col("dst"))

  # abort when resulted network contains no edges
  if res_hin.non_zero() == 0:
    sys.exit(100)

  # write output hin to hdfs
  res_hin.sort()
  res_hin.write(hin_out)

  if "Ranking" in analyses:
    ranks = graph.pagerank(res_hin, alpha, tol)
    #ranks.coalesce(1).map(utils.toCSVLine).saveAsTextFile(ranking_out)
    # convert to DF to overwrite output
    ranks.coalesce(1).toDF().write.csv(ranking_out, sep='\t',  mode='overwrite')

  if "Community Detection" in analyses and community_detection_algorithm == "LPA (GraphFrames)":
    communities = graph.lpa(res_hin, community_detection_iter)
    communities.coalesce(1).write.csv(communities_out, sep='\t', mode='overwrite')

  verbose = False

if "Similarity Join" in analyses or "Similarity Search" in analyses:
  graph = Graph()
  res_hin = graph.transform(spark, joinpath, nodes_dir, relations_dir, constraints, verbose)

  # write output hin to hdfs
  #res_hin.write(join_hin_out)

  graph.similarities(res_hin, config)
