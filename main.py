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
		community_detection_iter = int(config["community_detection_iter"])
printLogs = True

if "Ranking" in analyses or "Community Detection" in analyses:
  # In ranking and community detection a homegeneous graph is needed
  graph = Graph()
  print(spark, metapath, nodes_dir, relations_dir, constraints, printLogs)
  graph.build(spark, metapath, nodes_dir, relations_dir, constraints, printLogs)
  res_hin = graph.transform(spark, printLogs)

  # apply filter in case of ranking and community detection
  res_hin.filter(col("numberOfPaths") >= edgesThreshold)

  # abort when resulted network contains no edges
  if res_hin.non_zero() == 0:
    sys.exit(100)

  if "Ranking" in analyses:
    print(res_hin, alpha, tol, ranking_out)
    ranks = graph.pagerank(res_hin, alpha, tol)
    ranks.coalesce(1).map(utils.toCSVLine).saveAsTextFile(ranking_out)


  if "Community Detection" in analyses:
    print(res_hin, community_detection_iter, communities_out)
    communities = graph.lpa(res_hin, community_detection_iter)
    communities.coalesce(1).write.csv(communities_out, sep='\t')

  # write output hin to hdfs
  res_hin.filter(col("src") != col("dst"))
  res_hin.sort()
  print(hin_out)
  res_hin.write(hin_out)

  printLogs = False

if "Similarity Join" in analyses or "Similarity Search" in analyses:
  graph = Graph()
  graph.build(spark, joinpath, nodes_dir, relations_dir, constraints, printLogs)

  # write output hin to hdfs
  # 	res_hin.write(join_hin_out)
	
  res_hin = graph.transform(spark, printLogs)
  
  graph.similarities(res_hin, config)
  
