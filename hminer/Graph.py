import numpy
from pyspark.sql.functions import concat, col, lit, struct, sum, collect_list, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType
from functools import reduce 
import utils
import Pagerank
import time
from SparseMatrix import SparseMatrix
from DynamicOptimizer import DynamicOptimizer
from array import array
import operator
from graphframes import GraphFrame
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT, SparseVector
import math
from pyspark.sql.functions import size

class Graph:

	_dimensions = []
	_transition_matrices = []

	def build(self, spark, metapath, nodes_dir, relations_dir, constraints, printLogs):
		if printLogs == True:
			print("HIN Transformation\t1\tLoading HIN Nodes", flush=True)

		# start_time = time.time()
		constraint_ids = self.build_constraint_matrices(spark, metapath, nodes_dir, constraints)

		# vertices.show(n=5)
		# print("--- read vertices %s %s---" % (time.time() - start_time, vertices.rdd.getNumPartitions()))
		if printLogs == True:
			print("HIN Transformation\t2\tLoading HIN Edges", flush=True)

		# start_time = time.time()
		self._transition_matrices = self.build_transition_matrices(spark, metapath, relations_dir, constraint_ids)
		# edges.show(n=5)
		# print("--- read edges  %s %s ---" % (time.time() - start_time, edges.rdd.getNumPartitions()))

		# self._graph = GraphFrame(vertices, edges)

	def build_constraint_matrices(self, spark, metapath, nodes_dir, constraints):
		
		vertices = None
		# print("##### Nodes #####")
		dims = {}
		constraint_ids = {}

		# loop in unique metapath entities
		for node in list(metapath):

			# we have already processed dimensions & constraints for this entity
			if node in dims:
				self._dimensions.append(dims[node])
				continue

			# read dataframe from csv file
			df = spark.read.csv(nodes_dir + node + '.csv', sep='\t', header=True, inferSchema=True)
			
			# count number of lines
			count = df.count()
			dims[node] = count
			self._dimensions.append(count)

			if node in constraints:
				df_filtered = df.select("id").where(constraints[node])
				constraint_ids[node] = df_filtered #SparseMatrix(df_filtered)
		
		return constraint_ids

	def build_transition_matrices(self, spark, metapath, relations_dir, constraint_ids):
		transition_matrices = []

		# print("##### Relations #####")
		for i in range(len(metapath)-1):
			relation = metapath[i:i+2]
			# print(relation)

			# read from csv file using a specific schema
			schema = StructType([
				StructField("row", IntegerType(), False),
				StructField("col", IntegerType(), False)])

			relations = spark.read.csv(relations_dir + relation + '.csv', sep='\t', header=False, schema=schema)

			if relation[0] in constraint_ids:
				relations = constraint_ids[relation[0]].join(relations, constraint_ids[relation[0]].id == relations.row).select(relations['*'])

			if relation[1] in constraint_ids:
				relations = relations.join(constraint_ids[relation[1]], relations.col == constraint_ids[relation[1]].id).select(relations['*'])

			transition_matrices.append(SparseMatrix(self._dimensions[i], self._dimensions[i+1], relations.withColumn("val", lit(1))))

		return transition_matrices

	def transform(self, spark, printLogs):
		if printLogs == True:
			print("HIN Transformation\t3\tPreparing Network", flush=True)
		
		if len(self._transition_matrices) == 1:
			return self._transition_matrices[0]

		optimizer = DynamicOptimizer()

		optimizer.sparse_optimimal_chain_order(self._dimensions, self._transition_matrices)

		chain_order = []
		optimizer.get_optimal_chain_order(0, len(self._dimensions) - 2, chain_order);
		# print(chain_order)

		temp = []
		tmp_ptr = None

		for (k, l) in chain_order:

			n = len(temp)

			if k >= 0 and l >= 0:

				res = self._transition_matrices[k].multiply(spark, self._transition_matrices[l])
				temp.append(res)

			elif k == -1 and l >= 0:

				temp[n-1] = temp[n-1].multiply(spark, self._transition_matrices[l])

			elif k >= 0 and l == -1:

				temp[n-1] = self._transition_matrices[k].multiply(spark, temp[n-1])

			else:

				temp[n-2] = temp[n-2].multiply(spark, temp[n-1], )
				temp.pop()
				
		return temp[0]

	def pagerank(self, graph, alpha, tol, outfile):

		# aggregate dest nodes based on source and sum number of outgoing edges
		grouped_df = graph.get_df().groupby("row").agg(struct(collect_list("col").alias("cols"), collect_list("val").alias("vals"), sum("val").alias("edges_num")))

		# transform to rdd that is needed for PR
		links = grouped_df.rdd.map(tuple).cache()

		return Pagerank.execute(links, alpha, tol, outfile)

	def lpa(self, graph, iter, outfile):
		print("Community Detection\t1\tInitializing Algorithm", flush=True)
		edges = graph.get_df().select(col('row').alias('src'), col('col').alias('dst'))
		vertices = edges.select('src').union(edges.select('dst')).distinct().withColumnRenamed('src', 'id')

		print("Community Detection\t2\tExecuting Label Propagation Algorithm", flush=True)
		graph = GraphFrame(vertices, edges)
		result = graph.labelPropagation(maxIter=iter)
		result.orderBy('label', ascending=True).withColumnRenamed('label', 'Community').coalesce(1).write.csv(outfile, sep='\t')	
	
	def similarities(self, graph, config):
		
		print("Similarity Analysis\t1\tComputing hashes of feature vectors")
		graph = graph.get_df()
		
		max_id = graph.agg({"col": "max"}).collect()[0][0] + 1
		
		# create features as sparse vectors from col and val columns
		def to_sparse_vector(indices, values):
			indices, values = zip(*sorted(zip(indices, values)))
			return Vectors.sparse(max_id, indices, values)

		def non_zero(v):
			return v.numNonzeros()
		
		to_sparse_vector_udf = udf(lambda indices, values: to_sparse_vector(indices, values), VectorUDT())
		non_zero_udf = udf(lambda v : non_zero(v), LongType())
		
		df = graph.groupby("row").agg(to_sparse_vector_udf(collect_list("col"), collect_list("val")).alias("features"))
		
		# do not consider vector smaller than this threshold
		df = df.filter(non_zero_udf("features") >= int(config["sim_min_values"]))
				
		# caclulate bucket length, given the specified number of buckets
		total_records = df.count()
		buckets_length = math.ceil(math.sqrt(total_records))
# 		buckets_length = math.pow(total_records, -1/max_id)
# 		print(total_records)
# 		print(buckets_length)
		brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=buckets_length, numHashTables=int(config["t"]))
		model = brp.fit(df)
		df_t = model.transform(df)
		
		if ("Similarity Join" in config["analyses"]):
			df_t.cache()
			
			# Compute the locality sensitive hashes for the input rows, then perform approximate similarity join.
			print("Similarity Analysis\t2\tCalculating Similarity Join")
			join_distance = 3
			while True:
				join_results = model.approxSimilarityJoin(df_t, df_t, join_distance, distCol="EuclideanDistance")\
					.select(
						col("datasetA.row").alias("idA"),
						col("datasetB.row").alias("idB"),
						col("EuclideanDistance"))\
					.filter(col("idA") != col("idB"))\
					.orderBy("EuclideanDistance", ascending=True)\
					.limit(int(config["searchK"]))
				
				# loop until we find as many results as requested
				if (join_results.count() >= int(config["searchK"])):
					break
				
				# else increase distance and try again
				join_distance *= 2
				
			join_results.coalesce(1).write.csv(config["sim_join_out"], sep='\t')	
		
		if ("Similarity Search" in config["analyses"]):
			print("Similarity Analysis\t2\tCalculating Top-k Similarity Search results")
			target_id = int(config["target_id"])
			key_vector = df.filter(col("row") == target_id).select(col("features")).collect()
			if (len(key_vector) == 0):
				return
			
			key_vector = key_vector[0]["features"]

			search_results = model.approxNearestNeighbors(df, key_vector, int(config["searchK"]) + 1).filter(col("row") != target_id).select(lit(config["target_id"]), "row", "distCol")
			search_results.coalesce(1).write.csv(config["sim_search_out"], sep='\t')	
