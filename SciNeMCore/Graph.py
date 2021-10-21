import numpy
from pyspark.sql.functions import concat, col, lit, struct, sum, collect_list, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType
from functools import reduce
import SciNeMCore.utils as utils
import SciNeMCore.Pagerank as Pagerank
import time
from SciNeMCore.SparseMatrix import SparseMatrix
from SciNeMCore.DynamicOptimizer import DynamicOptimizer
from array import array
import operator
from graphframes import GraphFrame
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT, SparseVector
import math
from pyspark.sql.functions import size

class Graph:

	#_dimensions = []
	#_transition_matrices = []

	def transform(self, spark, queries, nodes_dir, relations_dir, verbose):

		step = 3 / (len(queries) * 3)

		for idx, query in enumerate(queries, start=0):

			metapath = query["metapath"]
			constraints = query["constraints"]

			if verbose == True:
                        	print("HIN Transformation\t" + str(step + idx * (step * 3)) + "\tLoading HIN Nodes for metapath " + metapath, flush=True)

			# start_time = time.time()
			dimensions, constraint_ids = self.build_constraint_matrices(spark, metapath, nodes_dir, constraints)

			# vertices.show(n=5)
			# print("--- read vertices %s %s---" % (time.time() - start_time, vertices.rdd.getNumPartitions()))
			if verbose == True:
				print("HIN Transformation\t" + str(2 * step + idx * (step * 3)) + "\tLoading HIN Edges for metapath " + metapath, flush=True)

			# start_time = time.time()
			transition_matrices = self.build_transition_matrices(spark, metapath, relations_dir, constraint_ids, dimensions)
			# edges.show(n=5)
			# print("--- read edges  %s %s ---" % (time.time() - start_time, edges.rdd.getNumPartitions()))
			if verbose == True:
                        	print("HIN Transformation\t" + str(3 * step + idx * (step * 3)) + "\tPreparing Network for metapath " + metapath, flush=True)

			partial_result = self.multiply(spark, transition_matrices, dimensions, verbose)

			if idx == 0:
				hin_view = partial_result
			else:
				hin_view = hin_view.merge(partial_result)

		return hin_view

	def build_constraint_matrices(self, spark, metapath, nodes_dir, constraints):

		vertices = None
		# print("##### Nodes #####")
		dims = {}
		constraint_ids = {}
		dimensions = []

		# loop in unique metapath entities
		for node in list(metapath):

			# we have already processed dimensions & constraints for this entity
			if node in dims:
				dimensions.append(dims[node])
				continue

			# read dataframe from csv file
			df = spark.read.csv(nodes_dir + node + '.csv', sep='\t', header=True, inferSchema=True)

			# count number of lines
			count = df.count()
			dims[node] = count
			dimensions.append(count)

			if node in constraints:
				df_filtered = df.select("id").where(constraints[node])
				constraint_ids[node] = df_filtered #SparseMatrix(df_filtered)

		return dimensions, constraint_ids

	def build_transition_matrices(self, spark, metapath, relations_dir, constraint_ids, dimensions):
		transition_matrices = []

		# print("##### Relations #####")
		for i in range(len(metapath)-1):
			relation = metapath[i:i+2]
			# print(relation)

			# read from csv file using a specific schema
			schema = StructType([
				StructField("src", IntegerType(), False),
				StructField("dst", IntegerType(), False)])

			relations = spark.read.csv(relations_dir + relation + '.csv', sep='\t', header=True, schema=schema)

			if relation[0] in constraint_ids:
				relations = constraint_ids[relation[0]].join(relations, constraint_ids[relation[0]].id == relations.src).select(relations['*'])

			if relation[1] in constraint_ids:
				relations = relations.join(constraint_ids[relation[1]], relations.dst == constraint_ids[relation[1]].id).select(relations['*'])

			transition_matrices.append(SparseMatrix(dimensions[i], dimensions[i+1], relations.withColumn("numberOfPaths", lit(1))))

		return transition_matrices

	def multiply(self, spark, transition_matrices, dimensions, verbose):

		if len(transition_matrices) == 1:
			return transition_matrices[0]

		optimizer = DynamicOptimizer()

		optimizer.sparse_optimimal_chain_order(dimensions, transition_matrices)

		chain_order = []
		optimizer.get_optimal_chain_order(0, len(dimensions) - 2, chain_order);
		# print(chain_order)

		temp = []
		tmp_ptr = None

		for (k, l) in chain_order:

			n = len(temp)

			if k >= 0 and l >= 0:

				res = transition_matrices[k].multiply(spark, transition_matrices[l])
				temp.append(res)

			elif k == -1 and l >= 0:

				temp[n-1] = temp[n-1].multiply(spark, transition_matrices[l])

			elif k >= 0 and l == -1:

				temp[n-1] = transition_matrices[k].multiply(spark, temp[n-1])

			else:

				temp[n-2] = temp[n-2].multiply(spark, temp[n-1], )
				temp.pop()

		return temp[0]

	def pagerank(self, graph, alpha, tol):

		# aggregate dest nodes based on source and sum number of outgoing edges
		grouped_df = graph.get_df().groupby("src").agg(struct(collect_list("dst").alias("cols"), collect_list("numberOfPaths").alias("vals"), sum("numberOfPaths").alias("edges_num")))

		# transform to rdd that is needed for PR
		links = grouped_df.rdd.map(tuple).cache()

		return Pagerank.execute(links, alpha, tol)

	def lpa(self, graph, iter):
		print("Community Detection\t1\tInitializing Algorithm", flush=True)
		edges = graph.get_df()
		vertices = edges.select('src').union(edges.select('dst')).distinct().withColumnRenamed('src', 'id')

		print("Community Detection\t2\tExecuting Label Propagation Algorithm", flush=True)
		graph = GraphFrame(vertices, edges)
		result = graph.labelPropagation(maxIter=iter)
		return result.orderBy('label', ascending=True).withColumnRenamed('label', 'Community')
		# .coalesce(1).write.csv(outfile, sep='\t')	

	def similarities(self, graph, config):

		print("Similarity Analysis\t1\tComputing hashes of feature vectors")
		graph = graph.get_df()

		max_id = graph.agg({"dst": "max"}).collect()[0][0] + 1

		# create features as sparse vectors from col and val columns
		def to_sparse_vector(indices, values):
			indices, values = zip(*sorted(zip(indices, values)))
			return Vectors.sparse(max_id, indices, values)

		def non_zero(v):
			return v.numNonzeros()

		to_sparse_vector_udf = udf(lambda indices, values: to_sparse_vector(indices, values), VectorUDT())
		non_zero_udf = udf(lambda v : non_zero(v), LongType())

		df = graph.groupby("src").agg(to_sparse_vector_udf(collect_list("dst"), collect_list("numberOfPaths")).alias("features"))

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
						col("datasetA.src").alias("idA"),
						col("datasetB.src").alias("idB"),
						col("EuclideanDistance"))\
					.filter(col("idA") != col("idB"))\
					.orderBy("EuclideanDistance", ascending=True)\
					.limit(int(config["searchK"]))

				# loop until we find as many results as requested
				if (join_results.count() >= int(config["searchK"])):
					break

				# else increase distance and try again
				join_distance *= 2

			join_results.coalesce(1).write.csv(config["sim_join_out"], sep='\t', mode='overwrite')

		if ("Similarity Search" in config["analyses"]):
			print("Similarity Analysis\t2\tCalculating Top-k Similarity Search results")
			target_id = int(config["target_id"])
			key_vector = df.filter(col("src") == target_id).select(col("features")).collect()
			if (len(key_vector) == 0):
				return

			key_vector = key_vector[0]["features"]

			search_results = model.approxNearestNeighbors(df, key_vector, int(config["searchK"]) + 1).filter(col("src") != target_id).select(lit(config["target_id"]), "src", "distCol")
			search_results.coalesce(1).write.csv(config["sim_search_out"], sep='\t', mode='overwrite')
