from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

import argparse
import time


COO_MATRIX_SCHEMA = StructType([
	StructField('src', LongType()),
	StructField('dst', LongType()),
	StructField('numberOfPaths', DoubleType())
])

class SparseMatrix:
	_matrix = None
	_rows = -1
	_cols = -1

	def __init__(self, rows, cols, df):
		self._rows = rows
		self._cols = cols
		self._matrix = df

	def get_sparsity(self):
		return self._matrix.count() / (self._rows * self._cols)

	def get_df(self):
		return self._matrix

	def get_rows(self):
		return self._rows

	def get_cols(self):
		return self._cols

	def non_zero(self):
		return self._matrix.count()

	def write(self, filename):
		self._matrix.coalesce(1).write.option("header", "true").csv(filename, sep='\t', mode='overwrite')

	def sort(self):
		self._matrix = self._matrix.sort(col("src"))

	def filter(self, condition):
		self._matrix = self._matrix.filter(condition)

	def multiply(self, spark, B, enable_broadcast=False):

		self._matrix.createOrReplaceTempView("A")

		if enable_broadcast:
			B = broadcast(B)

		B.get_df().createOrReplaceTempView("B")
		df = spark.sql("""
		SELECT
			A.src src,
			B.dst dst,
			SUM(A.numberOfPaths * B.numberOfPaths) numberOfPaths
		FROM
			A
		JOIN B ON A.dst = B.src
		GROUP BY A.src, B.dst
		""")

		return SparseMatrix(self.get_rows(), B.get_cols(), df)

	def merge(self, B):
		df = self._matrix.union(B.get_df()).groupby(["src", "dst"]).agg(sum("numberOfPaths").alias("numberOfPaths"))
		return SparseMatrix(self.get_rows(), self.get_cols(), df)

