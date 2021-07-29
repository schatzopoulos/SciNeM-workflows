import sys

class DynamicOptimizer:

	# def __init__(self, n):
	def compute_sparsity(self, a_sparsity, b_sparsity, common_dim):
		return 1 - pow(1 - a_sparsity * b_sparsity, common_dim);

	def compute_sparse_cost(self, a_sparsity, b_sparsity, c_sparsity, a_rows, b_rows, b_cols):

		tempA = a_rows * (a_sparsity * b_rows)
		tempB = ((a_rows * b_rows) * a_sparsity * b_cols) * b_sparsity;
		tempC = (a_rows * c_sparsity) * b_cols ;

		return tempA + tempB + tempC;
		
	def sparse_optimimal_chain_order(self, dims, matrices):

			self._n = len(dims) - 1
			self._m = [[0 for i in range(self._n)] for j in range(self._n)]
			self._s = [[0 for i in range(self._n)] for j in range(self._n)]
			sparsities = [[0 for i in range(self._n)] for j in range(self._n)]


			for length in range (1, self._n):
				for i in range (0, self._n - length):

					j = i + length;
					self._m[i][j] = sys.maxsize

					for k in range(i, j):

						a_sparsity = sparsities[i][k];
						if a_sparsity == 0:
							a_sparsity = matrices[i].get_sparsity()

						b_sparsity = sparsities[k + 1][j];
						if b_sparsity == 0:
							b_sparsity = matrices[j].get_sparsity()

						result_sparsity = self.compute_sparsity(a_sparsity, b_sparsity, dims[k+1]);

						cur_sparse_cost = self.compute_sparse_cost(a_sparsity, b_sparsity, result_sparsity, dims[i], dims[k+1], dims[j+1]);

						cost = self._m[i][k] + self._m[k + 1][j] + cur_sparse_cost;

						if cost < self._m[i][j]:
							self._m[i][j] = cost;
							self._s[i][j] = k;
							sparsities[i][j] = result_sparsity;
	
			return self._m[0][self._n-1]

	def get_optimal_chain_order(self, i, j, chain_order):
		if i == j:
			return i;
		else:
			k = self.get_optimal_chain_order(i, self._s[i][j], chain_order);
			l = self.get_optimal_chain_order(self._s[i][j] + 1, j, chain_order);
			chain_order.append((k, l))

		return -1