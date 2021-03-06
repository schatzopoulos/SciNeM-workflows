#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function
import re
import sys
import time
from math import ceil
from operator import add
from pyspark.sql import SparkSession
import utils

def compute_contribs(outgoing_edges, rank):
    """Calculates contributions based on the number of edges between the two nodes."""

    # total_weight = sum(edge['val'] for edge in outgoing_edges["edges"])

    # for edge in outgoing_edges["edges"]:
    #     yield (edge['col'], rank * edge['val'] / outgoing_edges["edges_num"])
    for i in range(len(outgoing_edges["cols"])):
        yield (outgoing_edges["cols"][i], rank * outgoing_edges["vals"][i] / outgoing_edges["edges_num"])

def pagerank_score(rank, alpha, initial_pagerank):
    return alpha * rank  + (1 - alpha) * initial_pagerank

def execute(links, alpha, convergence_error, outfile):
    print("Ranking\t1\tInitializing Ranking Algorithm", flush=True)

    # sum all weights
    # total_weights = links.map(lambda x: (1, sum(record['val'] for record in x[1]))) \
    # .reduceByKey(lambda x,y: x + y).collect()[0][1]
    # total number of nodes
    node_count = links.count()
    # print("--- links count %s %s---" % (time.time() - start_time, links.getNumPartitions()))

    # print("Number of nodes: %s" % (node_count))
    # print("Convergence Error: %s" % (convergence_error))
    # start_time = time.time()
    # initialize pagerank score
    initial_pagerank = 1 / float(node_count)
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], initial_pagerank), preservesPartitioning = True)

    # print(ranks.take(5))
    # print("--- ranks init %s %s---" % (time.time() - start_time, ranks.getNumPartitions()))

    # find dangling nodes
    # dangling_nodes = links.filter(lambda link: not link[1]).cache()

    # initialize error in a high value
    max_error = 100
    
    iteration = 0
    print("Ranking\t2\tExecuting Ranking Algorithm", flush=True)

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    while(max_error >= convergence_error):        

        start_time = time.time()

        prev_ranks = ranks

        # calculate dangling sum
        # dangling_sum = dangling_nodes.join(ranks).map(lambda x: x[1][1]).sum()
        # dangling_sum /= node_count

        # add dangling sum to all nodes
        # dangling_contribs = links.mapValues(lambda x: dangling_sum)
       
        contribs = links.join(ranks, numPartitions = links.getNumPartitions()).flatMap(
            lambda outgoing_edges: compute_contribs(outgoing_edges[1][0], outgoing_edges[1][1]))
        
        # contribs = contribs.union(dangling_contribs).coalesce(links.getNumPartitions())

        # re-calculate pagerank score from neighbor contributions
        ranks = contribs.reduceByKey(add, numPartitions = links.getNumPartitions()).mapValues(lambda rank: pagerank_score(rank, alpha, initial_pagerank))

        # calculate error between consecutive iterations
        max_error = ranks.join(prev_ranks).mapValues(lambda rank: abs(rank[0] - rank[1])).values().max()
        print("Ranking\t2\tExecuting Ranking Algorithm (iteration %s)" % (iteration+1), flush=True)
        iteration += 1
    
    print("Ranking\t3\tSorting Ranking List", flush=True)
    ranks.sortBy(lambda x: - x[1]).coalesce(1).map(utils.toCSVLine).saveAsTextFile(outfile)

    return ranks
