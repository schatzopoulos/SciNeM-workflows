#!/bin/bash
cd "$(dirname "$0")"
config="$1"
CONFIG_FILE="../config.properties"

function clean_exit() {
# 	hadoop dfs -rm -r `cat "$config" | jq -r .hdfs_out_dir` > /dev/null
	exit $1
}

# performs HIN transformation and ranking (if needed)
# spark-submit --master local[*] --conf spark.sql.shuffle.partitions=32 --driver-memory=20G --packages graphframes:graphframes:0.8.0-spark3.0-s_2.12 --py-files=../SciNeMCore/sources.zip ../main.py "$config"
spark-submit \
 --master spark://"${spark_master}" \
 --conf spark.sql.shuffle.partitions="${shuffle_partitions}" \
 --executor-cores "${executor_cores}" \
 --total-executor-cores "${total_executor_cores}" \
 --executor-memory "${executor_memory}" \
 --num-executors "${num_executors}" \
 --packages graphframes:graphframes:0.8.0-spark3.0-s_2.12 \
 --py-files=../SciNeMCore/sources.zip ../main.py "$config"

ret_val=$?

if [ $ret_val -ne 0 ]; then
   	echo "Error: HIN Transformation"
   	clean_exit $ret_val
fi

analyses=`cat "$config" | jq -r .analyses`


if [[ " ${analyses[@]} " =~ "Transformation" ]]; then

	transformation_algorithm=`cat "$config" | jq -r .transformation_algorithm`

	# performs transformation using the Pregel API
	if [[ "${transformation_algorithm}" == "Pregel" ]]; then
          	spark-submit \
           	--master spark://"${spark_master}" \
            	--conf spark.sql.shuffle.partitions="${shuffle_partitions}" \
		--conf spark.driver.maxResultSize=0 \
            	--executor-cores "${executor_cores}" \
	    	--driver-memory="${driver_memory}" \
	    	--executor-memory="${executor_memory}" \
	    	--num-executors "${num_executors}" \
            	../HINGraphX/target/scala-2.12/HINGraphX-assembly-3.0.1-1.3.4.jar "$config"

		ret_val=$?
		if [ $ret_val -ne 0 ]; then
		        echo "Error: HIN Transformation with Pregel"
		        clean_exit $ret_val
		fi
	fi
fi

# format ranking ouput
if [[ " ${analyses[@]} " =~ "Ranking" ]]; then
	ranking_out=`cat "$config" | jq -r .ranking_out`
	ranking_final=`cat "$config" | jq -r .final_ranking_out`

	if ! python3 ../utils/add_names.py -c "$config" "Ranking" "$ranking_out" "$ranking_final"; then
         echo "Error: Finding node names in Ranking output"
         clean_exit 2
	fi
fi

if [[ " ${analyses[@]} " =~ "Similarity Join" ]]; then

	if ! python3 ../utils/add_names_sim.py -c "$config" "Similarity Join"; then
         echo "Error: Finding node names in Similarity Join output"
         clean_exit 2
	fi
fi

if [[ " ${analyses[@]} " =~ "Similarity Search" ]]; then

	if ! python3 ../utils/add_names_sim.py -c "$config" "Similarity Search"; then
         echo "Error: Finding node names in Similarity Search output"
         clean_exit 2
	fi
fi

community_algorithm=`cat "$config" | jq -r .community_algorithm`

# perform Community Detection
if [[ " ${analyses[@]} " =~ "Community Detection" ]]; then

	communities_out=`cat "$config" | jq -r .communities_out`
	final_communities_out=`cat "$config" | jq -r .final_communities_out`

	# execute community detection algorithms in scala
	if [[ "$community_algorithm" == "LPA" ]] || [[ "$community_algorithm" == "OLPA" ]] || [[ "$community_algorithm" == "PIC" ]] || [[ "$community_algorithm" == "HPIC" ]]; then

		spark-submit \
			--master spark://"${spark_master}" \
			--conf spark.sql.shuffle.partitions="${shuffle_partitions}" \
			--conf spark.network.timeout=600s \
			--executor-cores "${executor_cores}" \
			--driver-memory="${driver_memory}" \
			--executor-memory="${executor_memory}" \
			--num-executors "${num_executors}" \
			../community/target/scala-2.12/AlgorithmsGraphX-assembly-3.0.1-1.3.4.jar "$config"

		ret_val=$?
		if [ $ret_val -ne 0 ]; then
		        echo "Error: Executing Community Detection"
		        clean_exit $ret_val
		fi
	fi

	# add entity attributes
        if ! python3 ../utils/add_names.py -c "$config" "Community Detection" "$communities_out" "$final_communities_out"; then
            echo "Error: Finding node names in Community Detection output"
            clean_exit 2
        fi
fi

if [[ " ${analyses[@]} " =~ "Path Searching" ]]; then
 	spark-submit \
        	--master spark://"${spark_master}" \
                --conf spark.sql.shuffle.partitions="${shuffle_partitions}" \
                --executor-cores "${executor_cores}" \
                --driver-memory="${driver_memory}" \
                --executor-memory="${executor_memory}" \
                --num-executors "${num_executors}" \
                ../FindAllPathsForPairsGraphX/target/scala-2.12/FindAllPathsForPairsGraphX-assembly-3.0.1-1.3.4.jar "$config"

                ret_val=$?
                if [ $ret_val -ne 0 ]; then
                        echo "Error: Executing Path Search"
                        clean_exit $ret_val
                fi

fi

# both ranking & community detection have been executed, merge their results
if [[ " ${analyses[@]} " =~ "Ranking - Community Detection" ]]; then

	if [[ "$community_algorithm" == "LPA (GraphFrames)" ]] || [[ "$community_algorithm" == "LPA" ]] || [[ "$community_algorithm" == "OLPA" ]] || [[ "$community_algorithm" == "PIC" ]]; then
		if ! python3 ../utils/merge_results.py -c "$config"; then
		        echo "Error: Combining Ranking with Community Detection"
		        clean_exit 2
		fi
	else
		echo "TODO: merge results for HPIC"
		clean_exit 2
	fi
fi

clean_exit 0
