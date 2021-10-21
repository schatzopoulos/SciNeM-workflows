import sys
import csv
import json
import os  
import pandas as pd 
import pydoop.hdfs as hdfs

def parse_entities(entity_file, select_field):
    with open(entity_file) as fp:
    
        # read header file and determine selected column
        line = fp.readline()

        fields = line.rstrip().split('\t')
        for i in range(len(fields)):

            if select_field in fields[i]: 
                break

        select_field_idx = i

        names_df = pd.read_csv(entity_file, sep='\t', usecols=["id", select_field])

    return names_df

def write_output(names, fin, fout):
    
# 		# in similarity search when no results are found, results file is not present
# 		# so create an empty local file to indicate no results found
# 		if os.path.isdir(fin) == False:
# 			fd = open(fout, "w")
# 			fd.close()
# 			return
			
		files = hdfs.ls(fin)
		# find file that has "part-" in the filename; it is the result
		for f in files:
			if "part-" in f:
				break

		with hdfs.open(f) as fd:            

			result = pd.read_csv(fd, sep='\t', header=None, names=["id", "id 2", "Euclidean Distance"])
			result = result.merge(names, on="id", how='inner')

			result.rename(columns={ 'name': 'Entity 1', 'id': 'id 1', 'id 2': 'id' }, inplace=True)

			result = result.merge(names, on="id", how='inner')

			result.rename(columns={ 'name': 'Entity 2', 'id': 'id 2' }, inplace=True)
			del result['id 1']
			del result['id 2']
      
			result = result.sort_values(by=["Euclidean Distance"])
			result[['Entity 1', 'Entity 2', 'Euclidean Distance']].to_csv(fout, index = False, sep='\t')


with open(sys.argv[2]) as config_file:
    print("Similarity Analysis\t3\tSorting and writing results")

    config = json.load(config_file)
    join_in = config["sim_join_out"]
    search_in = config["sim_search_out"]

    join_out = config["final_sim_join_out"]
    search_out = config["final_sim_search_out"]    
    entity_file = config["indir_local"] + config["primary_entity"][:1] + ".csv"

    names = parse_entities(entity_file, config["select_field"])

    if sys.argv[3] == "Similarity Join":
        write_output(names, join_in, join_out)

    if sys.argv[3] == "Similarity Search":
        write_output(names, search_in, search_out)
    
    print(sys.argv[3] + "\t3\tCompleted")


