import sys
import csv
import json
import os  
import pandas as pd 
import pydoop.hdfs as hdfs

class ResultsParser:

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

    def write_output(names, analysis, fin, fout, community_details_out):

        # read community detection result
        if analysis == "Ranking":
            with hdfs.open(fin + "/part-00000") as fd:
                result = pd.read_csv(fd, sep='\t', header=None, names=["id", "Ranking Score"])

        elif analysis == "Community Detection":
            files = hdfs.ls(fin)
            # find file that has "part-" in the filename; it is the result
            for f in files:
              if "part-" in f:
                break

            with hdfs.open(f) as fd:            
              df = pd.read_csv(fd, sep='\t', header=None, names=["id", "Community"])
              result = df.sort_values(by=["Community"])

              # count total communities and entities inside each community
              community_counts =  df.groupby('Community')['id'].nunique()
              community_counts.loc["total"] = community_counts.count()
              community_counts.to_json(community_details_out)


        result = result.merge(names, on="id", how='inner')
        del result['id']
        # result.rename(columns={'name': 'Entity'}, inplace=True)

        cols = result.columns.tolist()

        # in case of ranking, move name first
        if analysis == "Ranking":
            cols = cols[-1:] + cols[:-1]

        result[cols].to_csv(fout, index = False, sep='\t')


    with open(sys.argv[2]) as config_file:
        analysis = sys.argv[3]
        fin = sys.argv[4]
        fout = sys.argv[5]
        config = json.load(config_file)
        community_details = config["communities_details"]

        entity_file = config["indir_local"] + config["query"]["metapath"][:1] + ".csv"

        names = parse_entities(entity_file, config["select_field"])
        write_output(names, analysis, fin, fout, community_details)
        print(analysis + "\t3\tCompleted")
