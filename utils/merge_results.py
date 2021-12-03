import sys
import csv
import json
import os  
import pandas as pd 

with open(sys.argv[2]) as config_file:
	    config = json.load(config_file)

print("Community Detection\t3\tCombining Results with Ranking Scores")

# read both ranking & community detection results
ranking_df = pd.read_csv(config["final_ranking_out"], sep='\t')
community_df = pd.read_csv(config["final_communities_out"], sep='\t')
select_field = config["select_field"]

# join on Entity column
result = ranking_df.merge(community_df, on=select_field, how='inner')

# swap community id and ranking score columns
cols = result.columns.tolist()
cols[1], cols[2] = cols[2], cols[1]

result[cols].to_csv(config["final_community_ranking_out"], index = False, sep='\t')
print("Community Detection - Ranking\t3\tCompleted")

result.sort_values(by=["Ranking Score"], ascending=False, inplace=True)
result.to_csv(config["final_ranking_community_out"], index = False, sep='\t')
print("Ranking - Community Detection\t3\tCompleted")
