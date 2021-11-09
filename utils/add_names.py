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

def write_output(names, analysis, fin, fout, community_details_out, community_algorithm, hdfs_hin_path, results_directory):
    files = hdfs.ls(fin)
    # find file that has "part-" in the filename; it is the result
    for f in files:
        if "part-" in f:
            break

    # read community detection result
    if analysis == "Ranking":
        with hdfs.open(f) as fd:
            result = pd.read_csv(fd, sep='\t', header=None, names=["id", "Ranking Score"])
            # set the target node ids as the ids of the first 10 results of ranking
            target_hin_nodes = {row[1]['id']:row[1]['Ranking Score'] for row in result.head(10).iterrows()}
            max_ranking_score = result["Ranking Score"].max()
            result["Ranking Score"] /= max_ranking_score

            # read hin file and store hin edge entries that connect nodes with the target ids\
            files = hdfs.ls(hdfs_hin_path)
            # find file that has "part-" in the filename; it is the result
            for f in files:
                if "part-" in f:
                    break
            with hdfs.open(f) as hin_edges:
                edge_entries = pd.read_csv(hin_edges, sep='\t', header=None, names=['id0', 'id1', 'weight'])
                target_edges = edge_entries[edge_entries['id0'].isin(target_hin_nodes)][
                    edge_entries['id1'].isin(target_hin_nodes)]
                edges_json_list = [
                    {'source': int(row[1]['id0']), 'target': int(row[1]['id1']), 'weight': float(row[1]['weight'])} for
                    row in target_edges.iterrows()]

                target_node_names = names[names['id'].isin(target_hin_nodes)].set_index('id')
                nodes_json_list = [{'id': int(id), 'label': target_node_names.loc[id][target_node_names.columns[0]],
                                    'value': float(target_hin_nodes[id])} for id in target_hin_nodes]

                hin_json = {
                    'nodes': nodes_json_list,
                    'edges': edges_json_list
                }

                with open(os.path.join(results_directory, 'RANKING_HIN_SCHEMA.json'), 'w',
                          encoding='utf-8') as hin_json_out:
                    json.dump(hin_json, hin_json_out)

    elif analysis == "Community Detection":
        with hdfs.open(f) as fd:

          if community_algorithm == "HPIC":
              result = pd.read_csv(fd, sep='\t', header=None)
              result = result.iloc[:, 1:] # delete first column as it is the same with the second (why?)

              # rename columns based on the hierarchy levels found
              colsLen = len(result.columns)
              colnames = [ i for i in range(colsLen, 0, -1) ]
              colnames[0] = 'id'
              result.rename(columns=dict(zip(result.columns.values, colnames)), inplace=True)

          else:
              colnames = [ "id", "Community" ]

              # OLPA also produces scores for each entity in a community
              if (community_algorithm == "OLPA"):
                  colnames.append("Community Membership Score")

              result = pd.read_csv(fd, sep='\t', header=None, names=colnames)
              result = result.sort_values(by=["Community"])

              # count total communities and entities inside each community
              community_counts =  result.groupby('Community')['id'].nunique()
              community_counts.loc["total"] = community_counts.count()
              community_counts.to_json(community_details_out)

    result = result.merge(names, on="id", how='inner')
    del result['id']

    cols = result.columns.tolist()

    # in case of ranking, move name first
    if analysis == "Ranking":
        cols = cols[-1:] + cols[:-1]

    # in case of community detection, sort by community and membership score
    elif analysis == "Community Detection" and not community_algorithm == "HPIC":
        result = result.sort_values(by=["Community"])

    result[cols].to_csv(fout, index = False, sep='\t')
    print(result)

with open(sys.argv[2]) as config_file:
    analysis = sys.argv[3]
    fin = sys.argv[4]
    fout = sys.argv[5]
    config = json.load(config_file)
    community_details = config["communities_details"]
    hin_out = config['hin_out']
    results_directory = config['local_out_dir']
    community_algorithm = config['community_algorithm']

    entity_file = config["indir_local"] + config["primary_entity"][:1] + ".csv"

    names = parse_entities(entity_file, config["select_field"])
    write_output(names, analysis, fin, fout, community_details, community_algorithm, hin_out, results_directory)
    print(analysis + "\t3\tCompleted")
