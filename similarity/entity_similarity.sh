#!/bin/bash
cd "$(dirname "$0")"

config="$1"

if ! ../run -c "$config"; then
        echo "Error: Association Mining"
        exit 1
fi

if ! java -jar EntitySimilarity.jar -c "$config"; then
        echo "Error: Similarity Join"
        exit 2
fi

if ! python3 ../add_names.py -c "$config"; then 
        echo "Error: Finding node names"
        exit 3
fi
