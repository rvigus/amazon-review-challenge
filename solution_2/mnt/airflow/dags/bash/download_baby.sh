#!/usr/bin/env bash
cd "/usr/local/airflow/dags/files" && pwd

# Baby
echo "Downloading baby"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Baby.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Baby.json.gz" -O

