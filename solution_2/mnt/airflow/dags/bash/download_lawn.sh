#!/usr/bin/env bash
cd "/usr/local/airflow/dags/files" && pwd

# Lawn and Garden
echo "Downloading lawn and garden"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Patio_Lawn_and_Garden.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Patio_Lawn_and_Garden.json.gz" -O