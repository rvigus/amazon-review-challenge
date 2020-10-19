#!/usr/bin/env bash
cd "/usr/local/airflow/dags/files" && pwd

# Musical Instruments
echo "Downloading intstruments"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Musical_Instruments.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Musical_Instruments.json.gz" -O