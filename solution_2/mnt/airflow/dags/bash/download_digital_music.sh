#!/usr/bin/env bash
cd "/usr/local/airflow/dags/files" && pwd

# Digital Music
echo "Downloading music"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Digital_Music.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Digital_Music.json.gz" -O