#!/usr/bin/env bash
cd "/usr/local/airflow/dags/files" && pwd

# Lawn and Garden
echo "Downloading lawn and garden"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Patio_Lawn_and_Garden.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Patio_Lawn_and_Garden.json.gz" -O

# Baby
echo "Downloading baby"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Baby.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Baby.json.gz" -O

# Digital Music
echo "Downloading music"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Digital_Music.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Digital_Music.json.gz" -O

# Musical Instruments
echo "Downloading intstruments"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Musical_Instruments.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Musical_Instruments.json.gz" -O
