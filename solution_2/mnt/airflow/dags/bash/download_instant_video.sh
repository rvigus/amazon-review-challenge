#!/usr/bin/env bash

# Instant Video
echo "Downloading instant video"
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video.json.gz" -O
curl "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Amazon_Instant_Video.json.gz" -O
