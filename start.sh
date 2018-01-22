#!/bin/bash

wget -qO- https://datasets.imdbws.com/title.basics.tsv.gz | gunzip > title.basics.tsv
python -u ./main.py --ingestion_type titles
rm title.basics.tsv

wget -qO- https://datasets.imdbws.com/title.ratings.tsv.gz | gunzip > title.ratings.tsv
python -u ./main.py --ingestion_type ratings
rm title.ratings.tsv

wget -qO- https://datasets.imdbws.com/name.basics.tsv.gz | gunzip > name.basics.tsv
python -u ./main.py --ingestion_type names
rm name.basics.tsv

wget -qO- https://datasets.imdbws.com/title.principals.tsv.gz | gunzip > title.principals.tsv
python -u ./main.py --ingestion_type principals