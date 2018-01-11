#!/bin/bash

wget -qO- https://datasets.imdbws.com/title.basics.tsv.gz | gunzip > data.tsv
python -u ./main.py