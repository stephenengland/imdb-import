#!/bin/bash

echo "Running latest version"
mkdir s3files
cd s3files
wget --timeout=10 https://datasets.imdbws.com/name.basics.tsv.gz
wget --timeout=10 https://datasets.imdbws.com/title.akas.tsv.gz
wget --timeout=10 https://datasets.imdbws.com/title.basics.tsv.gz
wget --timeout=10 https://datasets.imdbws.com/title.crew.tsv.gz
wget --timeout=10 https://datasets.imdbws.com/title.episode.tsv.gz
wget --timeout=10 https://datasets.imdbws.com/title.principals.tsv.gz
wget --timeout=10 https://datasets.imdbws.com/title.ratings.tsv.gz
cd ..

cd imdbpy
ls
python ./bin/s32imdbpy.py ../s3files postgres://$RDS_USER:$RDS_PASSWORD@$RDS_SERVER/$RDS_DATABASE