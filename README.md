# Genetic evidence enrichment pipeline for CACHE

## Set up the environment
```bash
python3 -m venv env
python3 -m pip -q install -r requirements.txt
```

## Download the data
```bash
export RELEASE=21.04
export DATASETS="targets diseases associationByDatatypeDirect"
for DATASET in ${DATASETS}; do
  wget --recursive --no-parent --no-host-directories --cut-dirs 8 \
    ftp://ftp.ebi.ac.uk/pub/databases/opentargets/platform/${RELEASE}/output/etl/parquet/${DATASET}
done
```

## Run
```bash
source env/bin/activate
python3 cache-genetic-evidence.py
```

## Current results
```
Total targets: 60608
  Of them, protein coding: 19933
Total diseases: 18497
  Of them, genetic diseases: 8147
```
