# Genetic evidence enrichment pipeline for CACHE

## Set up the environment
```bash
python3 -m venv env
python3 -m pip -q install -r requirements.txt
```

## Download the data
```bash
# Open Targets Platform release.
export RELEASE=21.04
export DATASETS="targets diseases associationByDatatypeDirect"
for DATASET in ${DATASETS}; do
  wget --recursive --no-parent --no-host-directories --cut-dirs 8 \
    ftp://ftp.ebi.ac.uk/pub/databases/opentargets/platform/${RELEASE}/output/etl/parquet/${DATASET}
done

# UniProt to Ensembl mapping.
echo -e 'UniProt\ttargetId' > uniprot_ensembl_mapping.tsv
wget -qO- https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping_selected.tab.gz \
  | gzip -cd | cut -d$'\t' -f1,19 >> uniprot_ensembl_mapping.tsv
```

The input list of targets is generated upstream and shared over email.

## Run
```bash
source env/bin/activate
python3 cache-genetic-evidence.py \
  --upstream-targets CACHE_List_SAR_GWAS_DiseaseV2.csv \
  --uniprot-ensembl-mapping uniprot_ensembl_mapping.tsv \
  --association-threshold 0.7
```

## Current results
```
Total UniProt AC → Ensembl gene ID mappings: 81,097

Total OT targets: 60,608
  Of them, protein coding: 19,933
Total OT diseases: 18,497
  Of them, genetic diseases: 8,147
Total OT direct associations by datatype: 2,653,077
  Of them, fitting all criteria (genetic association, good score, coding target, genetic disease): 13,697

Successive filtering of upstream targets:
- On load: 6,742 distinct (6,742 total)
- Mapped to Ensembl gene IDs: 6,617 distinct (7,485 total)
- Present in the OT target index as protein coding: 6,385 distinct (6,439 total)
```
