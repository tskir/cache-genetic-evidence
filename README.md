# Genetic evidence enrichment pipeline for CACHE

## Running instructions

### Set up the environment
```bash
python3 -m venv env
python3 -m pip -q install -r requirements.txt
```

### Download the data
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

### Run
```bash
source env/bin/activate
python3 cache-genetic-evidence.py \
  --upstream-targets CACHE_List_SAR_GWAS_DiseaseV2.csv \
  --uniprot-ensembl-mapping uniprot_ensembl_mapping.tsv \
  --association-threshold 0.7 \
  --output-dataset results.tsv
```

## Summary of operations
For the input target list (UniProt ACs), add associations from the Open Targets platform data which fit certain criteria.

Open Targets Platform associations are ingested from `associationByDatatypeDirect`. Only the following associations are considered:
* Datatype ID = **genetic association**
* Datatype harmonic score >= **0.7** (current threshold, arbitrarily chosen). Filter by datatype evidence count is currently **not** applied.
* Association target is **protein coding** according to OT target index.
* Association disease is a **genetic disease,** as calculated by having OTAR_0000018 (“genetic, familial or congenital disease”) in the list of therapeutic areas.

The output is the spreadsheet obtained at input, containing only the rows for which at least one association was found. If a target has multiple associations, it will be exploded into multiple rows accordingly. For this reason, at each processing step two metrics are calculated: the total number of associations, and the number of distinct original targets.

## Current results

This section is generated automatically by the pipeline on each run. The results below are for run 2021-05-12 on input data 2021-05-07.

----

### Identifier mapping statistics
Total UniProt AC → Ensembl gene ID mappings: 81,097

### Open Targets data statistics
* Total OT targets: 60,608
  + Of them, protein coding: 19,933
* Total OT diseases: 18,497
  + Of them, genetic diseases: 8,147
* Total OT direct associations by datatype: 2,653,077
  + Of them, fitting all criteria (genetic association, good score, coding target, genetic disease): 13,697

### Upstream target list filtering
This is performed in a successive manner (one step after the other):
* On load: 6,742 distinct (6,742 total)
* Mapped to Ensembl gene IDs: 6,617 distinct (7,485 total)
* Present in the OT target index as protein coding: 6,385 distinct (6,439 total)
* Have good OT associations: 1,968 distinct (5,992 total)
