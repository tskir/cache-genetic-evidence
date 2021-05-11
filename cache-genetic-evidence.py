#!/usr/bin/env python3

import argparse

import pyspark
import pyspark.sql.functions as pf

parser = argparse.ArgumentParser()
parser.add_argument(
    '--upstream-targets', required=True,
    help='A CSV list of targets, generated upstream.'
)
parser.add_argument(
    '--uniprot-ensembl-mapping', required=True,
    help='A TSV mapping from UniProt AC to Ensembl gene ID.'
)
args = parser.parse_args()

# Initialise the Spark session and ingest the data.
spark = pyspark.sql.SparkSession.builder.appName('cache_genetic_evidence').getOrCreate()
ot_datasets = {d: spark.read.parquet(d) for d in ('targets', 'diseases', 'associationByDatatypeDirect')}
uniprot_ensembl_mapping = (
    spark.read.csv(args.uniprot_ensembl_mapping, header=True, sep='\t')
    .withColumn('Ensembl gene ID', pf.split(pf.col('Ensembl gene ID'), '; '))
    .withColumn('Ensembl gene ID', pf.explode('Ensembl gene ID'))
    .filter(pf.col('Ensembl gene ID').isNotNull())
)

# OT platform: Filter only protein coding targets.
coding_targets = (
    ot_datasets['targets']
    .filter(pf.col('bioType') == 'protein_coding')
    .select(pf.col('id').alias('Ensembl gene ID'))
    .distinct()
)

# OT platform: Filter only genetic, familial or congenital diseases.
genetic_diseases = (
    ot_datasets['diseases']
    .withColumn('therapeuticArea', pf.explode('therapeuticAreas'))
    .filter(pf.col('therapeuticArea') == 'OTAR_0000018')
    .select('id')
    .distinct()
)

# Load and process the upstream targets.
upstream_targets = spark.read.csv(args.upstream_targets, header=True)

# Map UniProt ACs into Ensembl gene IDs.
upstream_targets_mapped = (
    upstream_targets
    .join(uniprot_ensembl_mapping, on='UniProt', how='inner')
    .filter(pf.col('Ensembl gene ID').isNotNull())
)

# Only retain the targets which are protein coding according to OT target index.
upstream_targets_coding = (
    upstream_targets_mapped
    .join(coding_targets, on='Ensembl gene ID', how='inner')
)

print(f"""
Total UniProt AC → Ensembl gene ID mappings: {uniprot_ensembl_mapping.count():,}

Total OT targets: {ot_datasets['targets'].count():,}
  Of them, protein coding: {coding_targets.count():,}
Total OT diseases: {ot_datasets['diseases'].count():,}
  Of them, genetic diseases: {genetic_diseases.count():,}

Successive filtering of upstream targets:
- On load: {upstream_targets.select('UniProt').distinct().count():,} distinct ({upstream_targets.count():,} total)
- Mapped to Ensembl gene IDs: {upstream_targets_mapped.select('UniProt').distinct().count():,} distinct ({upstream_targets_mapped.count():,} total)
- Present in the OT target index as protein coding: {upstream_targets_coding.select('UniProt').distinct().count():,} distinct ({upstream_targets_coding.count():,} total)
""")
