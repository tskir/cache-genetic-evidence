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
upstream_targets = spark.read.csv(args.upstream_targets, header=True)
uniprot_ensembl_mapping = (
    spark.read.csv(args.uniprot_ensembl_mapping, header=True, sep='\t')
    .withColumn('Ensembl', pf.split(pf.col('Ensembl'), '; '))
    .withColumn('Ensembl', pf.explode('Ensembl'))
    .filter(pf.col('Ensembl').isNotNull())
)

# OT platform: Filter only protein coding targets.
coding_targets = (
    ot_datasets['targets']
    .filter(pf.col('bioType') == 'protein_coding')
    .select('id')
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

print(f"""
Total UniProt AC → Ensembl gene ID mappings: {uniprot_ensembl_mapping.count():,}

Total OT targets: {ot_datasets['targets'].count():,}
  Of them, protein coding: {coding_targets.count():,}
Total OT diseases: {ot_datasets['diseases'].count():,}
  Of them, genetic diseases: {genetic_diseases.count():,}

Total upstream targets: {upstream_targets.count():,}
""")
