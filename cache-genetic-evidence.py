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
parser.add_argument(
    '--association-threshold', required=True, type=float, default=0.7,
    help='A minimum target-disease association score to consider relevant.'
)
parser.add_argument(
    '--output-dataset', required=True,
    help='A filename to save the processed dataset into.'
)
args = parser.parse_args()

# Initialise the Spark session and ingest the data.
spark = pyspark.sql.SparkSession.builder.appName('cache_genetic_evidence').getOrCreate()
ot_datasets = {d: spark.read.parquet(d) for d in ('targets', 'diseases', 'associationByDatatypeDirect')}
uniprot_ensembl_mapping = (
    spark.read.csv(args.uniprot_ensembl_mapping, header=True, sep='\t')
    .withColumn('targetId', pf.split(pf.col('targetId'), '; '))
    .withColumn('targetId', pf.explode('targetId'))
    .filter(pf.col('targetId').isNotNull())
)

# OT platform: Filter only protein coding targets.
coding_targets = (
    ot_datasets['targets']
    .filter(pf.col('bioType') == 'protein_coding')
    .select(pf.col('id').alias('targetId'))
    .distinct()
)

# OT platform: Filter only genetic, familial or congenital diseases.
genetic_diseases = (
    ot_datasets['diseases']
    .withColumn('therapeuticArea', pf.explode('therapeuticAreas'))
    .filter(pf.col('therapeuticArea') == 'OTAR_0000018')
    .select(pf.col('id').alias('diseaseId'))
    .distinct()
)

# OT associations: Only retain target-disease associations which fit all of the criteria:
relevant_associations = (
    ot_datasets['associationByDatatypeDirect']
    # 1. Only consider genetic association evidence
    .filter(pf.col('datatypeId') == 'genetic_association')
    # 2. Only consider evidence with a score above the threshold
    .filter(pf.col('datatypeHarmonicScore') >= args.association_threshold)
    # 3. Only consider inherited/genetic diseases
    .join(genetic_diseases, on='diseaseId', how='inner')
    # 4. Only consider protein coding targets
    .join(coding_targets, on='targetId', how='inner')
    .select('targetId', 'diseaseId', 'diseaseLabel', 'datatypeHarmonicScore')
)

# Load and process the upstream targets.
upstream_targets = spark.read.csv(args.upstream_targets, header=True)

# Map UniProt ACs into Ensembl gene IDs.
upstream_targets_mapped = (
    upstream_targets
    .join(uniprot_ensembl_mapping, on='UniProt', how='inner')
    .filter(pf.col('targetId').isNotNull())
)

# Only retain the targets which are protein coding according to OT target index.
upstream_targets_coding = (
    upstream_targets_mapped
    .join(coding_targets, on='targetId', how='inner')
)

# Only retain the targets which have relevant OT associations.
upstream_targets_associated = (
    upstream_targets_coding
    .join(relevant_associations, on='targetId', how='inner')
)

# Save the resulting dataset.
upstream_targets_associated.toPandas().to_csv(args.output_dataset, sep='\t', index=False)

print(f"""
### Successive target list filtering statistics
* On load: {upstream_targets.select('UniProt').distinct().count():,} distinct ({upstream_targets.count():,} total)
* Mapped to Ensembl gene IDs: {upstream_targets_mapped.select('UniProt').distinct().count():,} distinct ({upstream_targets_mapped.count():,} total)
* Present in the OT target index as protein coding: {upstream_targets_coding.select('UniProt').distinct().count():,} distinct ({upstream_targets_coding.count():,} total)
* Have good OT associations: {upstream_targets_associated.select('UniProt').distinct().count():,} distinct ({upstream_targets_associated.count():,} total)

### Dataset statistics
Total UniProt AC → Ensembl gene ID mappings: {uniprot_ensembl_mapping.count():,}

Open Targets Platform data:
* Total OT targets: {ot_datasets['targets'].count():,}
  + Of them, protein coding: {coding_targets.count():,}
* Total OT diseases: {ot_datasets['diseases'].count():,}
  + Of them, genetic diseases: {genetic_diseases.count():,}
* Total OT direct associations by datatype: {ot_datasets['associationByDatatypeDirect'].count():,}
  + Of them, fitting all criteria (genetic association, good score, coding target, genetic disease): {relevant_associations.count():,}
""")
