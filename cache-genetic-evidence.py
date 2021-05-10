#!/usr/bin/env python3

import pyspark
import pyspark.sql.functions as pf

spark = pyspark.sql.SparkSession.builder.appName('cache_genetic_evidence').getOrCreate()
datasets = {d: spark.read.parquet(d) for d in ('targets', 'diseases', 'associationByDatatypeDirect')}

# Filter only protein coding targets.
coding_targets = (
    datasets['targets']
    .filter(pf.col('bioType') == 'protein_coding')
    .select('id')
    .distinct()
)

# Filter only genetic, familial or congenital diseases.
genetic_diseases = (
    datasets['diseases']
    .withColumn('therapeuticArea', pf.explode('therapeuticAreas'))
    .filter(pf.col('therapeuticArea') == 'OTAR_0000018')
    .select('id')
    .distinct()
)

print(f"""
Total targets: {datasets['targets'].count()}
  Of them, protein coding: {coding_targets.count()}
Total diseases: {datasets['diseases'].count()}
  Of them, genetic diseases: {genetic_diseases.count()}
""")
