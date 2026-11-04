"""PySpark transformations: raw -> silver -> gold, data-quality checks, and the run that ties them together.

schemas.py  explicit schemas of the raw JSON files
silver.py   pure functions: raw DataFrame -> clean silver DataFrame
gold.py     pure functions: silver DataFrames -> star-schema DataFrames
quality.py  data-quality checks (pure: DataFrames in, check results out)
tables.py   Iceberg table definitions, CREATE and MERGE
pipeline.py one transform run: read raw, write silver/gold, run checks, record the run
"""
