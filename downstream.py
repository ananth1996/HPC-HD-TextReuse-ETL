#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from time import perf_counter as time
import argparse
import logging
import os
from pathlib import Path
import pandas as pd
import numpy as np
import sys
from spark_utils import *
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
#%%

#%%
estc_core = get_s3("estc_core",bucket="textreuse-raw-data")
ecco_core = get_s3("ecco_core",bucket="textreuse-raw-data")
eebo_core = get_s3("eebo_core",bucket="textreuse-raw-data")
#%%
edition_mapping = materialise_s3(
    fname="edition_mapping",
    df=spark.sql("""
    SELECT DISTINCT
        ecco_id AS manifestation_id,
        estc_id AS edition_id
    FROM ecco_core ecco

    UNION ALL

    SELECT DISTINCT
        eebo_tcp_id AS manifestation_id,
        (CASE 
            WHEN estc_id iS NULL THEN eebo_tcp_id
            ELSE estc_id
        END) AS edition_id
    FROM eebo_core
    WHERE eebo_tcp_id IS NOT NULL
    """),
    bucket="textreuse-processed-data"
)
#%%
work_mapping = materialise_s3(
    fname = "work_mapping",
    df=spark.sql("""
    SELECT DISTINCT
        em.manifestation_id,
        (CASE
            WHEN ec.work_id IS NULL THEN CONCAT(em.manifestation_id,"-not in estc_core")
            ELSE ec.work_id
        END) AS work_id
    FROM edition_mapping em
    LEFT JOIN estc_core ec ON em.edition_id = ec.estc_id
    """),
    bucket="textreuse-processed-data"
)
#%%

#%%
spark.sql("""
SELECT 
    work_id,
    min(publication_year) 
FROM estc_core 
GROUP BY work_id

UNION ALL

SELECT 
	CONCAT(ec.estc_id,"-estc from EEBO") AS work_id,
	MIN(
		CASE 
			WHEN LENGTH(eebo_tls_publication_date) = 4 THEN CAST(eebo_tls_publication_date AS INT) -- Eg: 1697
			WHEN LENGTH(eebo_tls_publication_date) = 5 THEN CAST(SUBSTRING(eebo_tls_publication_date,-4) AS INT) -- Eg: -1697
			WHEN LENGTH(eebo_tls_publication_date) = 9 THEN CAST(SUBSTRING(eebo_tls_publication_date,1,4) AS INT) -- Eg: 1690-1697
			WHEN LENGTH(eebo_tls_publication_date) > 9 THEN CAST(SUBSTRING(eebo_tls_publication_date,-4) AS INT) -- Eg: April 24, 1649
		END
		) AS publication_year
FROM eebo_core ec 
LEFT JOIN estc_core eca USING(estc_id)
WHERE eebo_tcp_id IS NOT NULL AND ec.estc_id IS NOT NULL AND eca.estc_id IS NULL
GROUP BY estc_id 

UNION ALL 

SELECT 
	CONCAT(ec.estc_id,"-estc_id from ECCO") AS work_id,
	MIN(CAST(SUBSTRING(ecco_date_start,1,4) AS INT)) AS publication_year
FROM ecco_core ec
LEFT JOIN estc_core eca USING(estc_id) 
WHERE eca.estc_id IS NULL
GROUP BY ec.estc_id 

UNION ALL 

SELECT 
	CONCAT(eebo_id,"-eebo_id from EEBO") AS work_id,
	MIN(
		CASE 
			WHEN LENGTH(eebo_tls_publication_date) = 4 THEN CAST(eebo_tls_publication_date AS INT) -- Eg: 1697
			WHEN LENGTH(eebo_tls_publication_date) = 5 THEN CAST(SUBSTRING(eebo_tls_publication_date,-4) AS INT) -- Eg: -1697
			WHEN LENGTH(eebo_tls_publication_date) = 9 THEN CAST(SUBSTRING(eebo_tls_publication_date,1,4) AS INT) -- Eg: 1690-1697
			WHEN LENGTH(eebo_tls_publication_date) > 9 THEN CAST(SUBSTRING(eebo_tls_publication_date,-4) AS INT) -- Eg: April 24, 1649
		END
		) AS publication_year
FROM eebo_core 
WHERE estc_id IS NULL AND eebo_tcp_id IS NOT NULL
GROUP BY eebo_id
""")
#%%

#%%
work_ids = materialise_row_numbers(
    fname="work_ids",
    df = spark.sql("SELECT DISTINCT work_id FROM estc_core"),
    col_name="work_id_i",
    bucket="textreuse-processed-data"
    )
#%%