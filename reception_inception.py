#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from pathlib import Path
from spark_utils import *
from pyspark.sql.functions import col
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
#%%
earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
#%%
# create a downstream 
reception_edges = materialise_s3_if_not_exists(
    fname = "reception_edges",
    df = spark.sql("""
    WITH non_source_pieces AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
    )
    SELECT ewapbca.piece_id as src_piece_id, nsp.piece_id as dst_piece_id
    FROM earliest_work_and_pieces_by_cluster ewapbca 
    LEFT JOIN non_source_pieces nsp USING(cluster_id)
    """),
    bucket=processed_bucket,
)
# spark.sql("ANALYZE TABLE reception_edges COMPUTE STATISTICS FOR COLUMNS src_piece_id,dst_piece_id")
# reception_edges.count()
#%%
defrag_pieces = get_s3("defrag_pieces",processed_bucket)
# spark.sql("ANALYZE TABLE defrag_pieces COMPUTE STATISTICS FOR COLUMNS piece_id")
# defrag_pieces.count()
#%%
textreuse_ids = get_s3("textreuse_ids",processed_bucket)
# spark.sql("ANALYZE TABLE textreuse_ids COMPUTE STATISTICS FOR COLUMNS trs_id,manifestation_id")
# textreuse_ids.count()
#%%
# Mandeville Fable of the Bees
doc_id = '0672600300'
#%%
result1 = spark.sql(f"""
SELECT 
    dp1.trs_id AS src_trs_id,
    dp1.trs_start AS src_trs_start,
    dp1.trs_end AS src_trs_end,
    dp2.trs_id AS dst_trs_id,
    dp2.trs_start AS dst_trs_start,
    dp2.trs_end AS dst_trs_end
FROM 
reception_edges re
INNER JOIN defrag_pieces dp1 ON re.src_piece_id = dp1.piece_id
INNER JOIN textreuse_ids ti ON ti.trs_id = dp1.trs_id
INNER JOIN defrag_pieces dp2 ON re.dst_piece_id = dp2.piece_id
WHERE ti.manifestation_id = {doc_id!r}
""")
result1.count()
#%%
result2 = spark.sql(f"""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id = {doc_id!r}
), doc_pieces AS (
    SELECT * FROM defrag_pieces 
    INNER JOIN doc_trs_ids USING(trs_id)
)
SELECT 
    doc.trs_id AS src_trs_id,
    doc.trs_start AS src_trs_start,
    doc.trs_end AS src_trs_end,
    dp.trs_id AS dst_trs_id,
    dp.trs_start AS dst_trs_start,
    dp.trs_end AS dst_trs_end
FROM reception_edges 
INNER JOIN doc_pieces doc ON src_piece_id = doc.piece_id
INNER JOIN defrag_pieces dp ON dst_piece_id = dp.piece_id
""")
result2.count()
# %%
result3 = spark.sql(f"""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id = {doc_id!r}
), doc_pieces AS (
    SELECT piece_id FROM defrag_pieces 
    INNER JOIN doc_trs_ids USING(trs_id)
), doc_reception_edges AS 
(
    SELECT src_piece_id,dst_piece_id
    FROM reception_edges 
    INNER JOIN doc_pieces doc ON src_piece_id = doc.piece_id
)
SELECT 
    dp1.trs_id AS src_trs_id,
    dp1.trs_start AS src_trs_start,
    dp1.trs_end AS src_trs_end,
    dp2.trs_id AS dst_trs_id,
    dp2.trs_start AS dst_trs_start,
    dp2.trs_end AS dst_trs_end
FROM doc_reception_edges
INNER JOIN defrag_pieces dp1 ON src_piece_id = dp1.piece_id
INNER JOIN defrag_pieces dp2 ON dst_piece_id = dp2.piece_id
""")
result3.count()
#%%