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
    SELECT 	ewapbca.piece_id as src_piece_id, nsp.piece_id as dst_piece_id
    FROM earliest_work_and_pieces_by_cluster ewapbca 
    LEFT JOIN non_source_pieces nsp USING(cluster_id)
    """),
    bucket=processed_bucket
).cache()
reception_edges.count()
#%%
defrag_pieces = get_s3("defrag_pieces",processed_bucket).cache()
defrag_pieces.count()
#%%
textreuse_ids = get_s3("textreuse_ids",processed_bucket).cache()
textreuse_ids.count()
#%%
# Mandeville Fable of the Bees
doc_id = '0672600300'

result = spark.sql(f"""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id = {doc_id}
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
""").cache()
result.count()
# %%
