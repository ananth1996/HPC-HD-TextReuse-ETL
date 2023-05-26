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
# find the textreuse source for the given document
doc_trs_id = textreuse_ids.filter(f"manifestation_id={doc_id}").select("trs_id")
# find the pieces of the document
doc_pieces = defrag_pieces.join(doc_trs_id,"trs_id")
# find the reception edges 
doc_reception_edges = reception_edges.join(doc_pieces,col("src_piece_id")==col("piece_id")).select("src_piece_id","dst_piece_id")
# join back with piece info to get the offset locations of source and destination
result = (
    doc_reception_edges.
    join(defrag_pieces,col("src_piece_id")==col("piece_id")).
    select(
        col("trs_id").alias("src_trs_id"),
        col("trs_start").alias("src_trs_start"),
        col("trs_end").alias("src_trs_end"),
        "dst_piece_id"
    ).
    join(defrag_pieces,col("dst_piece_id")==col("piece_id"))
    .select(
        "src_trs_id",
        "src_trs_start",
        "src_trs_end",
        col("trs_id").alias("dst_trs_id"),
        col("trs_start").alias("dst_trs_start"),
        col("trs_end").alias("dst_trs_end"),
    )
).cache()
result.count()
# %%
