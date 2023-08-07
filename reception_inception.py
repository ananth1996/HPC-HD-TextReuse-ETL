#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from spark_utils import *
from pyspark.sql.functions import col
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
from time import perf_counter as time
import numpy as np
import pandas as pd
from tqdm.autonotebook import trange,tqdm
#%%
earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
defrag_pieces = get_s3("defrag_pieces",processed_bucket)
textreuse_ids = get_s3("textreuse_ids",processed_bucket)
#%%
# materialize a downstream table specifically for the task
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
    -- only if a cluster has non_source pieces add edges
    --  hence, some clusters which are only source pieces will not have edges
    INNER JOIN non_source_pieces nsp USING(cluster_id)
    """),
    bucket=processed_bucket,
)
#%%
## Create a denormalized version of the reception edges 
##   here the pieces are denormalizes to ensure quicker searches
reception_edges_denorm = materialise_s3_if_not_exists(
    fname="reception_edges_denorm",
    df = spark.sql("""
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
    INNER JOIN defrag_pieces dp2 ON re.dst_piece_id = dp2.piece_id
    """),
    bucket=denorm_bucket
)
#%%
