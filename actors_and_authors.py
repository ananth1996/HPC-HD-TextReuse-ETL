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
from pyspark.sql.functions import *
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
#%%
estc_actors = get_s3("estc_actors",raw_bucket)
estc_actor_links = get_s3("estc_actor_links",raw_bucket)
edition_ids = get_s3("edition_ids",processed_bucket)
#%%
# gather the actor_ids and their name and make INT ids

actor_ids = materialise_row_numbers(
    fname="actor_ids",
    df=spark.sql("""
    SELECT DISTINCT actor_id,name_unified
    FROM estc_actors
    ORDER BY actor_id,name_unified
    """),
    col_name="actor_id_i",
    bucket=processed_bucket
    
)
#%%
# For every edition we have find the actors who are authors
edition_authors = materialise_s3_if_not_exists(
    fname = "edition_authors",
    df = spark.sql("""
    SELECT edition_id_i, actor_id_i
    FROM
    edition_ids ei
    LEFT JOIN estc_actor_links eal ON
    eal.estc_id = ei.edition_id 
    -- Ensure we only take actors who are authors
    AND eal.actor_role_author = 1
    LEFT JOIN actor_ids ai ON eal.actor_id = ai.actor_id
    """),
    bucket = processed_bucket

)
#%%
earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
reception_edges = get_s3("reception_edges",processed_bucket)
defrag_pieces = get_s3("defrag_pieces",processed_bucket)
textreuse_edition_mapping = get_s3("textreuse_edition_mapping",processed_bucket)
textreuse_work_mapping = get_s3("textreuse_work_mapping",processed_bucket)
clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
#%%
spark.sql("""
SELECT src_piece_id,dst_piece_id,
SUM()
FROM reception_edges re
INNER JOIN defrag_pieces dp_src ON re.src_piece_id = dp_src.piece_id
INNER JOIN textreuse_edition_mapping tem_src ON tem_src.trs_id = dp_src.trs_id
INNER JOIN edition_authors ea_src ON ea_src.edition_id_i = tem_src.edition_id_i
INNER JOIN textreuse_work_mapping twm_src ON twm_src.trs_id = dp_src.trs_id
-- destination pieces
INNER JOIN defrag_pieces dp_dst ON re.dst_piece_id = dp_dst.piece_id
INNER JOIN textreuse_edition_mapping tem_dst ON tem_dst.trs_id = dp_dst.trs_id
INNER JOIN edition_authors ea_dst ON ea_dst.edition_id_i = tem_dst.edition_id_i
INNER JOIN textreuse_work_mapping twm_dst ON twm_dst.trs_id = dp_dst.trs_id
WHERE 
twm_src.work_id_i <> twm_dst.work_id_i
AND 
CASE 
    WHEN ea_src.actor_id_i IS NULL THEN 
""").show() 
# %%
non_source_pieces= spark.sql("""
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
    """)