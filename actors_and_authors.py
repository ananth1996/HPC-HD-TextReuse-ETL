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
source_piece_statistics = materialise_s3_if_not_exists(
    fname = "source_piece_statistics",
    df = spark.sql("""
    SELECT 
        src_piece_id AS piece_id,
        FIRST(cdp.cluster_id) AS cluster_id,
        FIRST(dp_src.trs_end)-FIRST(dp_src.trs_start) as piece_length,
        COUNT(*) AS num_reception_edges,
        COUNT(
            DISTINCT 
            CASE 
                WHEN twm_src.work_id_i <> twm_dst.work_id_i THEN twm_dst.work_id_i 
                ELSE NULL 
            END ) AS num_different_work_ids,
        COUNT(
            DISTINCT
            (CASE 
                -- if source has an author
                WHEN ea_src.actor_id_i IS NOT NULL AND 
                -- and the destination author is different or NULL
                (ea_src.actor_id_i <> ea_dst.actor_id_i OR ea_dst.actor_id_i IS NULL)  
                -- count the work_id of the destination
                THEN twm_dst.work_id_i
                -- if source author is not there then count destination works
                WHEN ea_src.actor_id_i IS NULL THEN twm_dst.work_id_i
                ELSE NULL 
            END)) AS num_work_ids_different_authors
    FROM reception_edges re
    INNER JOIN defrag_pieces dp_src ON re.src_piece_id = dp_src.piece_id
    INNER JOIN textreuse_edition_mapping tem_src ON tem_src.trs_id = dp_src.trs_id
    INNER JOIN edition_authors ea_src ON ea_src.edition_id_i = tem_src.edition_id_i
    INNER JOIN textreuse_work_mapping twm_src ON twm_src.trs_id = dp_src.trs_id
    INNER JOIN clustered_defrag_pieces cdp ON re.src_piece_id = cdp.piece_id
    -- destination pieces
    INNER JOIN defrag_pieces dp_dst ON re.dst_piece_id = dp_dst.piece_id
    INNER JOIN textreuse_edition_mapping tem_dst ON tem_dst.trs_id = dp_dst.trs_id
    INNER JOIN edition_authors ea_dst ON ea_dst.edition_id_i = tem_dst.edition_id_i
    INNER JOIN textreuse_work_mapping twm_dst ON twm_dst.trs_id = dp_dst.trs_id
    GROUP BY src_piece_id
    """),
    bucket=processed_bucket
)
#%%
estc_id = "R22993"
actor_id = "34459614"
work_mapping = get_s3("work_mapping",processed_bucket)
edition_mapping = get_s3("edition_mapping",processed_bucket)
manifestation_ids = get_s3("manifestation_ids",processed_bucket)
textreuse_ids = get_s3("textreuse_ids",processed_bucket)
# %%
unmat_result=spark.sql(f"""
WITH filtered_clusters AS (
    SELECT DISTINCT cluster_id FROM 
    edition_ids
    INNER JOIN textreuse_edition_mapping USING(edition_id_i)
    INNER JOIN defrag_pieces USING(trs_id)
    INNER JOIN earliest_work_and_pieces_by_cluster USING(piece_id)
    WHERE edition_id = {estc_id!r}
), non_source_pieces AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
), top_clusters AS (
 	SELECT 
        cluster_id,
        -- number of different work ids not 
        --  from the same author
        COUNT(DISTINCT work_id_i) as n_works
    FROM filtered_clusters 
	INNER JOIN non_source_pieces USING (cluster_id)
	INNER JOIN defrag_pieces USING (piece_id)
    INNER JOIN textreuse_edition_mapping USING(trs_id)
    INNER JOIN textreuse_work_mapping USING(trs_id)
    INNER JOIN edition_authors USING(edition_id_i)
    LEFT JOIN actor_ids USING(actor_id_i) -- don't drop NULLs
	WHERE actor_id <> {actor_id!r} OR actor_id IS NULL
	GROUP  BY cluster_id
	ORDER BY n_works DESC
), top_pieces AS (
	SELECT 
        cluster_id,
        dp.*,
        trs_end-trs_start AS piece_length,
        n_works 
    FROM top_clusters
	INNER JOIN earliest_work_and_pieces_by_cluster ewapbca  USING (cluster_id)
    INNER JOIN defrag_pieces dp USING(piece_id)
)
SELECT * FROM top_pieces
WHERE piece_length >= 150 AND piece_length<=300
ORDER BY n_works DESC,piece_length DESC
""")
unmat_result.show()
#%%
mat_result = spark.sql(f"""
SELECT dp.*,sps.cluster_id,piece_length,num_work_ids_different_authors FROM
edition_ids
INNER JOIN textreuse_edition_mapping USING(edition_id_i)
INNER JOIN defrag_pieces dp USING(trs_id)
INNER JOIN source_piece_statistics sps  USING(piece_id)
WHERE edition_id = {estc_id!r} AND piece_length >= 150 AND piece_length<=300
ORDER BY num_work_ids_different_authors DESC, piece_length DESC
""")
mat_result.show()
# %%
source_piece_statistics_denorm = materialise_s3_if_not_exists(
fname= "source_piece_statistics_denorm",
df=spark.sql("""
    SELECT * FROM 
    source_piece_statistics
    INNER JOIN defrag_pieces USING(piece_id)
    INNER JOIN textreuse_edition_mapping USING(trs_id)
"""),
bucket=denorm_bucket
)
# %%
denorm_result = spark.sql(f"""
SELECT * FROM 
source_piece_statistics_denorm
INNER JOIN edition_ids USING(edition_id_i)
WHERE edition_id = {estc_id!r} AND piece_length >= 150 AND piece_length<=300
ORDER BY num_work_ids_different_authors DESC, piece_length
""")
denorm_result.show()
#%%