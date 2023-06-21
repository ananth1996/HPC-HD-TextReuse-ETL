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
actor_ids = get_s3("actor_ids",processed_bucket)
edition_ids = get_s3("edition_ids",processed_bucket)
earliest_work_and_pieces_by_cluster = get_s3("earliest_work_and_pieces_by_cluster",processed_bucket)
clustered_defrag_pieces = get_s3("clustered_defrag_pieces",processed_bucket)
textreuse_edition_mapping = get_s3("textreuse_edition_mapping",processed_bucket)
defrag_pieces = get_s3("defrag_pieces",processed_bucket)
edition_authors = get_s3("edition_authors",processed_bucket)
coverages = get_s3("coverages",processed_bucket)
textreuse_ids = get_s3("textreuse_ids",processed_bucket)
#%%
actor_id = "22145254"
#%%
df = spark.sql(f"""
WITH non_source_pieces AS (
    SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
    RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
    WHERE work_id_i IS NULL -- where it is not the earliest piece
), non_source_pieces_denorm AS (
    SELECT * FROM non_source_pieces
    INNER JOIN defrag_pieces USING(piece_id)
    INNER JOIN textreuse_edition_mapping USING(trs_id)
    INNER JOIN edition_authors USING(edition_id_i)
    LEFT JOIN actor_ids USING(actor_id_i)
), reception_clusters AS (
    SELECT DISTINCT cluster_id FROM actor_ids
    INNER JOIN edition_authors USING(actor_id_i)
    INNER JOIN textreuse_edition_mapping USING(edition_id_i)
    INNER JOIN defrag_pieces USING(trs_id)
    INNER JOIN earliest_work_and_pieces_by_cluster USING(piece_id)
    WHERE actor_id = {actor_id!r}
), authors AS (
    SELECT DISTINCT actor_id_i FROM 
    reception_clusters
    INNER JOIN non_source_pieces_denorm USING(cluster_id)
    -- ensure we don't pick up same author again
    -- also don't take any anonymous authors
    WHERE actor_id IS NOT NULL AND actor_id <> {actor_id!r}
), dst_trs_and_cluster AS (
    SELECT DISTINCT trs_id,cluster_id FROM 
    non_source_pieces_denorm
    INNER JOIN authors USING(actor_id_i)
    -- don't pick original clusters again
    WHERE cluster_id NOT IN (SELECT * FROM reception_clusters)
), inception_edges AS (
    SELECT DISTINCT 
        dp.trs_id AS src_trs_id, 
        dtac.trs_id AS dst_trs_id 
    FROM 
    dst_trs_and_cluster  dtac
    INNER JOIN earliest_work_and_pieces_by_cluster USING(cluster_id)
    INNER JOIN defrag_pieces dp USING(piece_id)
), author_coverages AS (
    SELECT 
        ie1.dst_trs_id AS author_trs_id,
        ie1.src_trs_id AS inception_trs_id,
        reuse_t1_t2 AS reuses,
        coverage_t1_t2 AS coverage
    FROM 
    inception_edges ie1
    -- trs1 is inception
    -- trs2 is author 
    INNER JOIN coverages c1 ON ie1.src_trs_id = c1.trs1_id AND ie1.dst_trs_id = c1.trs2_id
    UNION ALL 
    SELECT 
        ie2.dst_trs_id AS author_trs_id,
        ie2.src_trs_id AS inception_trs_id,
        reuse_t2_t1 AS reuses,
        coverage_t2_t1 AS coverage
    FROM 
    inception_edges ie2
    -- trs1 is author
    -- trs2 is inception
    INNER JOIN coverages c2 ON ie2.src_trs_id = c2.trs2_id AND ie2.dst_trs_id = c2.trs1_id
)
SELECT 
    actor_id as author_id,
    name_unified AS author_name,
    ti_author.manifestation_id AS author_book,
    ti_inception.manifestation_id AS inception_book,
    reuses,
    coverage
FROM author_coverages ac
INNER JOIN textreuse_ids ti_author ON ti_author.trs_id = ac.author_trs_id
INNER JOIN textreuse_edition_mapping tem_author ON ac.author_trs_id = tem_author.trs_id
INNER JOIN edition_authors ea_author ON tem_author.edition_id_i = ea_author.edition_id_i
INNER JOIN actor_ids ai USING (actor_id_i)
-- get inception docuement id
INNER JOIN textreuse_ids ti_inception ON ti_inception.trs_id = ac.inception_trs_id
""")
    # %%