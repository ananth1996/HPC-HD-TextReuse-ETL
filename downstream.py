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
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
#%%[markdown]
## Metadata Gathering for Downstream Taskss
#%%
estc_core = get_s3("estc_core",bucket=raw_bucket)
ecco_core = get_s3("ecco_core",bucket=raw_bucket)
eebo_core = get_s3("eebo_core",bucket=raw_bucket)
#%%[markdown]
# Create INT ids for the manifestation ids which are 
#  ECCO and EEBO_TCP
#%%
manifestation_ids = materialise_row_numbers(
    fname="manifestation_ids",
    df=spark.sql("""
    SELECT * FROM(
        SELECT DISTINCT ecco_id AS manifestation_id FROM ecco_core
        UNION ALL 
        SELECT DISTINCT eebo_tcp_id AS manifestation_id FROM eebo_core 
        WHERE eebo_tcp_id IS NOT NULL
    )
    ORDER BY manifestation_id
    """),
    col_name="manifestation_id_i",
    bucket=processed_bucket
)
#%%[markdown]
# create mapping between documents from ECCO and EEBO_TCP to ESTC id.
# There are 1143 EEBO_TCP documents that don'y have a ESTC id, 
#   in those cases just use the EEBO_TCP id as a placeholder
#   call this an edition_id 
#%%
edition_mapping = materialise_with_int_id(
    fname="edition_mapping",
    df=spark.sql("""
    SELECT DISTINCT
        manifestation_id_i,
        estc_id AS edition_id
    FROM ecco_core ecco
    INNER JOIN manifestation_ids mids ON mids.manifestation_id = ecco.ecco_id

    UNION ALL

    SELECT DISTINCT
        manifestation_id_i,
        (CASE 
            WHEN estc_id iS NULL THEN eebo_tcp_id
            ELSE estc_id
        END) AS edition_id
    FROM eebo_core eebo
    INNER JOIN manifestation_ids mids ON mids.manifestation_id = eebo.eebo_tcp_id
    """),
    col_name="edition_id",
    id_col_name="edition_id_i",
    keep_id_mapping=True,
    id_fname="edition_ids",
    bucket=processed_bucket,
    drop_col=True
)
edition_ids = get_s3("edition_ids",processed_bucket)
#%%[markdown]
# For each edition find the work_id from ESTC
#  and if the information is not present in ESTC (as for 113 ECCO documents)
#  or if the edition_id is new (the EEBO_TCP documents from above),
#  then make new work ids with a suffix
#%%
#%%
work_mapping = materialise_with_int_id(
    fname = "work_mapping",
    df=spark.sql("""
    SELECT DISTINCT
        em.manifestation_id_i,
        (CASE
            WHEN ec.work_id IS NULL THEN CONCAT(mids.manifestation_id,"-not_in_estc_core")
            ELSE ec.work_id
        END) AS work_id
    FROM edition_mapping em
    INNER JOIN manifestation_ids mids USING (manifestation_id_i)
    INNER JOIN edition_ids eids USING (edition_id_i)
    LEFT JOIN estc_core ec ON eids.edition_id = ec.estc_id
    """),
    col_name="work_id",
    id_col_name="work_id_i",
    keep_id_mapping=True,
    id_fname="work_ids",
    bucket=processed_bucket,
    drop_col=True
)
work_ids = get_s3("work_ids",processed_bucket)
#%%[markdown]
## Publication Year Metadata Gathering
# gather the publication year for each edition from ESTC first,
#   if information does not exits then take from ECCO or EEBO_TCP source
#   clean and put metadata in a table
#%%
edition_years = materialise_s3_if_not_exists(
    fname="edition_publication_year",
    df=spark.sql("""
    SELECT 
        em.edition_id_i,
        (CASE
            WHEN publication_year IS NULL THEN -- when estc_core doesn't have data
            (CASE 
                WHEN LENGTH(eebo_tls_publication_date) = 4 THEN CAST(eebo_tls_publication_date AS INT) -- Eg: 1697
                WHEN LENGTH(eebo_tls_publication_date) = 5 THEN CAST(SUBSTRING(eebo_tls_publication_date,-4) AS INT) -- Eg: -1697
                WHEN LENGTH(eebo_tls_publication_date) = 9 THEN CAST(SUBSTRING(eebo_tls_publication_date,1,4) AS INT) -- Eg: 1690-1697
                WHEN LENGTH(eebo_tls_publication_date) > 9 THEN CAST(SUBSTRING(eebo_tls_publication_date,-4) AS INT) -- Eg: April 24, 1649
            END)
            ELSE CAST(estc.publication_year AS INT)
        END) AS publication_year
    FROM eebo_core ec
    INNER JOIN manifestation_ids mids ON ec.eebo_tcp_id = mids.manifestation_id
    INNER JOIN edition_mapping em USING(manifestation_id_i)
    INNER JOIN edition_ids eids USING(edition_id_i)
    LEFT JOIN estc_core estc ON eids.edition_id = estc.estc_id

    UNION

    SELECT edition_id_i,
        (CASE
            WHEN publication_year IS NULL AND ec.ecco_date_start != 0 -- when estc_core doesn't have data
            THEN CAST(SUBSTRING(CAST(ec.ecco_date_start AS INT),1,4) AS INT) -- Eg: 1.7580101E7 -> 17580101 -> "1758" -> 1758
            WHEN publication_year IS NULL AND ec.ecco_date_start == 0 -- Don't record 0 years  
            THEN NULL
            ELSE CAST(estc.publication_year AS INT)
        END) AS publication_year
    FROM ecco_core ec
    INNER JOIN manifestation_ids mids ON ec.ecco_id = mids.manifestation_id
    INNER JOIN edition_mapping em USING(manifestation_id_i)
    INNER JOIN edition_ids eids USING(edition_id_i)
    LEFT JOIN estc_core estc ON eids.edition_id = estc.estc_id
    """),
    bucket=processed_bucket
)
#%%[markdown]
# For each work find the earliest year of publication based on the editions
#%%
work_earliest_publication_year = materialise_s3_if_not_exists(
    fname="work_earliest_publication_year",
    df=spark.sql("""
    SELECT work_id_i,MIN(publication_year) as publication_year FROM edition_publication_year
    LEFT JOIN edition_mapping USING(edition_id_i)
    LEFT JOIN work_mapping USING(manifestation_id_i)
    GROUP BY work_id_i
    """),
    bucket=processed_bucket
)
#%%[markdown]
# create new id mapping table for textreuse ids
#%%
textreuse_ids = get_s3("textreuse_ids",bucket=processed_bucket)
#%%
textreuse_edition_mapping = materialise_s3_if_not_exists(
    fname="textreuse_edition_mapping",
    df=spark.sql("""
    SELECT DISTINCT trs_id, edition_id_i
    FROM textreuse_ids ti
    INNER JOIN manifestation_ids mids USING(manifestation_id)
    INNER JOIN edition_mapping em USING(manifestation_id_i) 
    """),
    bucket=processed_bucket
)
#%%
textreuse_work_mapping = materialise_s3_if_not_exists(
    fname="textreuse_work_mapping",
    df=spark.sql("""
    SELECT DISTINCT trs_id,work_id_i
    FROM textreuse_ids ti
    INNER JOIN manifestation_ids USING (manifestation_id)
    INNER JOIN work_mapping wm USING (manifestation_id_i)
    """),
    bucket=processed_bucket
)
#%%[markdown]
# find the earliest publication of textreuse sources
#  from the editions they belong to 
#%%
textreuse_earliest_publication_year = materialise_s3_if_not_exists(
    fname="textreuse_earliest_publication_year",
    df=spark.sql("""
    SELECT 
        trs_id, 
        MIN(publication_year) as publication_year
    FROM textreuse_edition_mapping 
    INNER JOIN edition_publication_year epy USING(edition_id_i)
    GROUP BY trs_id
    """),
    bucket=processed_bucket
)
#%%[markdown]
# ## Load clusters
#%%
# load the clusters found from the Chinese Whispers algorithm 
clusters = get_s3("clusters",bucket=processed_bucket)
#%%
# create row numbers which are the defrag_piece_id
clustered_defrag_pieces = materialise_s3_if_not_exists(
     fname="clustered_defrag_pieces",
    df=spark.sql("""
    SELECT
    (row_number() OVER (ORDER BY monotonically_increasing_id())) AS piece_id, 
    cluster AS cluster_id 
    FROM clusters"""),
    bucket=processed_bucket
)
#%%[markdown]
# Find earliest textreuse source in each cluster
#%%
defrag_pieces = get_s3("defrag_pieces",bucket=processed_bucket)

earliest_textreuse_by_cluster = materialise_s3_if_not_exists(
    fname="earliest_textreuse_by_cluster",
    df=spark.sql("""
    SELECT cluster_id, trs_id
    FROM (
    SELECT 
        cluster_id, 
        trs_id,
        publication_year,
        MIN(publication_year) OVER (PARTITION BY cluster_id) AS min_publication_year
    FROM clustered_defrag_pieces cdp  
    INNER JOIN defrag_pieces dp USING (piece_id)
    INNER JOIN textreuse_earliest_publication_year tsepy USING (trs_id)
    )
    WHERE publication_year=min_publication_year
    """),
    bucket=processed_bucket
)
#%%[markdown]
# Find earliest work in cluster each cluster
#  and also the pieces of the text which is the earliest of that work
#    in that cluster
#%%                          
earliest_work_and_pieces_by_cluster = materialise_s3_if_not_exists(
    fname="earliest_work_and_pieces_by_cluster",
    df=spark.sql("""
    SELECT cluster_id, work_id_i, piece_id
    FROM (
    SELECT 
        cluster_id,
        work_id_i,
        piece_id,
        w.publication_year AS publication_year_work,
        t.publication_year AS publication_year_text, 
        MIN(w.publication_year) OVER (PARTITION BY cluster_id) AS min_publication_year_work, 
        MIN(t.publication_year) OVER (PARTITION BY cluster_id, work_id_i) AS min_publication_year_text
    FROM clustered_defrag_pieces cdp
    INNER JOIN defrag_pieces dp USING (piece_id)
    INNER JOIN textreuse_work_mapping twm USING (trs_id)
    INNER JOIN work_earliest_publication_year w USING (work_id_i)
    INNER JOIN textreuse_earliest_publication_year t USING (trs_id)
    )
    WHERE 
        publication_year_work=min_publication_year_work AND -- earliest work in cluster
        publication_year_text=min_publication_year_text -- earliest text in earliest work in cluster
    """),
    bucket=processed_bucket
)
#%%