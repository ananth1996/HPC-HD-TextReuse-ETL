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
from pyspark.sql.functions import to_date,col
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
#%%[markdown]
## Metadata Gathering for Downstream Tasks

newspapers_raw_csv = f"s3a://{raw_bucket}/bl_newspapers_meta.csv"
newspapers_raw_parquet = f"s3a://{raw_bucket}/newspapers_core.parquet"
if s3_uri_exists(newspapers_raw_parquet,raw_bucket):
    newspapers_core = get_s3("newspapers_core",raw_bucket)
else:
    newspapers_core = spark.read.option("header",True).csv(newspapers_raw_csv)
    newspapers_core.createOrReplaceTempView("newspapers_core")
    newspapers_core = materialise_s3(
        fname="newspapers_core",
        df=spark.sql("""
        SELECT *,
        (CASE 
        WHEN to_date(issue_date_start,'yyyy-MM-dd') IS NULL -- when str is '1732-00-00'
        THEN to_date(SUBSTRING(issue_date_start,1,4),'yyyy') '1732-00-00' -> '1732' -> 1732-01-01
        ELSE to_date(issue_date_start,'yyyy-MM-dd')
        END) AS issue_start_date,
        (CASE 
        WHEN to_date(issue_date_end,'yyyy-MM-dd') IS NULL 
        THEN to_date(SUBSTRING(issue_date_end,1,4),'yyyy')
        ELSE to_date(issue_date_end,'yyyy-MM-dd')
        END) AS issue_end_date
        FROM newspapers_core
        """),
        bucket=raw_bucket
    )
#%%
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
        UNION ALL 
        SELECT DISTINCT article_id AS manifestation_id FROM newspapers_core
    )
    ORDER BY manifestation_id
    """),
    col_name="manifestation_id_i",
    bucket=processed_bucket
)
#%%[markdown]
# create mapping between documents from manifestations and editions.
# For books this is bewtwen ECCO and EEBO_TCP to ESTC id.
# There are 1143 EEBO_TCP documents that don'y have a ESTC id, 
#       in those cases just use the EEBO_TCP id as a placeholder
#       call this an edition_id.
# Similarly, there are some ECCO documents that don't have an ESTC id,
#       in those cases the ECCO id is used as placeholder edition
# For newspapers there is not true edition. While there are issues, these
# are logically different. Therefore, we just map each article to its own individual issue
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
                 
    UNION ALL 
                 
    SELECT 
        manifestation_id_i,
        article_id AS edition_id 
    FROM newspapers_core news
    INNER JOIN manifestation_ids mids ON mids.manifestation_id = news.article_id
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
#  then make new work ids with the same manifestation_id
# This will the default for the newspapers data which has no mapping to ESTC
#%%
#%%
work_mapping = materialise_with_int_id(
    fname = "work_mapping",
    df=spark.sql("""
    SELECT DISTINCT
        em.manifestation_id_i,
        (CASE
            WHEN ec.work_id IS NULL THEN mids.manifestation_id
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
edition_dates = materialise_s3_if_not_exists(
    fname="edition_publication_date",
    df=spark.sql("""
    SELECT 
        em.edition_id_i,
        (CASE
            WHEN publication_year IS NULL THEN -- when estc_core doesn't have data
            (CASE 
                WHEN LENGTH(eebo_tls_publication_date) = 4 THEN to_date(eebo_tls_publication_date,'yyyy') -- Eg: 1697
                WHEN LENGTH(eebo_tls_publication_date) = 5 THEN to_date(SUBSTRING(eebo_tls_publication_date,-4),'yyyy') -- Eg: -1697
                WHEN LENGTH(eebo_tls_publication_date) = 9 THEN to_date(SUBSTRING(eebo_tls_publication_date,1,4), 'yyyy') -- Eg: 1690-1697
                WHEN LENGTH(eebo_tls_publication_date) > 9 THEN to_date(eebo_tls_publication_date,'LLLL d, yyyy') -- Eg: April 24, 1649
            END)
            ELSE to_date(CAST(estc.publication_year AS INT),'yyyy')
        END) AS publication_date
    FROM eebo_core ec
    INNER JOIN manifestation_ids mids ON ec.eebo_tcp_id = mids.manifestation_id
    INNER JOIN edition_mapping em USING(manifestation_id_i)
    INNER JOIN edition_ids eids USING(edition_id_i)
    LEFT JOIN estc_core estc ON eids.edition_id = estc.estc_id

    UNION

    SELECT edition_id_i,
        (CASE
            WHEN publication_year IS NULL AND ec.ecco_date_start != 0 -- when estc_core doesn't have data
            THEN to_date(SUBSTRING(CAST(ec.ecco_date_start AS INT),1,4),'yyyy') -- Eg: 1.7580101E7 -> 17580101 -> "1758" -> date(1758-01-01)
            WHEN publication_year IS NULL AND ec.ecco_date_start == 0 -- Don't record 0 years  
            THEN NULL
            ELSE to_date(CAST(estc.publication_year AS INT),'yyyy')
        END) AS publication_date
    FROM ecco_core ec
    INNER JOIN manifestation_ids mids ON ec.ecco_id = mids.manifestation_id
    INNER JOIN edition_mapping em USING(manifestation_id_i)
    INNER JOIN edition_ids eids USING(edition_id_i)
    LEFT JOIN estc_core estc ON eids.edition_id = estc.estc_id
    
    UNION 
                 
    SELECT edition_id_i, issue_start_date AS publication_date
    FROM newspapers_core nc
    INNER JOIN manifestation_ids mids ON nc.article_id = mids.manifestation_id
    INNER JOIN edition_mapping em USING(manifestation_id_i)
    """),
    bucket=processed_bucket
)#%%
#%%[markdown]
# For each work find the earliest year of publication based on the editions
#%%
work_earliest_publication_date = materialise_s3_if_not_exists(
    fname="work_earliest_publication_date",
    df=spark.sql("""
    SELECT work_id_i,MIN(publication_date) as publication_date FROM edition_publication_date
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
textreuse_earliest_publication_date = materialise_s3_if_not_exists(
    fname="textreuse_earliest_publication_date",
    df=spark.sql("""
    SELECT 
        trs_id, 
        MIN(publication_date) as publication_date
    FROM textreuse_edition_mapping 
    INNER JOIN edition_publication_date epy USING(edition_id_i)
    GROUP BY trs_id
    """),
    bucket=processed_bucket
)
#%%[markdown]
# ## Load clusters
#%%
# load the clusters found from the Chinese Whispers algorithm 
clusters = get_s3("clusters_counts_100",bucket=processed_bucket,table_name="clusters")
#%%
# create row numbers which are the defrag_piece_id
clustered_defrag_pieces = materialise_s3_if_not_exists(
     fname="clustered_defrag_pieces",
    df=spark.sql("""
    SELECT
    piece_id, 
    cluster_id 
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
        publication_date,
        MIN(publication_date) OVER (PARTITION BY cluster_id) AS min_publication_date
    FROM clustered_defrag_pieces cdp  
    INNER JOIN defrag_pieces dp USING (piece_id)
    INNER JOIN textreuse_earliest_publication_date USING (trs_id)
    )
    WHERE publication_date=min_publication_date
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
        w.publication_date AS publication_date_work,
        t.publication_date AS publication_date_text, 
        MIN(w.publication_date) OVER (PARTITION BY cluster_id) AS min_publication_date_work, 
        MIN(t.publication_date) OVER (PARTITION BY cluster_id, work_id_i) AS min_publication_date_text
    FROM clustered_defrag_pieces cdp
    INNER JOIN defrag_pieces dp USING (piece_id)
    INNER JOIN textreuse_work_mapping twm USING (trs_id)
    INNER JOIN work_earliest_publication_date w USING (work_id_i)
    INNER JOIN textreuse_earliest_publication_date t USING (trs_id)
    )
    WHERE 
        publication_date_work=min_publication_date_work AND -- earliest work in cluster
        publication_date_text=min_publication_date_text -- earliest text in earliest work in cluster
    """),
    bucket=processed_bucket
)
#%%