#%%
from etl_textreuse.spark_utils import *
#%%

all_tables = [
    ["textreuse_ids",processed_bucket,],
    ["manifestation_ids",processed_bucket,],
    ["edition_ids",processed_bucket,],
    ["work_ids",processed_bucket,],
    ["actor_ids",processed_bucket,],
    ["textreuse_work_mapping",processed_bucket,],
    ["textreuse_edition_mapping",processed_bucket,],
    ["work_mapping",processed_bucket,],
    ["edition_mapping",processed_bucket,],
    ["edition_publication_date",processed_bucket,],
    ["work_earliest_publication_date",processed_bucket,],
    ["textreuse_earliest_publication_date",processed_bucket,],
    ["textreuse_source_lengths",processed_bucket],
    ["edition_authors",processed_bucket,],
    ["estc_core",raw_bucket,],
    ["ecco_core",raw_bucket,],
    ["eebo_core",raw_bucket,],
    ["newspapers_core",raw_bucket,],
    ["estc_actor_links",raw_bucket,],
    ["estc_actors",raw_bucket,],
    ["defrag_pieces",processed_bucket,],
    ["defrag_textreuses",processed_bucket,],
    ["clustered_defrag_pieces",processed_bucket,],
    ["earliest_textreuse_by_cluster",processed_bucket,],
    ["earliest_work_and_pieces_by_cluster",processed_bucket,],
    ["reception_edges",processed_bucket,],
    ["source_piece_statistics",processed_bucket,],
    ["reception_edges_denorm",processed_bucket,],
    ["source_piece_statistics_denorm",processed_bucket,],
    ["coverages",processed_bucket,],
    ["manifestation_publication_date",processed_bucket],
    ["textreuse_manifestation_mapping",processed_bucket]
]
#%%
spark = get_spark_session(application_name="scratch")
#%%
for table,bucket in all_tables:
    get_s3(spark,table,bucket)
#%%
spark.sql("""
SELECT DISTINCT cluster_id FROM earliest_work_and_pieces_by_cluster ewapbc 
INNER JOIN defrag_pieces dp USING(piece_id)
INNER JOIN earliest_textreuse_by_cluster etbc USING (cluster_id)
WHERE etbc.trs_id <> dp.trs_id 
          """).count()

#%%
# time spans of clusters
spark.sql("""
          SELECT 
          cluster_id, 
          MAX(publication_date) as max_pub_date,
          MIN(publication_date) as min_pub_date,
          MAX(publication_date) - MIN(publication_date) as span
          FROM clustered_defrag_pieces 
          INNER JOIN defrag_pieces USING(piece_id)
          INNER JOIN textreuse_manifestation_mapping USING(trs_id)
          INNER JOIN manifestation_publication_date USING(manifestation_id_i)
          GROUP BY cluster_id
          ORDER BY span DESC
          LIMIT 100
""").show()