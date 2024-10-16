from etl_textreuse.spark_utils import *
from dagster import asset, Output
from etl_textreuse.assets.chinese_label_propagation import clusters
from etl_textreuse.assets.defragmentation import defrag_pieces
# %%


@asset(
    deps=[clusters],
    description="The mapping of each defragmented piece to a unique cluster",
    group_name="textreuses"
)
def clustered_defrag_pieces() -> Output[None]:
    spark = get_spark_session(
        project_root, application_name="clustered degrag pieces")
    #TODO fix the clustering phase
    # for the time being take manaually run 100 iterations of Chinese Label Propagation
    get_s3(spark, "clusters_counts_0", processed_bucket,
           table_name="clusters_counts")
    materialise_s3(
        spark,
        fname="clustered_defrag_pieces",
        df=spark.sql("""
        SELECT
        piece_id, 
        cluster_id
        FROM clusters_counts"""),
        bucket=processed_bucket
    )

    num_clusters = spark.sql(
        "SELECT DISTINCT(cluster_id) FROM clusters_counts").count()
    return Output(None, metadata={"Number of Clusters": num_clusters})


# @asset(
#     deps=[clustered_defrag_pieces, defrag_pieces,
#           textreuse_earliest_publication_date],
#     description="The earliest textreuse in each cluster",
#     group_name="downstream_textreuses"
# )
# def textreuse_earliest_publication_date() -> None:
#     spark = get_spark_session(
#         project_root, application_name="Earliest textreuse in cluster")
#     get_s3(spark, "clustered_defrag_pieces", processed_bucket)
#     get_s3(spark, "defrag_pieces", processed_bucket)
#     get_s3(spark, "textreuse_earliest_publication_date", processed_bucket)
#     materialise_s3(
#         spark,
#         fname="earliest_textreuse_by_cluster",
#         df=spark.sql("""
#         SELECT cluster_id, trs_id
#         FROM (
#         SELECT 
#             cluster_id, 
#             trs_id,
#             publication_date,
#             MIN(publication_date) OVER (PARTITION BY cluster_id) AS min_publication_date
#         FROM clustered_defrag_pieces cdp  
#         INNER JOIN defrag_pieces dp USING (piece_id)
#         INNER JOIN textreuse_earliest_publication_date USING (trs_id)
#         )
#         WHERE publication_date=min_publication_date
#         """),
#         bucket=processed_bucket
#     )

# Find earliest work in cluster each cluster
#  and also the pieces of the text which is the earliest of that work
#    in that cluster


# @asset(
#     deps=[clustered_defrag_pieces, textreuse_earliest_publication_date,
#           work_earliest_publication_date, defrag_pieces],
#     description="The earliest work and corresponding piece in each cluster",
#     group_name="downstream_textreuses"
# )
# def earliest_work_and_pieces_by_cluster() -> None:
#     spark = get_spark_session(
#         project_root, application_name="Earliest work and piece in cluster")
#     get_s3(spark, "clustered_defrag_pieces", processed_bucket)
#     get_s3(spark, "defrag_pieces", processed_bucket)
#     get_s3(spark, "textreuse_earliest_publication_date", processed_bucket)
#     get_s3(spark, "textreuse_work_mapping", processed_bucket)
#     get_s3(spark, "work_earliest_publication_date", processed_bucket)
#     materialise_s3(
#         spark,
#         fname="earliest_work_and_pieces_by_cluster",
#         df=spark.sql("""
#         SELECT cluster_id, work_id_i, piece_id
#         FROM (
#         SELECT 
#             cluster_id,
#             work_id_i,
#             piece_id,
#             w.publication_date AS publication_date_work,
#             t.publication_date AS publication_date_text, 
#             MIN(w.publication_date) OVER (PARTITION BY cluster_id) AS min_publication_date_work, 
#             MIN(t.publication_date) OVER (PARTITION BY cluster_id, work_id_i) AS min_publication_date_text
#         FROM clustered_defrag_pieces cdp
#         INNER JOIN defrag_pieces dp USING (piece_id)
#         INNER JOIN textreuse_work_mapping twm USING (trs_id)
#         INNER JOIN work_earliest_publication_date w USING (work_id_i)
#         INNER JOIN textreuse_earliest_publication_date t USING (trs_id)
#         )
#         WHERE 
#             publication_date_work=min_publication_date_work AND -- earliest work in cluster
#             publication_date_text=min_publication_date_text -- earliest text in earliest work in cluster
#         """),
#         bucket=processed_bucket
#     )

@asset(
    deps=[clustered_defrag_pieces, 
          "manifestation_publication_date",
          "textreuse_manifestation_mapping", 
          defrag_pieces],
    description="The earliest manifestation and corresponding piece in each cluster",
    group_name="downstream_textreuses"
)
def earliest_manifestation_and_pieces_by_cluster() -> None:
    spark = get_spark_session(
        project_root, application_name="Earliest work and piece in cluster")
    get_s3(spark, "clustered_defrag_pieces", processed_bucket)
    get_s3(spark, "defrag_pieces", processed_bucket)
    get_s3(spark, "manifestation_publication_date", processed_bucket)
    get_s3(spark, "textreuse_manifestation_mapping", processed_bucket)
    materialise_s3(
        spark,
        fname="earliest_manifestation_and_pieces_by_cluster",
        df=spark.sql("""
        SELECT cluster_id,manifestation_id_i,piece_id
        FROM (
        SELECT 
            cluster_id,
            manifestation_id_i,
            piece_id,
            publication_date,
            MIN(publication_date) OVER (PARTITION BY cluster_id) AS min_publication_date
        FROM clustered_defrag_pieces cdp
        INNER JOIN defrag_pieces dp USING (piece_id)
        INNER JOIN textreuse_manifestation_mapping tmm USING (trs_id)
        INNER JOIN manifestation_publication_date mpd USING (manifestation_id_i)
        )
        WHERE 
            publication_date=min_publication_date-- earliest work piece
        """),
        bucket=processed_bucket
    )
