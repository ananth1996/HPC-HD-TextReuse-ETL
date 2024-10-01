from dagster import asset
from etl_textreuse.spark_utils import *

# %%


@asset(
    deps=["reception_edges", "defrag_pieces", "textreuse_edition_mapping",
          "textreuse_work_mapping", "edition_authors", "clustered_defrag_pieces"],
    description="Statistics for each source piece",
    group_name="downstream_textreuses"
)
def source_piece_statistics() -> None:
    spark = get_spark_session(project_root, "Source Piece Statistics")
    get_s3(spark, "reception_edges", processed_bucket)
    get_s3(spark, "defrag_pieces", processed_bucket)
    get_s3(spark, "textreuse_edition_mapping", processed_bucket)
    get_s3(spark, "textreuse_work_mapping", processed_bucket)
    get_s3(spark, "edition_authors", processed_bucket)
    get_s3(spark, "earliest_work_and_pieces_by_cluster", processed_bucket)
    get_s3(spark, "clustered_defrag_pieces", processed_bucket)
    materialise_s3(
        spark,
        fname="source_piece_statistics",
        df=spark.sql("""
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

@asset(
        deps=[source_piece_statistics,"defrag_pieces","textreuse_edition_mapping"],
        description="Denormalised Source Piece Statistics Table",
        group_name="denormalised"
)
def source_piece_statistics_denorm() -> None:
    spark = get_spark_session(project_root, "Source Piece Statistics")
    get_s3(spark, "source_piece_statistics", processed_bucket)
    get_s3(spark, "defrag_pieces", processed_bucket)
    get_s3(spark, "textreuse_edition_mapping", processed_bucket)
    materialise_s3(
        spark,
        fname= "source_piece_statistics_denorm",
        df=spark.sql("""
            SELECT * FROM 
            source_piece_statistics
            INNER JOIN defrag_pieces USING(piece_id)
            INNER JOIN textreuse_edition_mapping USING(trs_id)
        """),
        bucket=denorm_bucket
    )