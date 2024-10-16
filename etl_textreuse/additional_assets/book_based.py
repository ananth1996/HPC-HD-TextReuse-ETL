from etl_textreuse.spark_utils import *
from dagster import asset, Output
from etl_textreuse.assets.downstream_clusters import (
    clustered_defrag_pieces,
)
from etl_textreuse.assets.defragmentation import defrag_pieces


@asset(
    deps=[clustered_defrag_pieces, 
          "manifestation_publication_date",
          "textreuse_manifestation_mapping", 
          "ecco_core",
          "eebo_core",
          "manifestation_ids",
          defrag_pieces],
    description="The earliest manifestation that is also a book (ECCO or EEBO-TCP) and corresponding piece in each cluster",
    group_name="additional_assets_for_ville"
)
def earliest_book_and_pieces_by_cluster() -> Output[None]:
    spark = get_spark_session(
        project_root, application_name="Earliest work and piece in cluster")
    get_s3(spark, "clustered_defrag_pieces", processed_bucket)
    get_s3(spark, "defrag_pieces", processed_bucket)
    get_s3(spark, "manifestation_publication_date", processed_bucket)
    get_s3(spark, "textreuse_manifestation_mapping", processed_bucket)
    get_s3(spark, "manifestation_ids", processed_bucket)
    get_s3(spark, "ecco_core", raw_bucket)
    get_s3(spark, "eebo_core", raw_bucket)
    df = materialise_s3(
        spark,
        fname="earliest_book_and_pieces_by_cluster",
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
        INNER JOIN manifestation_ids mi USING (manifestation_id_i)
        LEFT JOIN ecco_core ec ON ec.ecco_id = mi.manifestation_id
        LEFT JOIN eebo_core eb ON eb.eebo_tcp_id = mi.manifestation_id
        INNER JOIN manifestation_publication_date mpd USING (manifestation_id_i)
        -- only consider pieces from ECCO and EEBO-TCP
        WHERE NOT (ec.ecco_id IS NULL AND eb.eebo_tcp_id IS NULL)
        )
        WHERE 
            publication_date=min_publication_date-- earliest work piece
        """),
        bucket=processed_bucket
    )   
    row_count = df.count()
    return Output(None,metadata={"Row Count":row_count})

@asset(
    deps=[
        earliest_book_and_pieces_by_cluster,
        clustered_defrag_pieces,
        "textreuse_ids",
        "defrag_pieces",
        "ecco_core",
        "eebo_core",
    ],
    description="Reception Edges by manifestation publication date",
    group_name="additional_assets_for_ville",
)
def reception_edges_between_books() -> Output[None]:
    spark = get_spark_session(application_name="Reception Edges Between Books")
    tables = [
        ("earliest_book_and_pieces_by_cluster", processed_bucket),
        ("clustered_defrag_pieces", processed_bucket),
        ("textreuse_ids", processed_bucket),
        ("defrag_pieces", processed_bucket),
        ("ecco_core", raw_bucket),
        ("eebo_core", raw_bucket),
    ]
    for table, bucket in tables:
        get_s3(spark, table, bucket)
    df = materialise_s3(
        spark=spark,
        fname="reception_edges_between_books",
        df=spark.sql(
            """
        WITH dest_book_pieces AS (
        SELECT /*+ BROADCAST(ti,ec,eb) */
        cdp.* FROM clustered_defrag_pieces cdp
        LEFT JOIN earliest_book_and_pieces_by_cluster ebapbc USING(cluster_id,piece_id)
        INNER JOIN defrag_pieces dp ON cdp.piece_id = dp.piece_id
        INNER JOIN textreuse_ids ti ON dp.trs_id = ti.trs_id
        LEFT JOIN ecco_core ec ON ec.ecco_id = ti.manifestation_id
        LEFT JOIN eebo_core eb ON eb.eebo_tcp_id = ti.manifestation_id
        WHERE
        -- where it is not earliest    
        ebapbc.manifestation_id_i IS NULL AND 
        -- only consider pieces from ECCO and EEBO-TCP
        NOT (ec.ecco_id IS NULL AND eb.eebo_tcp_id IS NULL)
        )
        SELECT 
        ebapbc.piece_id AS src_piece_id,
        dstp.piece_id AS dst_piece_id
        FROM earliest_book_and_pieces_by_cluster ebapbc
        INNER JOIN dest_book_pieces dstp USING(cluster_id) 
        """
        ),
        bucket=processed_bucket,
    )
    row_count = df.count()
    return Output(None, metadata={"Row Count": row_count})


@asset(
    deps=[reception_edges_between_books, defrag_pieces],
    description="Denormalised Reception Edges Between books",
    group_name="additional_assets_for_ville",
)
def reception_edges_between_books_denorm() -> None:
    spark = get_spark_session(project_root, "Reception Edges Between Books Denormalized")
    get_s3(spark, "reception_edges_between_books", processed_bucket)
    get_s3(spark, "defrag_pieces", processed_bucket)

    materialise_s3(
        spark,
        fname="reception_edges_between_books_denorm",
        df=spark.sql(
            """
        SELECT 
            dp1.trs_id AS src_trs_id,
            dp1.trs_start AS src_trs_start,
            dp1.trs_end AS src_trs_end,
            dp2.trs_id AS dst_trs_id,
            dp2.trs_start AS dst_trs_start,
            dp2.trs_end AS dst_trs_end
        FROM 
        reception_edges_between_books re
        INNER JOIN defrag_pieces dp1 ON re.src_piece_id = dp1.piece_id
        INNER JOIN defrag_pieces dp2 ON re.dst_piece_id = dp2.piece_id
        """
        ),
        bucket=processed_bucket,
    )

@asset(
    deps=[reception_edges_between_books_denorm, "textreuse_source_lengths"],
    description="Coverage network from reception/inception only between books",
    group_name="downstream_textreuses"
)
def reception_inception_between_book_coverages() -> None:
    spark = get_spark_session(application_name="Book Reception Coverages")
    get_s3(spark,"textreuse_source_lengths",processed_bucket)
    get_s3(spark,"reception_edges_between_books_denorm",processed_bucket)
    materialise_s3(
        spark,
        fname="reception_inception_between_book_coverages", 
        df= spark.sql("""
    WITH groups AS 
    (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY src_trs_id,dst_trs_id ORDER BY src_trs_start,src_trs_end) AS t1_RN,
            ROW_NUMBER() OVER(PARTITION BY src_trs_id,dst_trs_id ORDER BY dst_trs_start,dst_trs_end) AS t2_RN,
            src_trs_id AS trs1_id,
            dst_trs_id AS trs2_id,
            src_trs_start AS trs1_start,
            src_trs_end AS trs1_end,
            dst_trs_start AS trs2_start,
            dst_trs_end AS trs2_end,
            MAX(src_trs_end) -- largest end date 
                OVER 
                (
                    PARTITION BY src_trs_id,dst_trs_id -- group by t1 and t2
                    ORDER BY src_trs_start,src_trs_end  -- order by times
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- FROM all rows before till 1 row before 
                ) as t1_previous_end,
            MAX(dst_trs_end) -- largest end date 
                OVER 
                (
                    PARTITION BY src_trs_id,dst_trs_id -- group by t1 and t2
                    ORDER BY dst_trs_start,dst_trs_end  -- order by times
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- FROM all rows before till 1 row before 
                ) as t2_previous_end
            
        FROM reception_edges_between_books_denorm red
    ), 
    islands AS
    (
        SELECT 
            *,
            -- debug for checking the t1 islands
            -- CASE WHEN t1_previous_end +1 >= trs1_start THEN 0 ELSE 1 END AS t1_island_start,
            -- previous_end+1 to catch ranges such as (1,6) and (7,12) to become (1,12)
            SUM(CASE WHEN t1_previous_end +1  >= trs1_start THEN 0 ELSE 1 END) OVER (PARTITION BY trs1_id,trs2_id ORDER BY t1_RN) as t1_island_id,
            -- t2 overalps 
            -- debug for checking the islands: 
            -- CASE WHEN t2_previous_end >= trs2_start THEN 0 ELSE 1 END AS t2_island_start,
            SUM(CASE WHEN t2_previous_end+1 >= trs2_start THEN 0 ELSE 1 END) OVER (PARTITION BY trs1_id,trs2_id ORDER BY t2_RN) as t2_island_id
        FROM groups
    ), 
    t1_merged_overlaps as 
    (
        SELECT
            trs1_id,
            trs2_id,
            MIN(trs1_start) as trs1_start_pos,
            MAX(trs1_end) as trs1_end_pos,
            MAX(trs1_end) - MIN(trs1_start) as trs1_overlap_length
        FROM islands
        GROUP BY 
            trs1_id,
            trs2_id,
            t1_island_id
    ),
    t1_final as 
    (
    SELECT 
        trs1_id,
        trs2_id,
        SUM(trs1_overlap_length) as trs1_correct_overlap,
        COUNT(*) as t1_num_merged_hits
    FROM t1_merged_overlaps
    GROUP BY 
        trs1_id,
        trs2_id
    ),
    t2_merged_overlaps AS 
    (
        SELECT
            trs1_id,
            trs2_id,
            MIN(trs2_start) as trs2_start_pos,
            MAX(trs2_end) as trs2_end_pos,
            MAX(trs2_end) - MIN(trs2_start) as trs2_overlap_length
        FROM islands
        GROUP BY 
            trs1_id,
            trs2_id,
            t2_island_id
    ),
    t2_final AS 
    (
    SELECT 
        trs1_id,
        trs2_id,
        SUM(trs2_overlap_length) as t2_correct_overlap,
        COUNT(*) as t2_num_merged_hits
    FROM t2_merged_overlaps
    GROUP BY 
        trs1_id,
        trs2_id
    ),
    reuses AS
    (
        SELECT 
            t1.trs1_id,
            t1.t1_num_merged_hits as t1_reuses,
            t1.trs1_correct_overlap as reuse_t1_t2,
            t1.trs2_id,
            t2.t2_num_merged_hits as t2_reuses,
            t2.t2_correct_overlap as reuse_t2_t1		
        FROM t1_final t1
        LEFT JOIN t2_final t2 
        ON 
            t1.trs1_id = t2.trs1_id AND
            t1.trs2_id = t2.trs2_id 
    )
    SELECT 
        /*+ BROADCAST(l1) BROADCAST(l2) */
        r.trs1_id AS src_trs_id,
        r.t1_reuses AS num_reuses_src , 
        r.reuse_t1_t2 reuses_src_in_dst,
        l1.text_length as src_length, 
        (r.reuse_t1_t2/l1.text_length)*100 AS coverage_src_in_dst,
        r.trs2_id AS dst_trs_id,
        r.t2_reuses AS num_reuses_dst, 
        r.reuse_t2_t1 AS reuses_dst_in_src, 
        l2.text_length AS dst_length, 
        (r.reuse_t2_t1/l2.text_length)*100 as coverage_dst_in_src
    FROM 
        reuses r
        LEFT JOIN textreuse_source_lengths l1 ON l1.trs_id = r.trs1_id
        LEFT JOIN textreuse_source_lengths l2 ON l2.trs_id = r.trs2_id
    """),
    bucket=processed_bucket
    )