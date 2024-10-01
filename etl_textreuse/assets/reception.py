from etl_textreuse.spark_utils import *
from dagster import asset, Output
from etl_textreuse.assets.downstream_clusters import (
    clustered_defrag_pieces,
    earliest_work_and_pieces_by_cluster,
)
from etl_textreuse.assets.defragmentation import defrag_pieces

# Non Source Pieces
@asset(
    deps=[earliest_work_and_pieces_by_cluster, clustered_defrag_pieces],
    description="The non-source pieces in each cluster",
    group_name="downstream_textreuses",
)
def non_source_pieces() -> Output[None]:
    spark = get_spark_session(application_name="Non Source Pieces")
    get_s3(spark,"earliest_work_and_pieces_by_cluster",processed_bucket)
    get_s3(spark,"clustered_defrag_pieces",processed_bucket)
    df = materialise_s3_if_not_exists(
        spark,
        fname="non_source_pieces",
        df = spark.sql("""
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
        """),
        bucket=processed_bucket
    )
    row_count = df.count()
    return Output(None,metadata={"Row Count":row_count})

# %%
# materialize a downstream table specifically for the reception task


@asset(
    deps=[earliest_work_and_pieces_by_cluster, clustered_defrag_pieces],
    description="Reception Edges",
    group_name="downstream_textreuses",
)
def reception_edges() -> None:
    spark = get_spark_session(project_root, "Reception Edges")
    get_s3(spark, "earliest_work_and_pieces_by_cluster", processed_bucket)
    get_s3(spark, "clustered_defrag_pieces", processed_bucket)
    materialise_s3(
        spark,
        fname="reception_edges",
        df=spark.sql(
            """
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
        """
        ),
        bucket=processed_bucket,
    )


# %%
# Create a denormalized version of the reception edges
# here the pieces are denormalizes to ensure quicker searches


@asset(
    deps=[reception_edges, defrag_pieces],
    description="Denormalised Reception Edges",
    group_name="denormalised",
)
def reception_edges_denorm() -> None:
    spark = get_spark_session(project_root, "Reception Edges Denormalized")
    get_s3(spark, "reception_edges", processed_bucket)
    get_s3(spark, "defrag_pieces", processed_bucket)

    materialise_s3(
        spark,
        fname="reception_edges_denorm",
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
        reception_edges re
        INNER JOIN defrag_pieces dp1 ON re.src_piece_id = dp1.piece_id
        INNER JOIN defrag_pieces dp2 ON re.dst_piece_id = dp2.piece_id
        """
        ),
        bucket=denorm_bucket,
    )


# %%


@asset(
    deps=["earliest_manifestation_and_pieces_by_cluster", clustered_defrag_pieces],
    description="Reception Edges by manifestation publication date",
    group_name="downstream_textreuses",
)
def reception_edges_by_manifestation_date() -> Output[None]:
    spark = get_spark_session(
        project_root, "Reception Edges By manifestation publication date"
    )
    get_s3(spark, "earliest_manifestation_and_pieces_by_cluster", processed_bucket)
    get_s3(spark, "clustered_defrag_pieces", processed_bucket)
    df = materialise_s3(
        spark,
        fname="reception_edges_by_manifestation_date",
        df=spark.sql(
            """
        WITH non_source_pieces AS (
            SELECT cluster_id,piece_id FROM earliest_manifestation_and_pieces_by_cluster epbc 
            RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
            WHERE manifestation_id_i IS NULL -- where it is not the earliest piece
        )
        SELECT ewapbca.piece_id as src_piece_id, nsp.piece_id as dst_piece_id
        FROM earliest_manifestation_and_pieces_by_cluster ewapbca 
        -- only if a cluster has non_source pieces add edges
        --  hence, some clusters which are only source pieces will not have edges
        INNER JOIN non_source_pieces nsp USING(cluster_id)
        """
        ),
        bucket=processed_bucket,
    )
    row_count = df.count()

    return Output(None, metadata={"Row count": row_count})


@asset(
    deps=[reception_edges_by_manifestation_date, defrag_pieces],
    description="Denormalised Reception Edges By Manifestation Date",
    group_name="denormalised",
)
def reception_edges_by_manifestation_date_denorm() -> None:
    spark = get_spark_session(project_root, "Reception Edges Denormalized")
    get_s3(spark, "reception_edges_by_manifestation_date", processed_bucket)
    get_s3(spark, "defrag_pieces", processed_bucket)

    materialise_s3(
        spark,
        fname="reception_edges_by_manifestation_date_denorm",
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
        reception_edges_by_manifestation_date re
        INNER JOIN defrag_pieces dp1 ON re.src_piece_id = dp1.piece_id
        INNER JOIN defrag_pieces dp2 ON re.dst_piece_id = dp2.piece_id
        """
        ),
        bucket=denorm_bucket,
    )


@asset(
    deps=[
        "earliest_book_and_pieces_by_cluster",
        clustered_defrag_pieces,
        "textreuse_ids",
        "defrag_pieces",
        "ecco_core",
        "eebo_core",
    ],
    description="Reception Edges by manifestation publication date",
    group_name="downstream_textreuses",
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
    group_name="denormalised",
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
        bucket=denorm_bucket,
    )