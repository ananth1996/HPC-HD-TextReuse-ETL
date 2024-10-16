from etl_textreuse.spark_utils import *
from dagster import asset, Output
from etl_textreuse.assets.downstream_clusters import (
    clustered_defrag_pieces,earliest_manifestation_and_pieces_by_cluster
)
from etl_textreuse.assets.defragmentation import defrag_pieces

# Non Source Pieces
@asset(
    deps=[earliest_manifestation_and_pieces_by_cluster, clustered_defrag_pieces],
    description="The non-source pieces in each cluster",
    group_name="downstream_textreuses",
)
def non_source_pieces() -> Output[None]:
    spark = get_spark_session(application_name="Non Source Pieces")
    get_s3(spark,"earliest_manifestation_and_pieces_by_cluster",processed_bucket)
    get_s3(spark,"clustered_defrag_pieces",processed_bucket)
    df = materialise_s3_if_not_exists(
        spark,
        fname="non_source_pieces",
        df = spark.sql("""
        SELECT cluster_id,piece_id FROM earliest_manifestation_and_pieces_by_cluster 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE manifestation_id_i IS NULL -- where it is not the earliest piece
        """),
        bucket=processed_bucket
    )
    row_count = df.count()
    return Output(None,metadata={"Row Count":row_count})

# %%
# materialize a downstream table specifically for the reception task


@asset(
    deps=["earliest_manifestation_and_pieces_by_cluster", clustered_defrag_pieces],
    description="Reception Edges by earliest published manifestation in each cluster",
    group_name="downstream_textreuses",
)
def reception_edges() -> Output[None]:
    spark = get_spark_session(
        project_root, "Reception Edges By manifestation publication date"
    )
    get_s3(spark, "earliest_manifestation_and_pieces_by_cluster", processed_bucket)
    get_s3(spark, "clustered_defrag_pieces", processed_bucket)
    df = materialise_s3(
        spark,
        fname="reception_edges",
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

# %%
# Create a denormalized version of the reception edges
# here the pieces are denormalized to ensure quicker searches

@asset(
    deps=[reception_edges, defrag_pieces],
    description="Denormalised Reception Edges By Manifestation Date",
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
        bucket=processed_bucket,
    )