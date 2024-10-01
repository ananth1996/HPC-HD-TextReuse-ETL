# %%
from etl_textreuse.assets.orig_textreuses import orig_pieces, orig_textreuses
from etl_textreuse.spark_utils import *
from dagster import asset
import subprocess
# %%


@asset(
    deps=[orig_pieces],
    description="Which defragmented piece each orignal piece goes to",
    group_name="textreuses"
)
def piece_id_mappings() -> None:
    process = subprocess.run(["jupyter", "nbconvert", "--to", "notebook",
                              "--inplace", "--execute", str(project_root/"etl_textreuse"/"assets"/"piece_id_mappings.ipynb")], capture_output=True)
    if process.returncode != 0:
        raise ChildProcessError(f"Error Running Notebook {process.stdout} {process.stderr}")
    else:
        print(process.stderr)
        print(process.stdout)


@asset(
    deps=[orig_pieces, piece_id_mappings],
    description="The pieces after defragmentation",
    group_name="textreuses"
)
def defrag_pieces() -> None:
    spark = get_spark_session(
        project_root, application_name="degragmented pieces")
    get_s3(spark, "orig_pieces", processed_bucket)
    get_s3(spark, "piece_id_mappings", processed_bucket)
    materialise_s3(
        spark,
        fname="defrag_pieces",
        df=spark.sql("""
        SELECT defrag_piece_id AS piece_id, trs_id, MIN(trs_start) AS trs_start, MAX(trs_end) AS trs_end
        FROM piece_id_mappings
        INNER JOIN orig_pieces ON piece_id = orig_piece_id
        GROUP BY defrag_piece_id, trs_id
        """),
        bucket=processed_bucket
    )


@asset(
    deps=[orig_textreuses, piece_id_mappings],
    description="The textreuses between defragmented pieces",
    group_name="textreuses"
)
def defrag_textreuses() -> None:
    spark = get_spark_session(
        project_root, application_name="degragmented textreuses")
    get_s3(spark, "orig_textreuses", processed_bucket)
    get_s3(spark, "piece_id_mappings", processed_bucket)
    materialise_row_numbers(
        spark,
        fname="defrag_textreuses",
        df=spark.sql("""
        SELECT
            pm1.defrag_piece_id AS piece1_id, 
            pm2.defrag_piece_id AS piece2_id,
            COUNT(*) AS num_orig_links
        FROM orig_textreuses tr
        LEFT JOIN piece_id_mappings pm1 ON (tr.piece1_id=pm1.orig_piece_id)
        LEFT JOIN piece_id_mappings pm2 ON (tr.piece2_id=pm2.orig_piece_id)
        GROUP BY pm1.defrag_piece_id,pm2.defrag_piece_id
        ORDER BY piece1_id,piece2_id
        """),
        col_name="textreuse_id",
        bucket=processed_bucket
    )
