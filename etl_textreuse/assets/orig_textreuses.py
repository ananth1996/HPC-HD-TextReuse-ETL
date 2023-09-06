from .raw_textreuses import textreuses
from ..spark_utils import *
from dagster import asset
import logging 

logger = logging.getLogger(__name__)

@asset(deps=[textreuses],description="orginal pieces of textreuse")
def orig_pieces() -> None:
    spark = get_spark_session(project_root,application_name="original pieces")
    get_s3(spark,"textreuses",processed_bucket)
    materialise_row_numbers(
        spark,
        fname="orig_pieces",
        df=spark.sql("""
        SELECT * FROM (
        SELECT  
            trs1_id AS trs_id,
            trs1_start AS trs_start,
            trs1_end AS trs_end 
        FROM textreuses
        UNION
        SELECT
            trs2_id AS trs_id,
            trs2_start AS trs_start,
            trs2_end AS trs_end 
        FROM textreuses
        )
        ORDER BY trs_id,trs_start,trs_end
        """),
        col_name="piece_id",
        bucket=processed_bucket
    )
@asset(deps=[textreuses,orig_pieces],description="The original textreuses between pieces")
def orig_textreuses() -> None:
    spark = get_spark_session(project_root,application_name="original pieces")
    get_s3(spark,"textreuses",processed_bucket)
    get_s3(spark,"orig_pieces",processed_bucket)
    materialise_s3(
        spark,
        fname="orig_textreuses",
        df=spark.sql("""
        SELECT 
            t.textreuse_id, 
            op1.piece_id AS piece1_id, 
            op2.piece_id AS piece2_id,
            t.align_length,
            t.positives_percent 
        FROM textreuses t
        LEFT JOIN orig_pieces op1 ON t.trs1_id = op1.trs_id AND t.trs1_start=op1.trs_start AND t.trs1_end=op1.trs_end
        LEFT JOIN orig_pieces op2 ON t.trs2_id = op2.trs_id AND t.trs2_start=op2.trs_start AND t.trs2_end=op2.trs_end
        """),
        bucket=processed_bucket
    )
