#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from spark_utils_alternate import *
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.resolve()
from time import perf_counter as time
import numpy as np
import pandas as pd
from tqdm.autonotebook import trange, tqdm
from mariadb_query_analysis import get_samples
from sklearn.model_selection import ParameterGrid
import threading
from py4j.protocol import Py4JJavaError
import csv
# %%
denorm_query="""
SELECT re.* FROM reception_edges_denorm re 
INNER JOIN textreuse_ids ti 
ON re.src_trs_id = ti.trs_id WHERE ti.manifestation_id={doc_id!r}
"""
standard_query="""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id={doc_id!r}
), 
non_source_pieces_tmp AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
),
doc_pieces AS (
    SELECT * FROM defrag_pieces 
    INNER JOIN doc_trs_ids USING(trs_id)
),
doc_clusters_and_src_pieces AS (
    SELECT 
        cluster_id,
        dp.*
    FROM earliest_work_and_pieces_by_cluster
    INNER JOIN doc_pieces dp USING(piece_id)
)
SELECT  
    dp1.trs_id AS src_trs_id,
    dp1.trs_start AS src_trs_start,
    dp1.trs_end AS src_trs_end,
    dp2.trs_id AS dst_trs_id,
    dp2.trs_start AS dst_trs_start,
    dp2.trs_end AS dst_trs_end
FROM 
doc_clusters_and_src_pieces dp1
INNER JOIN non_source_pieces_tmp nsp USING(cluster_id) 
INNER JOIN defrag_pieces dp2 ON nsp.piece_id = dp2.piece_id
"""
intermediate_query="""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id={doc_id!r}
), 
doc_pieces AS (
    SELECT * FROM defrag_pieces 
    INNER JOIN doc_trs_ids USING(trs_id)
),
doc_clusters_and_src_pieces AS (
    SELECT 
        cluster_id,
        dp.*
    FROM earliest_work_and_pieces_by_cluster
    INNER JOIN doc_pieces dp USING(piece_id)
)
SELECT  
    dp1.trs_id AS src_trs_id,
    dp1.trs_start AS src_trs_start,
    dp1.trs_end AS src_trs_end,
    dp2.trs_id AS dst_trs_id,
    dp2.trs_start AS dst_trs_start,
    dp2.trs_end AS dst_trs_end
FROM 
doc_clusters_and_src_pieces dp1
INNER JOIN non_source_pieces nsp USING(cluster_id) 
INNER JOIN defrag_pieces dp2 ON nsp.piece_id = dp2.piece_id"""


QUERY_TYPE_MAP = {
    "denorm":denorm_query,
    "intermediate": intermediate_query,
    "standard":standard_query
}

QUERY_TABLES_MAP = {
    "reception": {
        "denorm": [("reception_edges_denorm", "denorm_bucket"), ("textreuse_ids", "processed_bucket")],
        "intermediate": [("textreuse_ids", "processed_bucket"), ("defrag_pieces", "processed_bucket"), ("earliest_work_and_pieces_by_cluster", "processed_bucket"), ("non_source_pieces", "processed_bucket")],
        "standard":  [("textreuse_ids", "processed_bucket"), ("earliest_work_and_pieces_by_cluster", "processed_bucket"), ("clustered_defrag_pieces", "processed_bucket"), ("defrag_pieces", "processed_bucket")]
    },
    "quote": {
        "denorm": [("source_piece_statistics_denorm", "denorm_bucket"), ("edition_ids", "processed_bucket")],
        "intermediate": [("textreuse_edition_mapping", "processed_bucket"), ("defrag_pieces", "processed_bucket"), ("earliest_work_and_pieces_by_cluster", "processed_bucket"), ("edition_authors", "processed_bucket"), ("edition_ids", "processed_bucket"), "non_source_pieces", "textreuse_work_mapping"],
        "standard": [("textreuse_edition_mapping", "processed_bucket"), ("defrag_pieces", "processed_bucket"), ("earliest_work_and_pieces_by_cluster", "processed_bucket"), ("edition_authors", "processed_bucket"), ("edition_ids", "processed_bucket"), "textreuse_work_mapping"]
    }
}
# %%


def stop_query(spark):
    spark.sparkContext.cancelAllJobs()
    raise Py4JJavaError("TimeOut")

def time_query(spark, query_statement: str, doc_id: str, ground_truth: int, timeout=None):
    error = None
    try:
        if timeout is not None:
            spark_timer = threading.Timer(timeout,lambda : stop_query(spark))
        spark_timer.start()
        start = time()
        df = spark.sql(query_statement.format(doc_id=doc_id))
        df = df.cache()
        df.count()
        iterator = df.toLocalIterator(prefetchPartitions = True)
        rows = sum(1 for _ in iterator)
        duration = time() - start
        if timeout is not None:
            spark_timer.cancel()
        df.unpersist()
        assert rows == ground_truth
    except AssertionError as e:
        error = (f"Assertion Error. {rows=} and {ground_truth=}")
        # duration = None   
    except Py4JJavaError as e:
        error = "Py4JJavaError. Timeout"
        duration = None
    return {"doc_id": doc_id, "duration": duration, "error": error}
# %%



def load_parquets(spark,query_type,dataset,case="reception"):
    # load the tables
    buckets = BUCKETS_MAP[dataset]
    for table, bucket in QUERY_TABLES_MAP[case][query_type]:
        get_s3(spark, table, buckets[bucket])

def profile_sample(query_type: str, sample: Tuple[str, int], dataset:str, case: str,timeout: Optional[float] = None):
    spark = get_spark_session(application_name="query_profiling")
    load_parquets(spark,query_type,dataset,case)
    query_statement = QUERY_TYPE_MAP[query_type]
    variable, ground_truth = sample
    result = time_query(spark, query_statement, variable,
                           ground_truth, timeout=timeout)
    result.update({
        "query_type": query_type,
        "database": dataset+"-spark"
    }
    )
    return result

def wrap_query_profile(kwargs):
    return profile_sample(**kwargs)


def profile(timeout: Optional[float] = None):
    param_grid = [
        {
            "query_type": list(QUERY_TYPE_MAP.keys()),
            "sample": list(get_samples("hpc-hd")[["manifestation_id", "ground_truth"]].itertuples(index=False, name=None)),
            "dataset":["hpc-hd"],
            "case":["reception"],
            "timeout":[timeout],
        },
        {
            "query_type": list(QUERY_TYPE_MAP.keys()),
            "sample": list(get_samples("hpc-hd-newspapers")[["manifestation_id", "ground_truth"]].itertuples(index=False, name=None)),
            "dataset":["hpc-hd-newspapers"],
            "case":["reception"],
            "timeout":[timeout],
        }
    ]
    grid = list(ParameterGrid(param_grid))

    rows = []
    with open(project_root/"data"/"reception-queries-results-3.csv", 'a+') as csvfile:
        fieldnames = ["doc_id","duration","query_type","database","error"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # go to beginning of file
        csvfile.seek(0)
        num_lines = len(csvfile.readlines())
        if num_lines>1:
            num_entries = num_lines-1
            grid = grid[num_entries:]
        elif num_lines==0:
            writer.writeheader()
        # go to end of file
        csvfile.seek(0,2)
        for params in tqdm(grid, total=len(grid)):
            result = wrap_query_profile(params)
            writer.writerow(result)
            csvfile.flush()
        # rows.append(result)
    # return pd.DataFrame(rows)

#%%
if __name__ == "__main__":
    profile(300)
    #%%
    # timeout = 90
    # param_grid = [
    #     {
    #         "query_type": list(QUERY_TYPE_MAP.keys()),
    #         "sample": list(get_samples("hpc-hd")[["manifestation_id", "ground_truth"]].itertuples(index=False, name=None)),
    #         "dataset":["hpc-hd"],
    #         "case":["reception"],
    #         "timeout":[timeout],
    #     },
    #     {
    #         "query_type": list(QUERY_TYPE_MAP.keys()),
    #         "sample": list(get_samples("hpc-hd-newspapers")[["manifestation_id", "ground_truth"]].itertuples(index=False, name=None)),
    #         "dataset":["hpc-hd-newspapers"],
    #         "case":["reception"],
    #         "timeout":[timeout],
    #     }
    # ]
    # grid = list(ParameterGrid(param_grid))
    # #%%
    # print(wrap_query_profile(grid[394]))
    # print(wrap_query_profile(grid[2]))
# %%
