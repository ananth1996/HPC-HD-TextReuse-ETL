# %%
from sklearn.model_selection import ParameterGrid
import functools
from IPython import get_ipython

if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path

from pyspark.sql.functions import col

if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from tqdm.autonotebook import tqdm, trange
from typing import Tuple, Optional
from db_utils import *
from plot_utils import log_binning
import multiprocessing as mp
import threading
from time import perf_counter as time
import csv
# %%
denorm_query = text("""
SELECT re.* FROM reception_edges_denorm re 
INNER JOIN textreuse_ids ti 
ON re.src_trs_id = ti.trs_id WHERE ti.manifestation_id=:doc_id""")

intermediate_query = text("""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id=:doc_id
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
INNER JOIN defrag_pieces dp2 ON nsp.piece_id = dp2.piece_id
""")
# %%
standard_query = text("""
WITH doc_trs_ids AS (
    SELECT trs_id FROM textreuse_ids WHERE manifestation_id=:doc_id
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
INNER JOIN defrag_pieces dp2 ON nsp.piece_id = dp2.piece_id""")

QUERY_TYPE_MAP = {
    "denorm":denorm_query,
    "intermediate": intermediate_query,
    "standard":standard_query
}

engines = {
    "hpc-hd": get_sqlalchemy_engine("hpc-hd"),
    "hpc-hd-columnstore": get_sqlalchemy_engine("hpc-hd-columnstore"),
    "hpc-hd-newspapers": get_sqlalchemy_engine("hpc-hd-newspapers"),
    "hpc-hd-newspapers-columnstore": get_sqlalchemy_engine("hpc-hd-newspapers-columnstore"),
}

# %%


def get_query_dists(statistics: pd.DataFrame, log_bins=True):
    df = statistics
    num_reception_edges = df.num_reception_edges.values
    # quantile = np.quantile(num_reception_edges,[0,0.25,0.5,0.75,1])
    num_buckets = 10
    num_bins = num_buckets + 1
    if log_bins:
        _quantile, _ = log_binning(
            num_reception_edges, num_bins, ret_bins=True)
    else:
        q = np.linspace(0, 1, num_bins)
        _quantile = np.quantile(num_reception_edges, q)
    low = _quantile[:-1]
    high = _quantile[1:]
    query_dist_id = range(num_buckets)
    df = pd.DataFrame(
        {"query_dist_id": query_dist_id, "low": low, "high": high})
    df = df.set_index("query_dist_id")
    return df
# %%


def get_samples(database: str, num_samples=100, seed=42, data_dir: Path = project_root/"data",log_bins:bool=True):
    statistics = pd.read_csv(data_dir/f"{database}-num-reception-edges.csv")
    query_dists = get_query_dists(statistics,log_bins=log_bins)
    if log_bins:
        query_dists.to_csv(data_dir/f"{database}-query-dists-log-bins.csv", index=False)
    else:
        query_dists.to_csv(data_dir/f"{database}-query-dists.csv", index=False)
    num_reception_edges = statistics.num_reception_edges.values
    manifestation_ids = statistics.manifestation_id.values
    num_buckets = len(query_dists)
    _num_samples = int(num_samples/num_buckets)
    rng = np.random.default_rng(seed=seed)
    samples = []
    sample_ground_truths = []
    query_dist_ids = []
    num_queries = len(num_reception_edges)
    for query_dist_id, low, high in query_dists.itertuples():
        dist_mask = (num_reception_edges >= low) & (
            num_reception_edges <= high)
        mask_size = sum(dist_mask)
        sample_ids = rng.choice(a=mask_size, size=_num_samples, replace=False)
        _samples = manifestation_ids[dist_mask][sample_ids]
        _sample_ground_truths = num_reception_edges[dist_mask][sample_ids]
        samples.extend(_samples)
        sample_ground_truths.extend(_sample_ground_truths)
        query_dist_ids.extend([query_dist_id]*_num_samples)
        print(
            f"Bucket {query_dist_id}: Sampling {_num_samples} from {low=:g} and {high=:g} ({mask_size} items, {(mask_size/num_queries)*100:g}%)")

    df = pd.DataFrame({"manifestation_id": samples,
                      "ground_truth": sample_ground_truths, "query_dists_id": query_dist_ids})
    df.to_csv(data_dir/f"{database}-samples.csv", index=False)
    # samples_df = pd.DataFrame.from_dict(samples).melt(var_name="quantile",value_name="doc_id")
    # sample_ground_truths_df = pd.DataFrame.from_dict(sample_ground_truths).melt(var_name="quantile",value_name="ground_truth")
    # samples_df = samples_df.join(sample_ground_truths_df["ground_truth"])
    # print("Maximum Number of rows:",sample_ground_truths_df.ground_truth.max())
    return df
# %%


doc_id_pattern = re.compile("manifestation_id='(.*?)'")

def kill_query(engine,thread_id):
    print(f"Killing trhead {thread_id}")
    with engine.connect() as conn:
        conn.execute(
                    text("kill :thread_id"), parameters={"thread_id": thread_id})

def profile_query(engine, statement, doc_id, ground_truth, database, timeout=None, conn=None):
    try:
        with engine.connect() as conn:
            columnstore_flag = "columnstore" in database 
            columnstore_timeout = timeout is not None and columnstore_flag
            rowstore_timeout = timeout is not None and not columnstore_flag 
            if rowstore_timeout:
                conn.execute(text(f"SET SESSION max_statement_time={timeout}"))

            if not columnstore_flag:
                conn.execute(text("FLUSH NO_WRITE_TO_BINLOG TABLES reception_edges_denorm,non_source_pieces,manifestation_ids,earliest_work_and_pieces_by_cluster,defrag_pieces,clustered_defrag_piecess;"))
            conn.execute(text("RESET QUERY CACHE;"))
        
        
            if columnstore_timeout:
                thread_id = conn.connection.thread_id()
                t = threading.Timer(timeout, lambda: kill_query(engine,thread_id))
                t.start()
            start = time()
            rows= 0
            with conn.execution_options(stream_results=True).execute(
                statement, parameters=dict(doc_id=doc_id)
                ) as result:
                for _ in result:
                    rows+=1
            duration = time() - start
            if columnstore_timeout:
                t.cancel()
            assert rows == ground_truth
    except OperationalError as e:
        # print("Timeout")
        # print(e)
        rows = None
    except IndexError as e:
        # print("ColumnStore Timeout")
        # print(e)
        rows = None
    except AssertionError as e:
        if rows != 0:
            print(f"Assertion Error. Query returns {rows}. Stat says: {ground_truth}")
            print("Query was probably killed before stream was complete")
        else:
            print("Query never started")
        rows = None
    if rows is None:
        duration = None
    return {"doc_id": doc_id, "duration": duration}

# %%


def extended_query_profile(query_type: str, sample: Tuple[str, int], database, timeout: Optional[float] = None):
    engine = engines[database]
    query = QUERY_TYPE_MAP[query_type]
    manifestation_id, ground_truth = sample
    result = profile_query(engine, query, manifestation_id,
                           ground_truth, database, timeout=timeout)
    result.update({
        "query_type": query_type,
        "database": database
    }
    )
    return result


def wrap_query_profile(kwargs):
    return extended_query_profile(**kwargs)


def profile(timeout: Optional[float] = None):
    param_grid = [
        {
            "query_type": list(QUERY_TYPE_MAP.keys()),
            "sample": list(get_samples("hpc-hd")[["manifestation_id", "ground_truth"]].itertuples(index=False, name=None)),
            "database":["hpc-hd-columnstore"],
            "timeout":[timeout]
        },
        {
            "query_type":list(QUERY_TYPE_MAP.keys()),
            "sample":list(get_samples("hpc-hd-newspapers")[["manifestation_id","ground_truth"]].itertuples(index=False,name=None)),
            "database":["hpc-hd-newspapers-columnstore"],
            "timeout":[timeout]
        }
    ]
    grid = list(ParameterGrid(param_grid))

    # with mp.Pool(processes=1) as pool:
    #     rows = []
    #     for result in tqdm(pool.imap_unordered(wrap_query_profile,grid), total=len(grid)):
    #         rows.append(result)
    rows = []
    with open(project_root/"data"/"reception-queries-results-2.csv", 'a+') as csvfile:
        fieldnames = ["doc_id","duration","query_type","database"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # go to beginning of file
        csvfile.seek(0)
        num_lines = len(csvfile.readlines())
        if num_lines>1:
            num_entries = num_lines-1
            grid = grid[num_entries:]
        else:
            writer.writeheader()
        # go to end of file
        csvfile.seek(0,2)
        for params in tqdm(grid, total=len(grid)):
            result = wrap_query_profile(params)
            writer.writerow(result)
            csvfile.flush()
        # rows.append(result)
    # return pd.DataFrame(rows)
# %%


if __name__ == "__main__":
    df = profile(300)
    # df.to_csv(project_root/"data"/"reception-queries-results-2.csv",index=False)
    # %%
    # timeout = 10
    # param_grid = [
    #     {
    #         "query_type": list(QUERY_TYPE_MAP.keys()),
    #         "sample": list(get_samples("hpc-hd")[["manifestation_id", "ground_truth"]].itertuples(index=False, name=None)),
    #         "database":["hpc-hd-columnstore"],
    #         "timeout":[timeout],
    #     },
    #     # {
    #     #     "query_type":list(QUERY_TYPE_MAP.keys()),
    #     #     "sample":list(get_samples("hpc-hd-newspapers")[["manifestation_id","ground_truth"]].itertuples(index=False,name=None)),
    #     #     "database":["hpc-hd-newspapers","hpc-hd-newspapers-columnstore"]
    #     # }
    # ]
    # grid = ParameterGrid(param_grid)
    # rows = []
    # for params in tqdm(grid, total=len(grid)):
    #     print(params["sample"])
    #     result = wrap_query_profile(params)
    #     print(result["duration"])
    #     rows.append(result)
    # df = pd.DataFrame(rows)
    # %%
