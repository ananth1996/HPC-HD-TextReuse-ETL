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
# %%
standard_query =text(f"""
WITH filtered_clusters AS (
    SELECT cluster_id, dp.*  FROM 
    edition_ids
    INNER JOIN textreuse_edition_mapping USING(edition_id_i)
    INNER JOIN defrag_pieces dp USING(trs_id)
    INNER JOIN earliest_work_and_pieces_by_cluster USING(piece_id)
    WHERE edition_id = :estc_id AND dp.trs_end - dp.trs_start BETWEEN 150 AND 300
),non_source_pieces_tmp AS (
        SELECT cluster_id,piece_id FROM earliest_work_and_pieces_by_cluster ewapbca2 
        RIGHT JOIN clustered_defrag_pieces cdp USING(cluster_id,piece_id) 
        WHERE work_id_i IS NULL -- where it is not the earliest piece
),query_authors AS (
	SELECT DISTINCT actor_id_i 
	FROM edition_authors 
	INNER JOIN edition_ids USING(edition_id_i)
	WHERE edition_id = :estc_id AND actor_id_i IS NOT NULL
), cluster_stats AS (
	SELECT 
	    cluster_id,
	    -- number of different work ids not 
	    --  from the same author as the query
	    COUNT(DISTINCT work_id_i) as n_works
	FROM filtered_clusters
	INNER JOIN non_source_pieces_tmp nsp USING (cluster_id)
	INNER JOIN defrag_pieces dp ON nsp.piece_id = dp.piece_id
	INNER JOIN textreuse_edition_mapping tem ON tem.trs_id = dp.trs_id
	INNER JOIN textreuse_work_mapping twm ON twm.trs_id = dp.trs_id
	INNER JOIN edition_authors USING(edition_id_i)
	-- left exclusive join
	LEFT JOIN query_authors qa USING(actor_id_i)
	-- only authors who are not the same as query authors
	WHERE qa.actor_id_i IS NULL 
	GROUP  BY cluster_id
),results AS (
	SELECT 
        *,
        trs_end-trs_start AS piece_length
    FROM cluster_stats
	INNER JOIN filtered_clusters fc USING (cluster_id)
)
SELECT * FROM results
ORDER BY n_works DESC,piece_length DESC
LIMIT 100 
""")
#%%
intermediate_query=text(f"""
WITH filtered_clusters AS (
    SELECT cluster_id, dp.*  FROM 
    edition_ids
    INNER JOIN textreuse_edition_mapping USING(edition_id_i)
    INNER JOIN defrag_pieces dp USING(trs_id)
    INNER JOIN earliest_work_and_pieces_by_cluster USING(piece_id)
    WHERE edition_id = :estc_id AND dp.trs_end - dp.trs_start BETWEEN 150 AND 300
),query_authors AS (
	SELECT DISTINCT actor_id_i 
	FROM edition_authors 
	INNER JOIN edition_ids USING(edition_id_i)
	WHERE edition_id = :estc_id AND actor_id_i IS NOT NULL
), cluster_stats AS (
	SELECT 
	    cluster_id,
	    -- number of different work ids not 
	    --  from the same author as the query
	    COUNT(DISTINCT work_id_i) as n_works
	FROM filtered_clusters
	INNER JOIN non_source_pieces nsp USING (cluster_id)
	INNER JOIN defrag_pieces dp ON nsp.piece_id = dp.piece_id
	INNER JOIN textreuse_edition_mapping tem ON tem.trs_id = dp.trs_id
	INNER JOIN textreuse_work_mapping twm ON twm.trs_id = dp.trs_id
	INNER JOIN edition_authors USING(edition_id_i)
	-- left exclusive join
	LEFT JOIN query_authors qa USING(actor_id_i)
	-- only authors who are not the same as query authors
	WHERE qa.actor_id_i IS NULL 
	GROUP  BY cluster_id
),results AS (
	SELECT 
        *,
        trs_end-trs_start AS piece_length
    FROM cluster_stats
	INNER JOIN filtered_clusters fc USING (cluster_id)
)
SELECT * FROM results
ORDER BY n_works DESC,piece_length DESC
LIMIT 100                   
""")
# %%
denorm_query = text(f"""
SELECT cluster_id,num_work_ids_different_authors as n_works, piece_id,trs_id,trs_start,trs_end,piece_length FROM 
source_piece_statistics_denorm
INNER JOIN edition_ids USING(edition_id_i)
WHERE edition_id = :estc_id 
AND piece_length BETWEEN 150 AND 300
ORDER BY n_works DESC, piece_length DESC
LIMIT 100
""")
# %%

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

def get_statistics(dataset,threshold=100,data_dir=project_root/"data",replace:bool=False):
    database= dataset + "-columnstore"
    stat_file=data_dir/f"{dataset}-quote-query-statistics.csv"
    if  not stat_file.exists() or replace:
        with engines[database].connect() as conn:
            statistics = pd.read_sql(text("""
                WITH non_null_edition_authors AS (
                SELECT DISTINCT edition_id_i FROM edition_authors
                WHERE actor_id_i IS NOT NULL
                ), filtered_rows AS (
                SELECT * FROM source_piece_statistics_denorm spsd 
                INNER JOIN non_null_edition_authors USING(edition_id_i)
                INNER JOIN edition_ids USING(edition_id_i)
                WHERE piece_length BETWEEN 150 AND 300 -- quote length
                ), windows AS (
                    SELECT edition_id,num_work_ids_different_authors as n_works, ROW_NUMBER() OVER( PARTITION BY edition_id ORDER BY num_work_ids_different_authors DESC ) AS rn
                    FROM filtered_rows 
                ), ground_truths AS (
                    SELECT edition_id, SUM(n_works) as ground_truth
                    FROM windows 
                    WHERE rn <=100 -- top 100 quotes
                    GROUP BY edition_id
                ), stats AS (
                SELECT edition_id,AVG(num_work_ids_different_authors) AS avg_n_works, STDDEV_POP(num_work_ids_different_authors) AS std_n_works, COUNT(*) AS num_quotes, SUM(num_work_ids_different_authors) as sum_n_works
                FROM filtered_rows
                GROUP BY edition_id
                )
                SELECT * 
                FROM ground_truths 
                INNER JOIN stats USING(edition_id)
                """),
                con = conn
                )
            statistics.to_csv(stat_file,index=False)
    else:
        statistics = pd.read_csv(stat_file)
    # For 100 quotes thre should be 100 other works
    # on average each quote is at least used by one other work
    statistics =statistics.query(f"ground_truth>{threshold}")
    return statistics
        
#%%

def get_query_dists(statistics: pd.DataFrame, log_bins=True):
    df = statistics
    sum_n_works = df.sum_n_works.values
    # quantile = np.quantile(sum_n_works,[0,0.25,0.5,0.75,1])
    num_buckets = 10
    num_bins = num_buckets + 1
    if log_bins:
        _quantile, _ = log_binning(
            sum_n_works, num_bins, ret_bins=True)
    else:
        q = np.linspace(0, 1, num_bins)
        _quantile = np.quantile(sum_n_works, q)
    low = _quantile[:-1]
    high = _quantile[1:]
    query_dist_id = range(num_buckets)
    df = pd.DataFrame(
        {"query_dist_id": query_dist_id, "low": low, "high": high})
    df = df.set_index("query_dist_id")
    return df
# %%


def get_samples(dataset: str, num_samples=10, seed=42, data_dir: Path = project_root/"data",log_bins:bool=True,replace:bool=False,verbose:bool=True):
    statistics = get_statistics(dataset,replace=replace)
    query_dists = get_query_dists(statistics,log_bins=log_bins)
    if log_bins:
        query_dists.to_csv(data_dir/f"{dataset}-quote-query-dists-log-bins.csv", index=False)
    else:
        query_dists.to_csv(data_dir/f"{dataset}-quote-query-dists.csv", index=False)
    sum_n_works = statistics.sum_n_works.values
    ground_truths = statistics.ground_truth.values
    edition_ids = statistics.edition_id.values
    num_buckets = len(query_dists)
    _num_samples = int(num_samples/num_buckets)
    rng = np.random.default_rng(seed=seed)
    samples = []
    sample_ground_truths = []
    query_dist_ids = []
    num_queries = len(sum_n_works)
    if verbose: print(f"Samples for {dataset=}")
    for query_dist_id, low, high in query_dists.itertuples():
        dist_mask = (sum_n_works >= low) & (
            sum_n_works <= high)
        mask_size = sum(dist_mask)
        if verbose:
            print(
                f"Bucket {query_dist_id}: Sampling {_num_samples} from {low=:g} and {high=:g} ({mask_size} items, {(mask_size/num_queries)*100:g}%)")
        sample_ids = rng.choice(a=mask_size, size=_num_samples, replace=False)
        _samples = edition_ids[dist_mask][sample_ids]
        _sample_ground_truths = ground_truths[dist_mask][sample_ids]
        samples.extend(_samples)
        sample_ground_truths.extend(_sample_ground_truths)
        query_dist_ids.extend([query_dist_id]*_num_samples)
        

    df = pd.DataFrame({"edition_id": samples,
                      "ground_truth": sample_ground_truths, "query_dists_id": query_dist_ids})
    df.to_csv(data_dir/f"{dataset}-quotes-samples.csv", index=False)
    return df
# %%

def profile_query(engine, statement, edition_id, ground_truth, database, timeout=None, conn=None):
    with engine.connect() as conn:
        if timeout:
            conn.execute(text(f"SET SESSION max_statement_time={timeout}"))

        if "columnstore" not in database:
            conn.execute(text("FLUSH NO_WRITE_TO_BINLOG TABLES source_piece_statistics_denorm,non_source_pieces,earliest_work_and_pieces_by_cluster,defrag_pieces,textreuse_edition_mapping,edition_authors,textreuse_work_mapping;"))
        conn.execute(text("RESET QUERY CACHE;"))
        try:
            start = time()
            sum_n_works= 0
            result =  conn.execute(
                statement, parameters=dict(estc_id=edition_id)
                ).fetchall()
            for row in result:
                sum_n_works+=row.n_works
            duration = time() - start
            assert sum_n_works == ground_truth
        except OperationalError as e:
            # print("Timeout")
            # print(e)
            sum_n_works = None
        except IndexError as e:
            # print("ColumnStore Timeout")
            # print(e)
            sum_n_works = None
        except AssertionError as e:
            if sum_n_works != 0:
                print(f"Assertion Error. Query returns {sum_n_works}. Stat says: {ground_truth}")
            else:
                # print("Query killed. Returned 0.")
                sum_n_works = None
        if sum_n_works is None:
            duration = None
        return {"edition_id": edition_id, "duration": duration}

# %%


def extended_query_profile(query_type: str, sample: Tuple[str, int], database, timeout: Optional[float] = None):
    engine = engines[database]
    query = QUERY_TYPE_MAP[query_type]
    edition_id, ground_truth = sample
    result = profile_query(engine, query, edition_id,
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
            "sample": list(get_samples("hpc-hd")[["edition_id", "ground_truth"]].itertuples(index=False, name=None)),
            "database":["hpc-hd","hpc-hd-columnstore"],
            "timeout":[timeout]
        },
        {
            "query_type":list(QUERY_TYPE_MAP.keys()),
            "sample":list(get_samples("hpc-hd-newspapers")[["edition_id","ground_truth"]].itertuples(index=False,name=None)),
            "database":["hpc-hd-newspapers","hpc-hd-newspapers-columnstore"],
            "timeout":[timeout]
        }
    ]
    grid = ParameterGrid(param_grid)

    # with mp.Pool(processes=1) as pool:
    #     rows = []
    #     for result in tqdm(pool.imap_unordered(wrap_query_profile,grid), total=len(grid)):
    #         rows.append(result)
    rows = []
    for params in tqdm(grid, total=len(grid)):
        result = wrap_query_profile(params)
        rows.append(result)
    return pd.DataFrame(rows)
# %%


if __name__ == "__main__":
    df = profile(900)
    df.to_csv(project_root/"data"/"quote-queries-results-1.csv",index=False)
    #%%
    # timeout = 120
    # param_grid = [
    #     {
    #         "query_type": ["standard"],#list(QUERY_TYPE_MAP.keys()),
    #         "sample": list(get_samples("hpc-hd")[["edition_id", "ground_truth"]].itertuples(index=False, name=None)),
    #         "database":["hpc-hd"],
    #         "timeout":[timeout],
    #     },
    #     # {
    #     #     "query_type":list(QUERY_TYPE_MAP.keys()),
    #     #     "sample":list(get_samples("hpc-hd-newspapers")[["edition_id","ground_truth"]].itertuples(index=False,name=None)),
    #     #     "database":["hpc-hd-newspapers"]#,"hpc-hd-newspapers-columnstore"]
    #     # }
    # ]
    # grid = ParameterGrid(param_grid)
    # rows = []
    # for params in tqdm(grid, total=len(grid)):
    #     result = wrap_query_profile(params)
    #     print(params["database"],params["sample"], result["duration"])
    #     rows.append(result)
    # df = pd.DataFrame(rows)
    # %%


# ANALYZE TABLE source_piece_statistics_denorm,non_source_pieces,earliest_work_and_pieces_by_cluster,defrag_pieces,textreuse_edition_mapping,edition_authors,textreuse_work_mapping;