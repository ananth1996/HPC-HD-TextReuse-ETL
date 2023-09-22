#%%
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
from typing import Tuple,Optional
from db_utils import *
from plot_utils import log_binning
import multiprocessing as mp

#%%
denorm_query=text("""
SELECT COUNT(*) FROM reception_edges_denorm re 
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
),final_result AS (
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
)
SELECT COUNT(*) FROM final_result""")
                          
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
),final_result AS (
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
)
SELECT COUNT(*) FROM final_result""")

QUERY_TYPE_MAP = { "denorm":denorm_query,"intermediate":intermediate_query,"standard":standard_query}

#%%


def get_query_dists(statistics:pd.DataFrame,log_bins = True):
    df = statistics
    num_reception_edges = df.num_reception_edges.values
    # quantile = np.quantile(num_reception_edges,[0,0.25,0.5,0.75,1])
    num_buckets= 10
    num_bins = num_buckets + 1
    q = np.linspace(0,1,num_buckets)
    if log_bins:
        _quantile,_ = log_binning(num_reception_edges,num_bins,ret_bins=True)
    else:
        _quantile = np.quantile(num_reception_edges,q)
    low = _quantile[:-1]
    high = _quantile[1:]
    query_dist_id = range(num_buckets)
    df = pd.DataFrame({"query_dist_id":query_dist_id,"low":low,"high":high})
    df = df.set_index("query_dist_id")
    return df
#%%
def get_samples(database:str,num_samples=100,seed=2,data_dir:Path=project_root/"data"):
    statistics = pd.read_csv(data_dir/f"{database}-num-reception-edges.csv")
    query_dists = get_query_dists(statistics)
    query_dists.to_csv(data_dir/f"{database}-query-dists.csv",index=False)
    num_reception_edges = statistics.num_reception_edges.values
    manifestation_ids = statistics.manifestation_id.values
    num_buckets = len(query_dists)
    _num_samples = int(num_samples/num_buckets)
    rng = np.random.default_rng(seed=seed)
    samples = []
    sample_ground_truths = []
    query_dist_ids = []
    for query_dist_id,low,high in query_dists.itertuples():
        dist_mask = (num_reception_edges>=low)&(num_reception_edges<=high)
        mask_size = sum(dist_mask)
        sample_ids = rng.choice(a=mask_size,size=_num_samples,replace=False)
        _samples = manifestation_ids[dist_mask][sample_ids]
        _sample_ground_truths = num_reception_edges[dist_mask][sample_ids]
        samples.extend(_samples)
        sample_ground_truths.extend(_sample_ground_truths)
        query_dist_ids.extend([query_dist_id]*_num_samples)
        print(f"Quantile {query_dist_id}: Sampling {_num_samples} from {low=:g} and {high=:g}")

    df = pd.DataFrame({"manifestation_id":samples,"ground_truth":sample_ground_truths,"query_dists_id":query_dist_ids})
    df.to_csv(data_dir/f"{database}-samples.csv",index=False)
    # samples_df = pd.DataFrame.from_dict(samples).melt(var_name="quantile",value_name="doc_id")
    # sample_ground_truths_df = pd.DataFrame.from_dict(sample_ground_truths).melt(var_name="quantile",value_name="ground_truth")
    # samples_df = samples_df.join(sample_ground_truths_df["ground_truth"])
    # print("Maximum Number of rows:",sample_ground_truths_df.ground_truth.max())
    return df
#%%

doc_id_pattern=re.compile("manifestation_id='(.*?)'")

def query_timeout(conn, cursor, statement, parameters,
                                    context, executemany):
    timeout = context.execution_options.get('timeout', None)
    if timeout is not None:
        statement = f"SET STATEMENT max_statement_time={timeout} FOR "+ statement
        # print("Adding timeout")
        # print(statement)
    return statement, parameters

def profile_query(statement,doc_id,ground_truth,database,timeout=None):
    engine = get_sqlalchemy_engine(version=database)
    with engine.connect() as conn:
        event.listen(conn, "before_cursor_execute", query_timeout,retval=True)
        conn.execute(text("SET SESSION profiling=0;"))
        if "columnstore" not in database:
            conn.execute(text("FLUSH NO_WRITE_TO_BINLOG TABLES reception_edges_denorm,non_source_pieces,manifestation_ids,earliest_work_and_pieces_by_cluster,defrag_pieces,clustered_defrag_piecess;"))
        conn.execute(text("RESET QUERY CACHE;"))
        conn.execute(text("SET SESSION profiling=1;"))
        conn.execute(text("SET SESSION profiling_history_size=1;"))
        try:
            result = conn.execute(statement,parameters=dict(doc_id=doc_id),execution_options={"timeout":timeout}).fetchall()[0][0]
        except OperationalError as e:
            # print("Timeout")
            result = None
        except IndexError as e:
            # print("ColumnStore Timeout")
            result = None
        if result is not None:
            assert result == ground_truth
            profiles = conn.execute(text("SHOW PROFILES;")).fetchall()
            # convert the first profile into dict
            profile = profiles[0]._asdict()
            # ensure we have correct document id query result
            assert doc_id_pattern.search(profile["Query"]).group(1) == doc_id
            duration = profile["Duration"]
        else:
            duration = None
        return {"doc_id":doc_id,"duration":duration}

#%%

from sklearn.model_selection import ParameterGrid
import functools
def extended_query_profile(query_type:str,sample:Tuple[str,int],database,timeout:Optional[float]=None):
        query = QUERY_TYPE_MAP[query_type]
        manifestation_id, ground_truth = sample
        result = profile_query(query,manifestation_id,ground_truth,database,timeout=timeout)
        result.update({
            "query_type":query_type,
            "database":database
        }
        )
        return result


def wrap_query_profile(kwargs):
    return extended_query_profile(**kwargs)
def profile(timeout:Optional[float]=None):
    param_grid = [
        {
            "query_type":list(QUERY_TYPE_MAP.keys()),
            "sample":list(get_samples("hpc-hd")[["manifestation_id","ground_truth"]].itertuples(index=False,name=None)),
            "database":["hpc-hd-columnstore"],
            "timeout":[2]
        },
        # {
        #     "query_type":list(QUERY_TYPE_MAP.keys()),
        #     "sample":list(get_samples("hpc-hd-newspapers")[["manifestation_id","ground_truth"]].itertuples(index=False,name=None)),
        #     "database":["hpc-hd-newspapers","hpc-hd-newspapers-columnstore"]
        # }
    ]
    grid = ParameterGrid(param_grid)

    with mp.Pool(processes=12) as pool:
        rows = []
        for result in tqdm(pool.imap_unordered(wrap_query_profile,grid), total=len(grid)):
            rows.append(result)
    
    return pd.DataFrame(rows)
# %%

if __name__ == "__main__":
    df = profile(2)
    df.to_csv(project_root/"data"/"results.csv",index=False)
# %%
