from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
from time import perf_counter as time
import argparse
import logging
import os
from pathlib import Path
import pandas as pd
import numpy as np
import sys
from spark_utils import *
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
#%%

def cons_merge(df:pd.DataFrame,upper_lim=180,lower_lim=10,size_frac=4,verbose=False):
    df = df.sort_values(by=["trs_start"])
    starts = np.array([])
    ends = np.array([])
    pieces = np.array([])
    mappings = []
    limit_min =[]
    for index,row in df.iterrows():
        t_start = row.trs_start
        t_end = row.trs_end
        piece = row.piece_id
        idx = np.where(starts>=t_start-upper_lim)[0]
        if len(idx) == 0:
            starts=np.array([])
            ends=np.array([])
            pieces=np.array([])
        else:
            starts = starts[idx[0]:]
            ends = ends[idx[0]:]
            pieces=pieces[idx[0]:]
        starts= np.append(starts,t_start)
        ends = np.append(ends,t_end)
        pieces = np.append(pieces,piece)

        limit = np.minimum(
                np.maximum(
                np.minimum(
                    t_end-t_start,ends-starts)
                    /size_frac,
                    lower_lim),
                upper_lim)
        if verbose:
            print(limit)
        idx = np.where((np.abs(starts-t_start)<=limit)&
                        (np.abs(ends-t_end)<=limit))[0][0]
        mappings.append(pieces[idx])
        limit_min.append(limit.min())
    df["new_piece"]= np.array(mappings).astype(int)
    df = df.groupby(["new_piece"]).agg(t_start=("trs_start",min),t_end=("trs_end",max),pieces=("piece_id",list))
    df = df.reset_index()
    df = df.rename(columns={"new_piece":"new_piece_id"})
    df["len"] = df.t_end-df.t_start
    return df
#%%
from typing import Iterator, Tuple
from pyspark.sql.functions import struct, col
from pyspark.sql.functions import pandas_udf
import bisect
@pandas_udf("int")
def map_piece(iterator: Iterator[Tuple[pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    starts = []
    ends = []
    pieces = []
    upper_lim = 180
    lower_lim = 10
    size_frac = 4
    for t_start,t_end,piece_id in iterator:
        # find valid start offsets for given start offset
        idx = bisect.bisect_left(starts,t_start-upper_lim)
        # drop invalid offsets
        starts = starts[idx:]
        ends = ends[idx:]
        pieces = pieces[idx:]
        # append current row
        starts.append(t_start)
        ends.append(t_end)
        pieces.append(piece_id)
        #
        _starts = np.asarray(starts)
        _ends = np.asarray(ends)
        limit = np.minimum(
                np.maximum(
                np.minimum(
                    t_end-t_start,_ends-_starts)
                    /size_frac,lower_lim),
                    upper_lim)
        idx = np.where((np.abs(_starts-t_start)<=limit)&
                        (np.abs(_ends-t_end)<=limit))[0][0]
        yield piece_id[idx]
#%%
def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_bucket",type=str,help="The s3 bucket where processed files are stored",default="textreuse-processed-data")
    parser.add_argument("--num_partitions",type=int,default=200,help="Number of partitions for spark")
    parser.add_argument(
    '-d', '--debug',
    help="Print lots of debugging statements",
    action="store_const", dest="loglevel", const=logging.DEBUG,
    default=logging.WARNING,
    )
    parser.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
    )
    return parser
#%%

args = get_parser().parse_args([])
#%%
textreuses = get_s3(fname="textreuses",bucket=args.s3_bucket)
textreuses = materialise_local("textreuses",df=sc.parallelize(textreuses.take(1000)).toDF())
#%%
pieces = materialise_s3(
    fname="pieces", 
    df = spark.sql("""
    SELECT 
        textreuse_id*2-1 AS piece_id, 
        trs1_id AS trs_id,
        trs1_start AS trs_start,
        trs1_end AS trs_end 
    FROM textreuses
    UNION ALL
    SELECT 
        textreuse_id*2 AS piece_id,
        trs2_id AS trs_id,
        trs2_start AS trs_start,
        trs2_end AS trs_end 
    FROM textreuses
    """),
    bucket=args.s3_bucket
    )
#%%

spark.sql("""SELECT piece_id AS orig_piece_id, map_piece(trs_start,trs_end,piece_id) OVER (PARTITION BY trs_id ORDER BY trs_start, piece_id) AS new_piece_id
FROM pieces""")
#%%

def _map_piece(df:pd.DataFrame):
    starts = []
    ends = []
    pieces = []
    upper_lim = 180
    lower_lim = 10
    size_frac = 4
    for _,row in df.iterrows():
        t_start = row.trs_start
        t_end = row.trs_end
        piece_id = row.piece_id
        # find valid start offsets for given start offset
        idx = bisect.bisect_left(starts,t_start-upper_lim)
        # drop invalid offsets
        starts = starts[idx:]
        ends = ends[idx:]
        pieces = pieces[idx:]
        # append current row
        starts.append(t_start)
        ends.append(t_end)
        pieces.append(piece_id)
        #
        _starts = np.asarray(starts)
        _ends = np.asarray(ends)
        limit = np.minimum(
                np.maximum(
                np.minimum(
                    t_end-t_start,_ends-_starts)
                    /size_frac,lower_lim),
                    upper_lim)
        idx = np.where((np.abs(_starts-t_start)<=limit)&
                        (np.abs(_ends-t_end)<=limit))[0][0]
        yield piece_id[idx]
# %%
