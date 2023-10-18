# %%
from tqdm.autonotebook import trange, tqdm
from plot_utils import *
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import seaborn as sns
from sqlalchemy import text
from db_utils import *
import pandas as pd
import numpy as np
from time import perf_counter as time
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
    project_root = Path.cwd().parent.resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
import sys
sys.path.append(str(project_root))
import mariadb_quote_query_analysis as quote_analysis
# from spark_utils import *
# %%
def load_row_store_table_sizes(dataset: str, data_dir: Path = project_root/"data", replace: bool = False):
    size_file = data_dir/f"{dataset}-aria-table-sizes.csv"
    if not size_file.exists() or replace:
        with get_sqlalchemy_connect(dataset) as conn:
            sizes = pd.read_sql(text("""
                    SELECT table_schema as `TABLE_SCHEMA`, table_name AS `TABLE_NAME`,
                    ROUND((data_length  / 1024 / 1024), 2) `Data Size (MB)`,
                    ROUND((index_length / 1024 / 1024), 2) `Index Size (MB)`,
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) `Total Size (MB)` 
                    FROM information_schema.TABLES 
                    WHERE table_schema = :dataset
                    ORDER BY (data_length + index_length) DESC;
                                    """),
                                con=conn,
                                params={"dataset": dataset})
            sizes.to_csv(size_file, index=False)
    else:
        sizes = pd.read_csv(size_file)
    return sizes


def load_columnstore_store_table_sizes(dataset: str, data_dir: Path = project_root/"data", replace: bool = False):
    database = dataset + "-columnstore"
    size_file = data_dir/f"{database}-table-sizes.csv"
    if not size_file.exists() or replace:
        with get_sqlalchemy_connect(database) as conn:
            sizes = pd.read_sql(text(f"""call columnstore_info.table_usage(:database,null)"""),
                                con=conn,
                                params={"database": database})
            sizes.to_csv(size_file,index=False)
    else:
        sizes = pd.read_csv(size_file)
    return sizes


def load_table_sizes(dataset, replace: bool = False):
    size_multiple = {"MB": 1024**2, "GB": 1024**3, "KB":1024}
    newspapers_columnstore_sizes = load_columnstore_store_table_sizes(
        dataset, replace=replace)
    newspapers_columnstore_sizes[[
        "size_number", "unit"]] = newspapers_columnstore_sizes.TOTAL_USAGE.str.split(expand=True)
    newspapers_columnstore_sizes.size_number = newspapers_columnstore_sizes.size_number.astype(
        float)
    newspapers_columnstore_sizes["total_size"] = newspapers_columnstore_sizes["size_number"].multiply(
        newspapers_columnstore_sizes["unit"].apply(lambda s: size_multiple[s]))
    newspapers_columnstore_sizes["total_data_size"] = newspapers_columnstore_sizes.total_size
    newspapers_columnstore_sizes["total_index_size"] = 0

    newspapers_aria_sizes = load_row_store_table_sizes(
        dataset, replace=replace)
    newspapers_aria_sizes["total_size"] = newspapers_aria_sizes[
        "Total Size (MB)"]*size_multiple["MB"]
    newspapers_aria_sizes["total_data_size"] = newspapers_aria_sizes[
        "Data Size (MB)"]*size_multiple["MB"]
    newspapers_aria_sizes["total_index_size"] = newspapers_aria_sizes[
        "Index Size (MB)"]*size_multiple["MB"]

    columns = ["TABLE_SCHEMA", "TABLE_NAME", "total_size",
               "total_data_size", "total_index_size"]
    df = pd.concat([
        newspapers_columnstore_sizes[columns],
        newspapers_aria_sizes[columns],
    ])
    return df

# https://stackoverflow.com/questions/71558497/matlibplot-network-bytes-y-axis-to-human-readable


def sizeof_fmt(x, pos=None):
    if x < 0:
        return ""
    for x_unit in ['bytes', 'kB', 'MB', 'GB', 'TB']:
        if x < 1024.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1024.0

# %%

QUERY_TABLES_MAP = {
    "reception": {
        "denorm": ["reception_edges_denorm", "textreuse_ids"],
        "intermediate": ["textreuse_ids", "defrag_pieces", "earliest_work_and_pieces_by_cluster", "non_source_pieces"],
        "standard":  ["textreuse_ids", "earliest_work_and_pieces_by_cluster", "clustered_defrag_pieces", "defrag_pieces"]
    },
    "quote":{
        "denorm": ["source_piece_statistics_denorm","edition_ids"],
        "intermediate":["textreuse_edition_mapping","defrag_pieces","earliest_work_and_pieces_by_cluster","edition_authors","edition_ids","non_source_pieces","textreuse_work_mapping"],
        "standard":["textreuse_edition_mapping","defrag_pieces","earliest_work_and_pieces_by_cluster","edition_authors","edition_ids","textreuse_work_mapping"]
    }
}


def get_query_types_table_sizes(query,sizes):

    query_tables = QUERY_TABLES_MAP[query]
    df = []
    for query_name, tables in query_tables.items():
        _df = sizes[sizes.TABLE_NAME.isin(tables)].groupby(
            "TABLE_SCHEMA").total_size.sum().to_frame().reset_index()
        _df["query_type"] = query_name
        df.append(_df)

    df = pd.concat(df)
    return df
# %%


def get_running_times(query,dataset,data_dir=project_root/"data"):
    # columnstore_results = pd.read_csv(
    #     "../data/hpc-hd-newspapers-columnstore-reception-analysis.csv")
    # columnstore_results["TABLE_SCHEMA"] = "hpc-hd-newspapers-columnstore"
    # aria_results = pd.read_csv(
    #     "../data/hpc-hd-newspapers-aria-reception-analysis.csv")
    # aria_results["TABLE_SCHEMA"] = "hpc-hd-newspapers"
    # df = pd.concat([aria_results, columnstore_results])
    if query == "reception":
        dfs = []
        for file in data_dir.glob("reception-queries-results*"):
            dfs.append(pd.read_csv(file))
        df = pd.concat(dfs)
        df = df[df.database.isin([dataset,dataset+"-columnstore"])]
        samples = pd.read_csv(data_dir/f"{dataset}-samples.csv")
        df = df.merge(samples,left_on="doc_id",right_on="manifestation_id")
        df = df.rename(columns={"database":"TABLE_SCHEMA"})
    elif query == "quote":
        df = pd.read_csv(data_dir/"quote-queries-results-1.csv")
        df = df[df.database.isin([dataset,dataset+"-columnstore"])]
        if dataset == "hpc-hd":
            samples = pd.read_csv(data_dir/f"{dataset}-quotes-samples-orig.csv")
        else:
            samples = pd.read_csv(data_dir/f"{dataset}-quotes-samples.csv")
        df = df.merge(samples,on="edition_id")
        df = df.rename(columns={"database":"TABLE_SCHEMA"})

    return df

#%%
dataset = "hpc-hd"
query = "reception"
sizes = load_table_sizes(dataset)
sizes["TABLE_SCHEMA"] = sizes.TABLE_SCHEMA.apply(lambda s: s if "columnstore" in s else s + "-rowstore" )
#%%
sizes.groupby("TABLE_SCHEMA").total_size.sum().plot(kind="bar")
plt.gca().yaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
plt.title("Database Total sizes")
plt.xticks(rotation=0)
plt.ylabel("Size")
plt.xlabel("Database")
plt.figure(figsize=(10, 10))
sns.barplot(data=sizes, y="TABLE_NAME", x="total_size",
            hue="TABLE_SCHEMA", orient='h')
plt.xscale("log", base=2)
plt.gca().xaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
# plt.xticks(rotation=90)
plt.legend(bbox_to_anchor=(1, 0.5), loc="upper left")
plt.title("Table Sizes (Data + Indexes)")
plt.xlabel("Sizes (log scale)")
query_table_sizes = get_query_types_table_sizes(query,sizes)
#%%
running_times = get_running_times(query,dataset)
running_times["TABLE_SCHEMA"] = running_times.TABLE_SCHEMA.apply(lambda s: s if "columnstore" in s else s + "-rowstore" )
running_times = running_times.merge(
    query_table_sizes, on=["TABLE_SCHEMA", "query_type"])
# %%
if dataset == "hpc-hd-newspapers" and query == "quote":
    _df = running_times.query("query_dists_id<7")
elif dataset == "hpc-hd" and query == "reception":
    _df = running_times.query("query_dists_id<9")
elif dataset == "hpc-hd-newspapers" and query == "reception":
    _df = running_times.query("query_dists_id<9")
else:
    _df = running_times
hue = _df[['query_type', 'TABLE_SCHEMA']].apply(
    lambda row: f"{row.query_type}, {row.TABLE_SCHEMA}", axis=1)
hue.name = 'query_type, query_type'
sns.pointplot(data=_df, x="total_size", y="duration", hue=hue,native_scale=True,log_scale=[2,False])
plt.yscale("log")
ticks = [s for s in _df.total_size.unique()]
labels = [sizeof_fmt(s) for s in ticks]
plt.xticks(labels=labels,ticks=ticks,rotation=90,minor=False)
plt.legend(bbox_to_anchor=(1,0.5),loc="center left",title=hue.name)
plt.xlabel("Disk Size Used (Tables + Indexes)")
plt.ylabel("Query Duration (in sec)")
#%%
sns.catplot(data=running_times,col="TABLE_SCHEMA",x="query_dists_id",y="duration",hue="query_type",kind="bar")
plt.yscale("log")
#%%
hpc_hd_stats = quote_analysis.get_statistics("hpc-hd-newspapers",threshold=0)
hpc_hd_samples = quote_analysis.get_samples("hpc-hd-newspapers")
hpc_hd_samples = hpc_hd_samples.merge(
    hpc_hd_stats, on=["edition_id", "ground_truth"])
#%%
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(5, 10))
loglog_hist(hpc_hd_stats.sum_n_works, ax=ax2, **
            {"alpha": 0.5, "ec": None, "label": "sum_n_works distribution"})
for bucket, sample in enumerate(hpc_hd_samples.sum_n_works.values):
    color = next(ax2._get_lines.prop_cycler)["color"]
    ax2.axvline(sample, color=color, linestyle="-",
                label=f"Bucket {bucket} sample")
ax2.legend(loc="center left", bbox_to_anchor=(1, 0.5))
ax2.set_xlabel("sum_n_works")
ax2.set_ylabel("frequency")
ax2.set_title("Quote Query Worload Distribution")

loglog_hist(hpc_hd_stats.ground_truth, ax=ax1, **
            {"alpha": 0.5, "ec": None, "label": "ground_truth distribution"})
ax1.axvline(100, color="black", linestyle="--", label="Threshold")
ax1.legend(loc="center left", bbox_to_anchor=(1, 0.5))
ax1.set_xlabel("ground_truth")
ax1.set_ylabel("frequency")
ax1.set_title("Query Ground Truth Distribution")

fig.suptitle("HPC-HD dataset")
# %%
