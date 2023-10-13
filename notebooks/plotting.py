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
    project_root = Path.cwd().parent.resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()
import sys
sys.path.append(str(project_root))
from spark_utils import *
from time import perf_counter as time
import numpy as np
import pandas as pd
from tqdm.autonotebook import trange,tqdm
from db_utils import *
from sqlalchemy import text
import seaborn as sns
import matplotlib.ticker as tkr
import matplotlib.pyplot as plt
from db_utils import *
#%%

def load_row_store_table_sizes(dataset:str,data_dir:Path = project_root/"data",replace:bool = True):
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
                    params= {"dataset":dataset})
            sizes.to_csv(size_file,index=False)
    else:
        sizes = pd.read_csv(size_file)
    return sizes



def load_table_sizes(replace:bool=False):
    size_multiple = {"MB":1024**2,"GB":1024**3}
    newspapers_columnstore_sizes = pd.read_csv("../data/hpc-hd-newspapers-columnstore-table-sizes.csv") 
    newspapers_columnstore_sizes[["size_number","unit"]] = newspapers_columnstore_sizes.TOTAL_USAGE.str.split(expand=True)
    newspapers_columnstore_sizes.size_number = newspapers_columnstore_sizes.size_number.astype(float)
    newspapers_columnstore_sizes["total_size"] = newspapers_columnstore_sizes["size_number"].multiply(newspapers_columnstore_sizes["unit"].apply(lambda s: size_multiple[s]))
    newspapers_columnstore_sizes["total_data_size"] = newspapers_columnstore_sizes.total_size
    newspapers_columnstore_sizes["total_index_size"] = 0

    newspapers_aria_sizes = load_row_store_table_sizes("hpc-hd-newspapers",replace=replace)
    newspapers_aria_sizes["total_size"] = newspapers_aria_sizes["Total Size (MB)"]*size_multiple["MB"]
    newspapers_aria_sizes["total_data_size"] = newspapers_aria_sizes["Data Size (MB)"]*size_multiple["MB"]
    newspapers_aria_sizes["total_index_size"] = newspapers_aria_sizes["Index Size (MB)"]*size_multiple["MB"]

    columns = ["TABLE_SCHEMA","TABLE_NAME","total_size","total_data_size","total_index_size"]
    df = pd.concat([
        newspapers_columnstore_sizes[columns],
        newspapers_aria_sizes[columns],
        ])
    return df

#https://stackoverflow.com/questions/71558497/matlibplot-network-bytes-y-axis-to-human-readable
def sizeof_fmt(x, pos):
    if x<0:
        return ""
    for x_unit in ['bytes', 'kB', 'MB', 'GB', 'TB']:
        if x < 1024.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1024.0
# %%
sizes = load_table_sizes()

# %%
sizes.groupby("TABLE_SCHEMA").total_size.sum().plot(kind="bar")
plt.gca().yaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
plt.title("Database Total sizes")
plt.xticks(rotation=0)
plt.ylabel("Size")
plt.xlabel("Database")
#%%
plt.figure(figsize=(10,10))
sns.barplot(data=sizes,y="TABLE_NAME",x="total_size",hue="TABLE_SCHEMA",orient='h')
plt.xscale("log",base=2)
plt.gca().xaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
# plt.xticks(rotation=90)
plt.legend(bbox_to_anchor=(1,0.5),loc="upper left")
plt.title("Table Sizes (Data + Indexes)")
plt.xlabel("Sizes (log scale)")
# %%

def get_query_types_table_sizes(sizes):
    denorm_query_tables = ["reception_edges_denorm","textreuse_ids"]
    intermediate_query_tables = ["textreuse_ids","defrag_pieces","earliest_work_and_pieces_by_cluster","non_source_pieces"]
    standard_query_tables = ["textreuse_ids","earliest_work_and_pieces_by_cluster","clustered_defrag_pieces","defrag_pieces"]
    df = []
    for query_name,tables in zip(["denorm","intermediate","standard"],[denorm_query_tables,intermediate_query_tables,standard_query_tables]):
        _df = sizes[sizes.TABLE_NAME.isin(tables)].groupby("TABLE_SCHEMA").total_size.sum().to_frame().reset_index()
        _df["query_type"]=query_name
        df.append(_df)

    df = pd.concat(df)
    return df 

query_table_sizes= get_query_types_table_sizes(sizes)
#%%
def get_running_times():
    columnstore_results = pd.read_csv("../data/hpc-hd-newspapers-columnstore-reception-analysis.csv")
    columnstore_results["TABLE_SCHEMA"] = "hpc-hd-newspapers-columnstore"
    aria_results = pd.read_csv("../data/hpc-hd-newspapers-aria-reception-analysis.csv")
    aria_results["TABLE_SCHEMA"] = "hpc-hd-newspapers"
    df = pd.concat([aria_results,columnstore_results])
    return df
# %%
running_times = get_running_times()
running_times = running_times.merge(query_table_sizes,on=["TABLE_SCHEMA","query_type"])
#%%

hue = running_times[['quantile','query_type', 'TABLE_SCHEMA']].apply(
    lambda row: f"{row['quantile']}, {row.query_type}, {row.TABLE_SCHEMA}", axis=1)
hue.name = 'quantile, query_type, Database'
hue = running_times[['query_type', 'TABLE_SCHEMA']].apply(
    lambda row: f"{row.query_type}, {row.TABLE_SCHEMA}", axis=1)
hue.name = 'quantile, query_type, Database'
sns.violinplot(x='total_size', y='Duration', hue=hue, data=running_times)
# plt.xscale("log",base=2)
plt.yscale("symlog")
labels = plt.gca().get_xticklabels()
print(labels)
labels = [sizeof_fmt(float(x.get_text())) for x in labels]
plt.gca().set_xticklabels(labels)
plt.legend(bbox_to_anchor=(1,0.5),loc="upper left")
plt.xlabel("Disk Size Used (Tables + Indexes)")
# plt.gca().xaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
# %%
sns.v(data=running_times[running_times.TABLE_SCHEMA=="hpc-hd-newspapers-columnstore"],x="quantile",y="Duration",hue="query_type")
plt.title("hcp-hd-newspapers-columnstore")
plt.yscale("log")
# %%
sns.barplot(data=running_times[running_times.TABLE_SCHEMA=="hpc-hd-newspapers-columnstore"],x="quantile",y="Duration",hue="query_type")
plt.yscale("log")
plt.title("hcp-hd-newspapers")
