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
from collections import OrderedDict
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
import mariadb_query_analysis as reception_analysis
from spark_utils_alternate import (
    get_spark_session,
    BUCKETS_MAP,
    TABLES_MAP,
    get_s3_parquet_size,
)

# %%

columnwidth = 241.1474
fullwidth = 506.295


def setup_matplotlib(dpi_scale=1, fontscale=1):
    plt.rcParams.update(
        {
            "text.usetex": True,
            "text.latex.preamble": r"\usepackage[tt=false]{libertine} \RequirePackage[varqu]{zi4} \usepackage[libertine]{newtxmath}",
            # 'text.latex.preamble' :r'\usepackage{amsmath} ',
            "font.size": 9 * fontscale,
            "font.family": "Times New Roman",
            "axes.titlesize": 9 * fontscale,
            "figure.dpi": 100 * dpi_scale,
            "xtick.labelsize": 8 * fontscale,
            "ytick.labelsize": 8 * fontscale,
            "legend.fontsize": 8 * fontscale,
            "legend.fontsize": 8 * fontscale,
            "figure.titlesize": 9 * fontscale,
        }
    )


def set_size(width, fraction=1, subplots=(1, 1)):
    """Set figure dimensions to avoid scaling in LaTeX.

    Parameters
    ----------
    width: float or string
            Document width in points, or string of predined document type
    fraction: float, optional
            Fraction of the width which you wish the figure to occupy
    subplots: array-like, optional
            The number of rows and columns of subplots.
    Returns
    -------
    fig_dim: tuple
            Dimensions of figure in inches
    """
    if width == "thesis":
        width_pt = 426.79135
    elif width == "beamer":
        width_pt = 307.28987
    else:
        width_pt = width

    # Width of figure (in pts)
    fig_width_pt = width_pt * fraction
    # Convert from pt to inches
    inches_per_pt = 1 / 72.27

    # Golden ratio to set aesthetic figure height
    # https://disq.us/p/2940ij3
    golden_ratio = (5**0.5 - 1) / 2

    # Figure width in inches
    fig_width_in = fig_width_pt * inches_per_pt
    # Figure height in inches
    fig_height_in = fig_width_in * golden_ratio * (subplots[0] / subplots[1])

    return (fig_width_in, fig_height_in)


def load_spark_table_sizes(
    dataset: str, data_dir: Path = project_root / "data", replace: bool = False
):
    size_file = data_dir / f"{dataset}-saprk-table-sizes.csv"
    database = dataset + "-spark"
    if not size_file.exists() or replace:
        spark = get_spark_session(application_name="sizes")
        rows = []
        for table, _bucket in TABLES_MAP[dataset]:
            bucket = BUCKETS_MAP[dataset][_bucket]
            fsize = get_s3_parquet_size(spark, table, bucket)
            rows.append(
                {"TABLE_SCHEMA": database, "TABLE_NAME": table, "total_size": fsize}
            )
        sizes = pd.DataFrame(rows)
        sizes.to_csv(size_file, index=False)
        spark.stop()
    else:
        sizes = pd.read_csv(size_file)
    return sizes


# %%


def load_row_store_table_sizes(
    dataset: str, data_dir: Path = project_root / "data", replace: bool = False
):
    size_file = data_dir / f"{dataset}-aria-table-sizes.csv"
    if not size_file.exists() or replace:
        with get_sqlalchemy_connect(dataset) as conn:
            sizes = pd.read_sql(
                text(
                    """
                    SELECT table_schema as `TABLE_SCHEMA`, table_name AS `TABLE_NAME`,
                    ROUND((data_length  / 1024 / 1024), 2) `Data Size (MB)`,
                    ROUND((index_length / 1024 / 1024), 2) `Index Size (MB)`,
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) `Total Size (MB)` 
                    FROM information_schema.TABLES 
                    WHERE table_schema = :dataset
                    ORDER BY (data_length + index_length) DESC;
                                    """
                ),
                con=conn,
                params={"dataset": dataset},
            )
            sizes.to_csv(size_file, index=False)
    else:
        sizes = pd.read_csv(size_file)
    return sizes


def load_columnstore_store_table_sizes(
    dataset: str, data_dir: Path = project_root / "data", replace: bool = False
):
    database = dataset + "-columnstore"
    size_file = data_dir / f"{database}-table-sizes.csv"
    if not size_file.exists() or replace:
        with get_sqlalchemy_connect(database) as conn:
            sizes = pd.read_sql(
                text(f"""call columnstore_info.table_usage(:database,null)"""),
                con=conn,
                params={"database": database},
            )
            sizes.to_csv(size_file, index=False)
    else:
        sizes = pd.read_csv(size_file)
    return sizes


size_multiple = {"TB": 1024**4, "GB": 1024**3, "MB": 1024**2, "KB": 1024}


def load_table_sizes(dataset, replace: bool = False):
    columnstore_sizes = load_columnstore_store_table_sizes(dataset, replace=replace)
    columnstore_sizes[
        ["size_number", "unit"]
    ] = columnstore_sizes.TOTAL_USAGE.str.split(expand=True)
    columnstore_sizes.size_number = columnstore_sizes.size_number.astype(float)
    columnstore_sizes["total_size"] = columnstore_sizes["size_number"].multiply(
        columnstore_sizes["unit"].apply(lambda s: size_multiple[s])
    )
    columnstore_sizes["total_data_size"] = columnstore_sizes.total_size
    columnstore_sizes["total_index_size"] = 0

    aria_sizes = load_row_store_table_sizes(dataset, replace=replace)
    aria_sizes["total_size"] = aria_sizes["Total Size (MB)"] * size_multiple["MB"]
    aria_sizes["total_data_size"] = aria_sizes["Data Size (MB)"] * size_multiple["MB"]
    aria_sizes["total_index_size"] = aria_sizes["Index Size (MB)"] * size_multiple["MB"]
    aria_sizes["TABLE_SCHEMA"] = aria_sizes.TABLE_SCHEMA.apply(
        lambda dataset: f"{dataset}-rowstore"
    )

    spark_sizes = load_spark_table_sizes(dataset, replace=replace)

    columns = [
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "total_size",
        "total_data_size",
        "total_index_size",
    ]
    df = pd.concat([columnstore_sizes[columns], aria_sizes[columns], spark_sizes])
    return df


# https://stackoverflow.com/questions/71558497/matlibplot-network-bytes-y-axis-to-human-readable


def sizeof_fmt(x, pos=None):
    if x < 0:
        return ""
    for x_unit in ["bytes", "kB", "MB", "GB", "TB"]:
        if x < 1024.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1024.0


# %%

# All storage costs are in Billing Units (BU)/TiB hr
storage_cost_rates = {
    "hpc-hd-spark": 1,
    "hpc-hd-rowstore": 3.5,
    "hpc-hd-columnstore": 3.5,
    "hpc-hd-newspapers-spark": 1,
    "hpc-hd-newspapers-rowstore": 3.5,
    "hpc-hd-newspapers-columnstore": 3.5,
}

# Processing costs are Billing Units(BU)/hr
processing_cost_rates = {
    "hpc-hd-spark": 1254,
    "hpc-hd-rowstore": 24,
    "hpc-hd-columnstore": 24,
    "hpc-hd-newspapers-spark": 1254,
    "hpc-hd-newspapers-rowstore": 24,
    "hpc-hd-newspapers-columnstore": 24,
}


def find_storage_cost(schema, size):
    return storage_cost_rates[schema] * (size / size_multiple["TB"])


def find_processing_cost(schema, duration):
    return processing_cost_rates[schema] * (duration / 3600)


QUERY_TABLES_MAP = {
    "reception": {
        "denorm": ["reception_edges_denorm", "textreuse_ids"],
        "intermediate": [
            "textreuse_ids",
            "defrag_pieces",
            "earliest_work_and_pieces_by_cluster",
            "non_source_pieces",
        ],
        "standard": [
            "textreuse_ids",
            "earliest_work_and_pieces_by_cluster",
            "clustered_defrag_pieces",
            "defrag_pieces",
        ],
    },
    "quote": {
        "denorm": ["source_piece_statistics_denorm", "edition_ids"],
        "intermediate": [
            "textreuse_edition_mapping",
            "defrag_pieces",
            "earliest_work_and_pieces_by_cluster",
            "edition_authors",
            "edition_ids",
            "non_source_pieces",
            "textreuse_work_mapping",
        ],
        "standard": [
            "textreuse_edition_mapping",
            "defrag_pieces",
            "earliest_work_and_pieces_by_cluster",
            "edition_authors",
            "edition_ids",
            "textreuse_work_mapping",
        ],
    },
}


def get_query_types_table_sizes(query, sizes):
    query_tables = QUERY_TABLES_MAP[query]
    df = []
    for query_name, tables in query_tables.items():
        _df = (
            sizes[sizes.TABLE_NAME.isin(tables)]
            .groupby("TABLE_SCHEMA")
            .total_size.sum()
            .to_frame()
            .reset_index()
        )
        _df["query_type"] = query_name
        df.append(_df)

    df = pd.concat(df)
    # find storage costs for tables
    df["storage_cost"] = df.apply(
        lambda row: find_storage_cost(row.TABLE_SCHEMA, row.total_size), axis=1
    )
    return df


# %%


def get_running_times(query, dataset, data_dir=project_root / "data", hot_cache=False):
    if query == "reception":
        dfs = []
        for file in data_dir.glob("reception-queries-results*"):
            print(file)
            dfs.append(pd.read_csv(file))
        if hot_cache:
            dfs[0] = pd.read_csv(
                data_dir / "double-reception-queries-results-rowstore.csv"
            )
        df = pd.concat(dfs)
        df = df[
            df.database.isin([dataset, dataset + "-columnstore", dataset + "-spark"])
        ]
        samples = pd.read_csv(data_dir / f"{dataset}-samples.csv")
        df = df.merge(samples, left_on="doc_id", right_on="manifestation_id")
        df = df.rename(columns={"database": "TABLE_SCHEMA"})
        df["TABLE_SCHEMA"] = df.TABLE_SCHEMA.apply(
            lambda s: s + "-rowstore" if dataset == s else s
        )
    elif query == "quote":
        dfs = []
        for file in data_dir.glob("quote-queries-results*"):
            print(file)
            dfs.append(pd.read_csv(file))
        df = pd.concat(dfs)
        df = df[
            df.database.isin([dataset, dataset + "-columnstore", dataset + "-spark"])
        ]
        samples = pd.read_csv(data_dir / f"{dataset}-quotes-samples.csv")
        df = df.merge(samples, on="edition_id")
        df = df.rename(columns={"database": "TABLE_SCHEMA"})
        df["TABLE_SCHEMA"] = df.TABLE_SCHEMA.apply(
            lambda s: s + "-rowstore" if dataset == s else s
        )
    df["schema"] = df.TABLE_SCHEMA.str.rsplit("-", n=1).str[-1]
    df["dataset"] = df.TABLE_SCHEMA.str.rsplit("-", n=1).str[0]
    df["processing_cost"] = df.apply(
        lambda row: find_processing_cost(row.TABLE_SCHEMA, row.duration), axis=1
    )
    return df


# %%
DATASET_MAP = {
    "hpc-hd": r"$\textsc{Original}$",
    "hpc-hd-newspapers": r"$\textsc{Large}$",
}
QUERY_TYPE_MAP = {
    "standard": r"$\texttt{Standard}$",
    "intermediate": r"$\texttt{Intermediate}$",
    "denorm": r"$\texttt{Denorm}$",
}
SCHEMA_TYPE_MAP = OrderedDict({
    "spark": r"$\texttt{Spark}$",
    "rowstore": r"$\texttt{Aria}$",
    "columnstore": r"$\texttt{Columnstore}$",
})


def remap_df(df):
    return df.replace(
        {
            "schema": SCHEMA_TYPE_MAP,
            "dataset": DATASET_MAP,
            "query_type": QUERY_TYPE_MAP,
        }
    )
#%%
def plot_sizes(save_fig=False):
    setup_matplotlib()
    _dfs = []
    for dataset in ["hpc-hd", "hpc-hd-newspapers"]:
        sizes = load_table_sizes(dataset)
        sizes["schema"] = sizes.TABLE_SCHEMA.str.rsplit("-", n=1).str[-1]
        sizes["dataset"] = sizes.TABLE_SCHEMA.str.rsplit("-", n=1).str[0]
        _dfs.append(sizes)

    necessary_tables = list(
        {
            table
            for _, materialisations in QUERY_TABLES_MAP.items()
            for _, tables in materialisations.items()
            for table in tables
        }
    )
    sizes = pd.concat(_dfs)
    _sizes = sizes[sizes.TABLE_NAME.isin(necessary_tables)]
    _sizes = remap_df(_sizes)
    schema_order = list(SCHEMA_TYPE_MAP.values())
    _sizes_gb = _sizes.groupby(["dataset", "schema"]).total_size.sum().reset_index()
    figsize=np.array(set_size(columnwidth,subplots=(1,1)))
    fig,ax = plt.subplots(1,1,figsize=figsize)
    sns.barplot(data=_sizes_gb, x="schema", y="total_size", hue="dataset",order=schema_order,ax=ax)
    sns.move_legend(ax,loc="best",title="Dataset")
    # _sizes.groupby("schema").total_size.sum().plot(kind="bar")
    ax.yaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
    # plt.xticks(rotation=90)
    ax.set_ylabel(f"Total Data Size on Disk")
    ax.set_xlabel("Framework")
    fig.subplots_adjust(right=1)
    if save_fig:
        plt.savefig(
            plots_dir/"sizes.pdf",
            bbox_inches="tight",
            pad_inches=0
        )
    
    plt.figure(figsize=(10, 10))
    sns.barplot(
        data=_sizes, y="TABLE_NAME", x="total_size", hue="TABLE_SCHEMA", orient="h"
    )
    plt.xscale("log", base=2)
    plt.gca().xaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
    # plt.xticks(rotation=90)
    plt.legend(bbox_to_anchor=(1, 0.5), loc="upper left")
    plt.title(f"Table Sizes (Data + Indexes)")
    plt.xlabel("Sizes (log scale)")
    if save_fig:
        plt.savefig(
            plots_dir/"sizes-breakdown.pdf",
            bbox_inches="tight",
            pad_inches=0
        )

# plot_sizes()
# %%
setup_matplotlib()
dataset = "hpc-hd-newspapers"
query = "reception"
save_fig = False
hot_cache = False
plots_dir = Path("/Users/mahadeva/Research/textreuse-pipeline-paper/figures")
sizes = load_table_sizes(dataset)
# %%
query_table_sizes = get_query_types_table_sizes(query, sizes)
running_times = get_running_times(query, dataset, hot_cache=hot_cache)
# %%
running_times = running_times.merge(
    query_table_sizes, on=["TABLE_SCHEMA", "query_type"]
)
# %%
# Total cost is storing data for 1hr and running query
running_times["total_cost"] = (
    running_times["processing_cost"] + running_times["storage_cost"]
)
# %%
if dataset == "hpc-hd-newspapers" and query == "quote":
    _df = running_times.query("query_dists_id<7")
elif dataset == "hpc-hd" and query == "quote":
    _df = running_times.query("query_dists_id<10")
elif dataset == "hpc-hd" and query == "reception":
    _df = running_times.query("query_dists_id<9")
elif dataset == "hpc-hd-newspapers" and query == "reception":
    _df = running_times.query("query_dists_id<9")
else:
    _df = running_times
_df = remap_df(_df)
# %%
hue = _df[["query_type", "schema"]].apply(
    lambda row: f"{row.query_type} | {row.schema}", axis=1
)
hue = hue.sort_values()
hue.name = "Normalization | Framework"
hue_color = {
    r"$\texttt{Denorm}$ | $\texttt{Aria}$":"tab:blue",
    r"$\texttt{Denorm}$ | $\texttt{Columnstore}$":"tab:blue",
    r"$\texttt{Denorm}$ | $\texttt{Spark}$":"tab:blue",
    r"$\texttt{Intermediate}$ | $\texttt{Aria}$":"tab:orange",
    r"$\texttt{Intermediate}$ | $\texttt{Columnstore}$":"tab:orange",
    r"$\texttt{Intermediate}$ | $\texttt{Spark}$":"tab:orange",
    r"$\texttt{Standard}$ | $\texttt{Aria}$":"tab:green",
    r"$\texttt{Standard}$ | $\texttt{Columnstore}$":"tab:green",
    r"$\texttt{Standard}$ | $\texttt{Spark}$":"tab:green"
    }
# %%
sns.pointplot(
    data=_df,
    x="total_size",
    y="duration",
    hue=hue,
    hue_order=hue.unique(),
    native_scale=True,
    log_scale=[2, False],
)
plt.yscale("log")
ticks = [s for s in _df.total_size.unique()]
labels = [sizeof_fmt(s) for s in ticks]
plt.xticks(labels=labels, ticks=ticks, rotation=90, minor=False)
plt.legend(bbox_to_anchor=(1, 0.5), loc="center left", title=hue.name)
plt.xlabel("Disk Size Used (Tables + Indexes)")
plt.ylabel("Query Duration (in sec)")
plt.title(f"{dataset.title()} dataset and {query.title()} use-case")
if save_fig:
    plt.savefig(plots_dir / f"{dataset}-{query}-storage.pdf")
# %%
setup_matplotlib(dpi_scale=3)
figsize = np.array(set_size(width=columnwidth, subplots=(1, 1)))
# figsize[1] *= 1.8
fig, ax = plt.subplots(1, 1, figsize=figsize)
sns.pointplot(
    data=_df,
    ax=ax,
    x="storage_cost",
    y="processing_cost",
    hue=hue,
    palette=hue_color,
    hue_order=hue.unique(),
    native_scale=True,
    log_scale=[True, True],
    legend=True,
    markersize=3,
    markers=["o","s","D","o",'s',"D",'o',"s",'D'],
    err_kws={"linewidth":1.5}
)
l = ax.get_legend_handles_labels()
ax.get_legend().remove()
# plt.legend(bbox_to_anchor=(1, 0.5), loc="center left", title=hue.name)
ax.set_xlabel("Storage Costs (in BU/hr)")
ax.set_ylabel("Query Execution Cost (in BU)")
# plt.title(f"{dataset.title()} dataset and {query.title()} use-case")
if save_fig:
    plt.savefig(plots_dir / f"{dataset}-{query}-costs.pdf",bbox_inches="tight",pad_inches=0)

figsize = np.array(set_size(width=fullwidth, subplots=(1, 1)))
figl, axl = plt.subplots(figsize=figsize)
axl.axis(False)
legend = axl.legend(
    *l,
    loc="center",
    bbox_to_anchor=(0.5, 0.5),
    ncols=3,
    title="Normalization Level | Framework",
    frameon=False,
)
fig = legend.figure
fig.canvas.draw()
bbox = legend.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
if save_fig:
    fig.savefig(plots_dir / "legend.pdf", bbox_inches=bbox)
# %%
sns.lineplot(data=_df, x="total_cost", y="duration", hue=hue, hue_order=hue.unique())
plt.yscale("log")
plt.xscale("log")
plt.xlabel("Total Costs in BU (1hr Storage + Query Execution)")
plt.ylabel("Query Latency (in sec)")
plt.legend(bbox_to_anchor=(1, 0.5), loc="center left", title=hue.name)
# plt.title(f"{dataset.title()} dataset and {query.title()} use-case")
if save_fig:
    plt.savefig(plots_dir / f"{dataset}-{query}-total-costs.pdf")
# %%
# Plotting Query Duration
setup_matplotlib()
figsize = np.array(set_size(width=fullwidth, subplots=(1, 3)))
# figsize[1] *= 1.8
fig, axes = plt.subplots(1, 3, figsize=figsize, sharey=True,sharex=True)
rm_df = remap_df(running_times)
for i, ((schema, schema_name), ax) in enumerate(zip(SCHEMA_TYPE_MAP.items(), axes)):
    tmp_df = rm_df[rm_df.schema == schema_name]
    ax = sns.barplot(
        data=tmp_df,
        x="query_dists_id",
        y="duration",
        hue="query_type",
        ax=ax,
        legend=True,
        err_kws={"linewidth":1}
    )
    ax.set_yscale("log")
    ax.set_xlabel("Workload")
    ax.set_ylabel("Query Latency")
    if i == 0:
        sns.move_legend(
            ax,
            bbox_to_anchor=(0.5, -0.1),
            bbox_transform=fig.transFigure,
            loc="upper center",
            ncols=3,
            title="",
            frameon=False,
        )
    else:
        ax.get_legend().remove()
    ax.set_title(schema_name)
fig.subplots_adjust(wspace=0.08, top=1, bottom=0.1, right=0.95, left=0.1)
if save_fig and not hot_cache:
    plt.savefig(
        plots_dir / f"{dataset}-{query}-duration.pdf", bbox_inches="tight", pad_inches=0
    )
# fig.tight_layout()
# %%
if hot_cache:
    _tmp = running_times[running_times.TABLE_SCHEMA == f"{dataset}-rowstore"]
    _tmp = remap_df(_tmp)
    setup_matplotlib()
    figsize = np.array(set_size(width=columnwidth, subplots=(1, 1)))
    # figsize[1] *= 1.8
    fig, ax = plt.subplots(1, 1, figsize=figsize, sharey=True,sharex=True)
    ax = sns.barplot(data=_tmp, x="query_dists_id", y="duration", hue="query_type",ax=ax)
    ax.set_yscale("log")
    ax.set_ylabel("Query Latency")
    ax.set_xlabel("Workload")
    ax.set_title(SCHEMA_TYPE_MAP["rowstore"])
    ax.legend(title="")
    # plt.gca().get_legend().remove()
    # sns.move_legend(
    #     ax,
    #     bbox_to_anchor=(0.5, -0.01),
    #     bbox_transform=fig.transFigure,
    #     loc="upper center",
    #     ncols=3,
    #     title="",
    # )
    if save_fig:
        plt.savefig(
            plots_dir / f"{dataset}-{query}-hot-cache-duration.pdf", bbox_inches="tight",pad_inches=0
        )


# %%
hpc_hd_stats = quote_analysis.get_statistics("hpc-hd", threshold=0)
hpc_hd_samples = quote_analysis.get_samples("hpc-hd")
hpc_hd_samples = hpc_hd_samples.merge(hpc_hd_stats, on=["edition_id", "ground_truth"])
# %%
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(5, 10))
loglog_hist(
    hpc_hd_stats.sum_n_works,
    ax=ax2,
    **{"alpha": 0.5, "ec": None, "label": "sum_n_works distribution"},
)
prop_cycle = plt.rcParams["axes.prop_cycle"]
colors = prop_cycle.by_key()["color"]

for (bucket, sample), color in zip(
    enumerate(hpc_hd_samples.sum_n_works.values), colors
):
    # color = next(ax2._get_lines.prop_cycler)["color"]
    ax2.axvline(sample, color=color, linestyle="-", label=f"Bucket {bucket} sample")
ax2.legend(loc="center left", bbox_to_anchor=(1, 0.5))
ax2.set_xlabel("sum_n_works")
ax2.set_ylabel("frequency")
ax2.set_title("Quote Query Worload Distribution")

loglog_hist(
    hpc_hd_stats.ground_truth,
    ax=ax1,
    **{"alpha": 0.5, "ec": None, "label": "ground_truth distribution"},
)
ax1.axvline(100, color="black", linestyle="--", label="Threshold")
ax1.legend(loc="center left", bbox_to_anchor=(1, 0.5))
ax1.set_xlabel("ground_truth")
ax1.set_ylabel("frequency")
ax1.set_title("Query Ground Truth Distribution")

fig.suptitle("HPC-HD dataset")
# %%
setup_matplotlib()
figsize = np.array(set_size(columnwidth,subplots=(2,2)))
fig, (ax,leg_ax) = plt.subplots(1, 2, figsize=figsize,width_ratios=[1,0.45])
loglog_hist(
    hpc_hd_stats.sum_n_works,
    ax=ax,
    **{"alpha": 0.5, "ec": None, "label": "Distribution"},
)
prop_cycle = plt.rcParams["axes.prop_cycle"]
colors = prop_cycle.by_key()["color"]

for (bucket, sample), color in zip(
    enumerate(hpc_hd_samples.sum_n_works.values), colors
):
    # color = next(ax._get_lines.prop_cycler)["color"]
    ax.axvline(sample, color=color, linestyle="-", label=f"Bucket {bucket}")
leg = ax.get_legend_handles_labels()
# ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
leg_ax.legend(*leg,borderaxespad=0)
leg_ax.axis('off')
ax.set_xlabel(r"$\texttt{sum_n_reuses}$")
ax.set_ylabel("Frequency")
fig.subplots_adjust(right=1,wspace=0.1)
if save_fig:
    fig.savefig(plots_dir / "quotes-hpc-hd-query-workload.pdf", bbox_inches="tight",pad_inches=0)
# %%
hpc_hd_reception_stats = pd.read_csv(
    project_root / "data" / f"hpc-hd-num-reception-edges.csv"
)
hpc_hd_query_dists = reception_analysis.get_query_dists(hpc_hd_reception_stats)
figsize = np.array(set_size(columnwidth,subplots=(2,2)))
fig, (ax,leg_ax) = plt.subplots(1, 2, figsize=figsize, width_ratios=[1,0.45])
loglog_hist(
    hpc_hd_reception_stats.num_reception_edges,
    ax=ax,
    **{
        "facecolor": "None",
        "ec": "black",
        "label": "Distribution",
        "histtype": "step",
        "linewidth": 2,
    },
)
prop_cycle = plt.rcParams["axes.prop_cycle"]
colors = prop_cycle.by_key()["color"]

for (bucket, low, high), color in zip(hpc_hd_query_dists.itertuples(), colors):
    # color = next(ax._get_lines.prop_cycler)["color"]
    # ax.axvline(low, color=color, linestyle="-",
    # label=f"Bucket {bucket}")
    # ax.axvline(high, color=color, linestyle="-",)
    # label=f"Bucket {bucket}")
    ax.axvspan(low, high, color=color, zorder=0, alpha=0.3, label=f"Bucket {bucket}")
leg = ax.get_legend_handles_labels()
leg_ax.legend(*leg,borderaxespad=0)
leg_ax.axis('off')
ax.set_xlabel("Number of reception edges")
ax.set_ylabel("Frequency")
fig.subplots_adjust(right=1,wspace=0.05)
if save_fig:
    fig.savefig(
        plots_dir/"reception-hpc-hd-query-workload.pdf",
        bbox_inches="tight", pad_inches = 0
    )
# fig.suptitle("HPC-HD dataset")
# %%


dataset = "hpc-hd"
query = "reception"
data_dir = project_root / "data"
df1 = pd.read_csv(data_dir / "reception-queries-results-1.csv")
df1["num_runs"] = 1
df2 = pd.read_csv(data_dir / "double-reception-queries-results-rowstore.csv")
df2["num_runs"] = 2
df = pd.concat([df1, df2])
df = df[df.database.isin([dataset, dataset + "-columnstore", dataset + "-spark"])]
samples = pd.read_csv(data_dir / f"{dataset}-samples.csv")
df = df.merge(samples, left_on="doc_id", right_on="manifestation_id")
df = df.rename(columns={"database": "TABLE_SCHEMA"})
df["TABLE_SCHEMA"] = df.TABLE_SCHEMA.apply(
    lambda s: s + "-rowstore" if dataset == s else s
)
df["processing_cost"] = df.apply(
    lambda row: find_processing_cost(row.TABLE_SCHEMA, row.duration), axis=1
)
# %%



#%% 
## Checking results from hot cache 
sizes = load_table_sizes(dataset)
running_times = df
query_table_sizes = get_query_types_table_sizes(query, sizes)
running_times = running_times.merge(
    query_table_sizes, on=["TABLE_SCHEMA", "query_type"]
)
running_times["total_cost"] = (
    running_times["processing_cost"] + running_times["storage_cost"]
)
# %%
_df = running_times
hue = _df[["num_runs", "query_type", "TABLE_SCHEMA"]].apply(
    lambda row: f"{row.num_runs}, {row.query_type}, {row.TABLE_SCHEMA}", axis=1
)
hue.name = "num_runs, query_type, TABLE_SCHEMA"
# %%
sns.pointplot(
    data=_df,
    x="total_size",
    y="duration",
    hue=hue,
    native_scale=True,
    log_scale=[2, False],
)
plt.yscale("log")
ticks = [s for s in _df.total_size.unique()]
labels = [sizeof_fmt(s) for s in ticks]
plt.xticks(labels=labels, ticks=ticks, rotation=90, minor=False)
plt.legend(bbox_to_anchor=(1, 0.5), loc="center left", title=hue.name)
plt.xlabel("Disk Size Used (Tables + Indexes)")
plt.ylabel("Query Duration (in sec)")
plt.title(f"{dataset.title()} dataset and {query.title()} use-case")
# %%
sns.catplot(
    data=running_times,
    col="query_type",
    x="query_dists_id",
    y="duration",
    hue="num_runs",
    kind="bar",
)
plt.yscale("log")
# %%
sns.lineplot(data=_df, x="total_cost", y="duration", hue=hue)
plt.yscale("log")
plt.xscale("log")
plt.xlabel("Total Costs in BU (1hr Storage + Query Processing costs)")
plt.ylabel("Query Duration (in sec)")
plt.legend(bbox_to_anchor=(1, 0.5), loc="center left", title=hue.name)
plt.title(f"{dataset.title()} dataset and {query.title()} use-case")
# %%
sns.pointplot(
    data=_df,
    x="storage_cost",
    y="processing_cost",
    hue=hue,
    native_scale=True,
    log_scale=[True, True],
)
plt.legend(bbox_to_anchor=(1, 0.5), loc="center left", title=hue.name)
plt.xlabel("Storage Costs (in BU/hr)")
plt.ylabel("Query Processing Cost (in BU)")
plt.title(f"{dataset.title()} dataset and {query.title()} use-case")
# %%
