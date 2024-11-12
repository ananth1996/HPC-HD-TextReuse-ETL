# %%
from tqdm.autonotebook import trange, tqdm
from plot_utils import *
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import matplotlib.lines as mlines
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

# thesis 
columnwidth = 355.65944
fullwidth = 355.65944

QUERY_MAP = {"reception": "Reception", "quote": "Top Quotes"}
QUERY_MAP = {"reception": "Reception", "quote": "Top Quotes"}


def setup_matplotlib(dpi_scale=1, fontscale=1):
    plt.rcParams.update(
        {
            "text.usetex": True,
            "text.latex.preamble": r"\usepackage{lmodern}", 
            # 'text.latex.preamble' :r'\usepackage{amsmath} ',
            "font.size": 10.95 * fontscale,
            "font.family": "lmodern",
            "axes.titlesize": 10.95 * fontscale,
            "figure.dpi": 100 * dpi_scale,
            "xtick.labelsize": 10.95 * fontscale,
            "ytick.labelsize": 10.95 * fontscale,
            "legend.fontsize": 10.95 * fontscale,
            "legend.fontsize": 10.95 * fontscale,
            "figure.titlesize": 10.95 * fontscale,
            "axes.labelsize": 10.95 * fontscale,
            "figure.labelsize": 10.95 * fontscale,
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


def get_running_times(
    query, dataset, data_dir=project_root / "data", hot_cache=False, verbose=False
):
    if query == "reception":
        dfs = []
        for file in data_dir.glob("reception-queries-results*"):
            if verbose:
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
            if verbose:
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
    "hpc-hd": r"$\textsc{Basic}$",
    "hpc-hd-newspapers": r"$\textsc{Extended}$",
}
QUERY_TYPE_MAP = {
    "standard": r"$\texttt{Standard}$",
    "intermediate": r"$\texttt{Intermediate}$",
    "denorm": r"$\texttt{Denormalized}$",
}
SCHEMA_TYPE_MAP = OrderedDict(
    {
        "spark": r"$\texttt{Spark}$",
        "rowstore": r"$\texttt{Aria}$",
        "columnstore": r"$\texttt{Columnstore}$",
    }
)


def remap_df(df):
    return df.replace(
        {
            "schema": SCHEMA_TYPE_MAP,
            "dataset": DATASET_MAP,
            "query_type": QUERY_TYPE_MAP,
        }
    )


# %%
def plot_sizes(save_fig=False):
    setup_matplotlib()
    _dfs = []
    for dataset in ["hpc-hd", "hpc-hd-newspapers"]:
        sizes = load_table_sizes(dataset)
        sizes["schema"] = sizes.TABLE_SCHEMA.str.rsplit("-", n=1).str[-1]
        sizes["dataset"] = sizes.TABLE_SCHEMA.str.rsplit("-", n=1).str[0]
        _dfs.append(sizes)

    # necessary_tables = list(
    #     {
    #         table
    #         for _, materialisations in QUERY_TABLES_MAP.items()
    #         for _, tables in materialisations.items()
    #         for table in tables
    #     }
    # )
    necessary_tables = [
        "reception_edges_denorm",
        "source_piece_statistics_denorm",
        "non_source_pieces",
    ]
    sizes = pd.concat(_dfs)
    _sizes = sizes[sizes.TABLE_NAME.isin(necessary_tables)]
    _sizes = remap_df(_sizes)
    schema_order = list(SCHEMA_TYPE_MAP.values())
    # _sizes_gb = _sizes.groupby(["dataset", "schema"]).total_size.sum().reset_index()
    figsize = np.array(set_size(columnwidth, subplots=(1.5, 2)))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize, sharey=True)
    table_rename = {
        "non_source_pieces": r"\texttt{destination_pieces}",
        "reception_edges_denorm": r"\texttt{reception_edges}",
        "source_piece_statistics_denorm": r"\texttt{source_piece_metrics}",
    }
    hue = {
        r"\texttt{destination_pieces}": "tab:red",
        r"\texttt{reception_edges}": "tab:purple",
        r"\texttt{source_piece_metrics}": "tab:olive",
    }
    _sizes = _sizes.replace({"TABLE_NAME": table_rename})
    for dataset, ax in zip(DATASET_MAP.values(), [ax1, ax2]):
        sns.barplot(
            data=_sizes[_sizes.dataset == dataset],
            y="schema",
            x="total_size",
            hue="TABLE_NAME",
            palette=hue,
            order=schema_order,
            ax=ax,
            orient="h",
        )
        ax.xaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
        ax.set_title(dataset)
        ax.set_ylabel("")
        ax.set_xlabel("")
        ax.get_legend().remove()
    fig.supxlabel("Data Storage Size", y=-0.2)
    fig.subplots_adjust(wspace=0.1)
    plt.legend(
        bbox_to_anchor=(0.5, 1.2),
        # loc="center right",
        loc="upper center",ncol=3,
        columnspacing=1,
        bbox_transform=fig.transFigure,
        borderaxespad=0,
        frameon=False,
    )
    # _sizes.groupby("schema").total_size.sum().plot(kind="bar")
    # # plt.xticks(rotation=90)
    # ax.set_ylabel(f"Total Data Size on Disk")
    # ax.set_xlabel("Framework")
    # fig.subplots_adjust(right=1)
    # if save_fig:
    #     plt.savefig(plots_dir / "sizes.pdf", bbox_inches="tight", pad_inches=0)

    # plt.figure(figsize=(10, 10))
    # sns.barplot(
    #     data=_sizes, y="TABLE_NAME", x="total_size", hue="TABLE_SCHEMA", orient="h"
    # )
    # plt.xscale("log", base=2)
    # plt.gca().xaxis.set_major_formatter(tkr.FuncFormatter(sizeof_fmt))
    # # plt.xticks(rotation=90)
    # plt.legend(bbox_to_anchor=(1, 0.5), loc="upper left")
    # plt.title(f"Table Sizes (Data + Indexes)")
    # plt.xlabel("Sizes (log scale)")
    # if save_fig:
    #     plt.savefig(
    #         plots_dir / "sizes-breakdown.pdf", bbox_inches="tight", pad_inches=0
    #     )


# plot_sizes()


def get_results_df(dataset, query, hot_cache=False):
    sizes = load_table_sizes(dataset)
    query_table_sizes = get_query_types_table_sizes(query, sizes)
    running_times = get_running_times(query, dataset, hot_cache=hot_cache)
    running_times = running_times.merge(
        query_table_sizes, on=["TABLE_SCHEMA", "query_type"]
    )
    # Total cost is storing data for 1hr and running query
    running_times["total_cost"] = (
        running_times["processing_cost"] + running_times["storage_cost"]
    )
    return running_times


def get_trade_off_dataframe(dataset, query,hot_cache=False):
    running_times = get_results_df(dataset, query,hot_cache=hot_cache)
    # if dataset == "hpc-hd-newspapers" and query == "quote":
    #     _df = running_times.query("query_dists_id<7")
    # elif dataset == "hpc-hd" and query == "quote":
    #     _df = running_times.query("query_dists_id<10")
    # elif dataset == "hpc-hd" and query == "reception":
    #     _df = running_times.query("query_dists_id<9")
    # elif dataset == "hpc-hd-newspapers" and query == "reception":
    #     _df = running_times.query("query_dists_id<9")
    # else:
    #     _df = running_times
    _df = running_times
    _df = remap_df(_df)
    return _df


def plot_latency_size_tradeoff(df):
    hue, palette = get_hue_and_palette(df)
    sns.pointplot(
        data=df,
        x="total_size",
        y="duration",
        hue=hue,
        hue_order=hue.unique(),
        native_scale=True,
        log_scale=[2, False],
    )
    plt.yscale("log")
    ticks = [s for s in df.total_size.unique()]
    labels = [sizeof_fmt(s) for s in ticks]
    plt.xticks(labels=labels, ticks=ticks, rotation=90, minor=False)
    plt.legend(bbox_to_anchor=(1, 0.5), loc="center left", title=hue.name)
    plt.xlabel("Disk Size Used (Tables + Indexes)")
    plt.ylabel("Query Latency (in sec)")
    plt.title(f"{dataset.title()} dataset and {query.title()} use-case")


def get_hue_and_palette(df):
    hue = df[["query_type", "schema"]].apply(
        lambda row: f"{row.query_type} | {row.schema}", axis=1
    )
    hue = hue.sort_values()
    hue.name = "Normalization | Framework"
    hue_color = {
        r"$\texttt{Denormalized}$ | $\texttt{Aria}$": "tab:blue",
        r"$\texttt{Denormalized}$ | $\texttt{Columnstore}$": "tab:blue",
        r"$\texttt{Denormalized}$ | $\texttt{Spark}$": "tab:blue",
        r"$\texttt{Intermediate}$ | $\texttt{Aria}$": "tab:orange",
        r"$\texttt{Intermediate}$ | $\texttt{Columnstore}$": "tab:orange",
        r"$\texttt{Intermediate}$ | $\texttt{Spark}$": "tab:orange",
        r"$\texttt{Standard}$ | $\texttt{Aria}$": "tab:green",
        r"$\texttt{Standard}$ | $\texttt{Columnstore}$": "tab:green",
        r"$\texttt{Standard}$ | $\texttt{Spark}$": "tab:green",
    }
    return hue, hue_color


def plot_cost_trade_off(df, ax=None):
    hue, palette = get_hue_and_palette(df)
    if ax is None:
        fig, ax = plt.subplots()
    sns.pointplot(
        data=df,
        ax=ax,
        x="estimated_storage_cost",
        y="estimated_processing_cost",
        hue=hue,
        palette=palette,
        hue_order=hue.unique(),
        native_scale=True,
        log_scale=[True, True],
        legend=True,
        markeredgecolor="black",
        markeredgewidth=0.5,
        markersize=5,
        markers=["o", "s", "D", "o", "s", "D", "o", "s", "D"],
        err_kws={"linewidth": 1.5},
    )
    l = ax.get_legend_handles_labels()
    ax.get_legend().remove()
    # return axis and legend handlers
    return ax, l


def plot_legend(legend_handlers):
    figsize = np.array(set_size(width=fullwidth, subplots=(1, 1)))
    figl, axl = plt.subplots(figsize=figsize)
    axl.axis(False)
    legend = axl.legend(
        *legend_handlers,
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


def is_pareto_efficient_dumb(costs):
    """
    Find the pareto-efficient points
    :param costs: An (n_points, n_costs) array
    :return: A (n_points, ) boolean array, indicating whether each point is Pareto efficient
    """
    is_efficient = np.ones(costs.shape[0], dtype = bool)
    for i, c in enumerate(costs):
        is_efficient[i] = np.all(np.any(costs[:i]>c, axis=1)) and np.all(np.any(costs[i+1:]>c, axis=1))
    return is_efficient

def plot_pareto_front(data,x,y,rescale=0.1,**kwargs):
    if "ax" in kwargs:
        ax = kwargs["ax"]
    else:
        ax = plt.gca()
    _data = data.copy()
    _data.loc[_data[x]==0,x]=np.nan
    _data.loc[_data[y]==0,y]=np.nan
    _data = _data.dropna()  
    pareto_front_mask = is_pareto_efficient_dumb(_data.values)
    _data = _data[pareto_front_mask]
    _data.loc[:,y] -= (_data.loc[:,y]*rescale) # move points 10% below and to the right 
    _data.loc[:,x] -= (_data.loc[:,x]*rescale) # move points 10% below and to the right 
    sns.lineplot(
        ax=ax,
        data=_data,
        x=x,
        y=y,
        linestyle="--",
        label="Pareto Front",
        color="black",
        legend=False,
        zorder=0,
        alpha=0.5
    )

def plot_cost_trade_off_grid(save_fig=False):
    setup_matplotlib()
    figsize = np.array(set_size(width=fullwidth, subplots=(1.8,2)))
    # fig,(leg_ax,axes) = plt.subplots(2,3,,figsize=figsize)
    fig = plt.figure(figsize=figsize)
    (plt_fig, leg_fig) = fig.subfigures(2, 1, height_ratios=[1,0.4], hspace=0.25)
    leg_ax1,leg_pareto, leg_ax2 = leg_fig.subplots(1, 3, width_ratios=[1,0.3,1])
    axes = plt_fig.subplots(1, 2)
    dataset_query = [
        ("reception", "hpc-hd-newspapers"),
        ("quote", "hpc-hd-newspapers"),
    ]
    analysis_module_map = {"reception":reception_analysis,"quote":quote_analysis}
    for ax, (query, dataset) in zip(axes, dataset_query):
        query_dists = analysis_module_map[query].get_query_dists(analysis_module_map[query].get_statistics(dataset))        
        df = get_trade_off_dataframe(dataset, query,hot_cache=True)
        df["query"] = query
        df = df.merge(query_dists,left_on="query_dists_id",right_index=True) 
        df = df.groupby(["dataset","query","schema","query_type","query_dists_id"]).mean(numeric_only=True)
        df["estimated_duration"] = df["duration"]*df["proportion"]
        df["estimated_storage_cost"] = df["storage_cost"]*df["proportion"]
        df["estimated_processing_cost"] = df["processing_cost"]*df["proportion"]
        df["estimated_total_cost"] = df["total_cost"]*df["proportion"] 
        df = df.groupby(level=[0,1,2,3])[["estimated_processing_cost","estimated_storage_cost"]].sum().reset_index()
        ax, legend = plot_cost_trade_off(df, ax=ax)
        plot_pareto_front(
            df,
            x="estimated_storage_cost",
            y="estimated_processing_cost",
            ax=ax,
            rescale=0,
        )
        ax.set_xlabel("")
        ax.set_ylabel("")
        # ax.set_title(f"{df.dataset.iloc[0]}")
        ax.set_title(f"{QUERY_MAP[query]} Task")

    plt_fig.supylabel("Expected Query\nExecution Cost (in BU)",ma="center")
    plt_fig.supxlabel("Storage Costs (in BU/hr)",y=0.01)

    circle = mlines.Line2D(
        [],
        [],
        markeredgecolor="black",
        markerfacecolor="none",
        marker="o",
        linestyle="None",
        markersize=10,
        label=SCHEMA_TYPE_MAP["rowstore"],
    )
    square = mlines.Line2D(
        [],
        [],
        markeredgecolor="black",
        markerfacecolor="none",
        marker="s",
        linestyle="None",
        markersize=10,
        label=SCHEMA_TYPE_MAP["columnstore"],
    )
    diamond = mlines.Line2D(
        [],
        [],
        markeredgecolor="black",
        markerfacecolor="none",
        marker="D",
        linestyle="None",
        markersize=10,
        label=SCHEMA_TYPE_MAP["spark"],
    )
    leg_ax1.axis("off")
    leg_ax1.legend(
        handles=[circle, square, diamond],
        title="Framework",
        ncol=1,
        loc="lower center",
        borderaxespad=0,
        frameon=False,
        # mode="expand",
        markerscale=0.75,
    )

    blue = mlines.Line2D(
        [],
        [],
        linestyle="none",
        label=QUERY_TYPE_MAP["denorm"],
        markeredgecolor="none",
        markerfacecolor="tab:blue",
        marker="o",
        markersize=10
    )

    orange = mlines.Line2D(
        [],
        [],
        linestyle="none",
        label=QUERY_TYPE_MAP["intermediate"],
        markersize=10,
        markeredgecolor="none",
        markerfacecolor="tab:orange",
        marker="o"
    )

    green = mlines.Line2D(
        [],
        [],
        linestyle="none",
        label=QUERY_TYPE_MAP["standard"],
        markeredgecolor="none",
        markerfacecolor="tab:green",
        marker="o",
        markersize=10,
    )

    leg_ax2.axis("off")
    leg_ax2.legend(
        handles=[blue, orange, green],
        title="Normalization Level",
        ncol=1,
        loc="lower center",
        borderaxespad=0,
        frameon=False,
        # mode="expand",
        markerscale=0.7,
    )

    leg_pareto.axis('off')

    pareto_line = mlines.Line2D(
        [],
        [],
        linestyle="--",
        alpha=0.5,
        color="black",  
        label="Pareto Front"
    )

    leg_pareto.legend(
        handles=[pareto_line],
        borderaxespad=0.1,
        frameon=False,
        loc="center",

    )
    leg_fig.subplots_adjust(right=0.95, top=0.8, left=0.01, bottom=0.1,wspace=0.5)
    plt_fig.subplots_adjust(bottom=0.25, left=0.18, right=0.99,wspace=0.22,top=0.9)
    if save_fig:
        fig.savefig(plots_dir / "trade-off-plot.pdf", bbox_inches="tight", pad_inches=0)


# %%
setup_matplotlib()
dataset = "hpc-hd"
query = "reception"
save_fig = True
hot_cache = False
plots_dir = Path("/Users/mahadeva/Research/phd-thesis/thesis/camera-ready-version/figs")
running_times = get_results_df(dataset, query, hot_cache=hot_cache)

# %%
df = get_trade_off_dataframe(dataset, query)
# %%
hue, palette = get_hue_and_palette(df)
# %%
plot_cost_trade_off_grid(save_fig)
# %%