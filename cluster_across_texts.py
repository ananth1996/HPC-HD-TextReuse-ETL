#%%
import typing
notebook = False
try:
    from IPython.core.getipython import get_ipython
    from IPython.core.interactiveshell import InteractiveShell
    if get_ipython() is not None and __name__ == "__main__":
        notebook = True
        ip: InteractiveShell = typing.cast(InteractiveShell,get_ipython())
        ip.run_line_magic("load_ext", "autoreload")
        ip.run_line_magic("autoreload", "2")
except ImportError:
    pass

#%%
from spark_utils import *
from pathlib import Path
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()

from pyspark.sql.functions import col, expr, rand, map_from_entries, explode, lit, isnull, collect_list
from pyspark.storagelevel import StorageLevel
from pyspark.sql import DataFrameWriter
#%%
def write_checkpoint(df: DataFrame, name: str, alter: Callable[[DataFrameWriter], DataFrameWriter] = lambda df: df):
  (alter(df.write)
#    .bucketBy(256, "piece_id")
#    .sortBy("piece_id")
  ).saveAsTable(name, mode='overwrite', format='parquet', path=f's3a://{processed_bucket}/{name}.parquet',compression='zstd')

def read_checkpoint(name):
  return (
    spark
      .read
      .option("path", f"s3a://{processed_bucket}/{name}.parquet")
      .table(name)
  )
#%%
build_adjacency_list = False
if build_adjacency_list:
  defrag_textreuses = spark.read.parquet(f"s3a://{processed_bucket}/defrag_textreuses.parquet")
  adjacency_list = (defrag_textreuses
    .select(col("piece1_id").alias("piece_id"), col("piece2_id").alias("other_piece_id"))
    .unionAll(
      defrag_textreuses
        .select(col("piece2_id").alias("piece_id"), col("piece1_id").alias("other_piece_id"))
    )
    .groupBy("piece_id")
    .agg(collect_list("other_piece_id").alias("other_piece_ids"))
  )
  (adjacency_list
     .write
     .bucketBy(256, "piece_id")
     .sortBy("piece_id")
     .saveAsTable("adjacency_list", mode='overwrite', format='parquet', path=f's3a://{processed_bucket}/adjacency_list.parquet',compression='zstd')
  )

#%%
iter = 745

adjacency_list = read_checkpoint("adjacency_list")
if iter == 0:
  clusters_counts = (adjacency_list
    .withColumn("cluster_counts", map_from_entries(expr("transform(other_piece_ids, other_piece_id -> (other_piece_id,1))")))
    .select("piece_id", "cluster_counts")
    .withColumn("cluster_id", col("piece_id"))
    .withColumn("active", lit(True))
  )
else:
  clusters_counts = read_checkpoint(f"clusters_counts_{iter%2}")
  # clusters_counts = spark.read.parquet(f's3a://{processed_bucket}/clusters_counts_{iter%2}.parquet')

# %%

total = clusters_counts.count()
if iter == 0:
    active_count = total
else:
    active_count = clusters_counts.filter(col("active")==True).count()

# %%
from tqdm.auto import tqdm

#spark.conf.set("spark.sql.adaptive.enabled", "false")

pbar = tqdm(initial=total-active_count, total=total, unit="piece", dynamic_ncols=True, unit_scale=True, unit_divisor=1000, smoothing=1)

while active_count > 0 and iter < 1000:
  pbar.n = total - active_count
  pbar.set_description(f"{iter}")
  print()
  new_cluster_updates = (clusters_counts
    .filter(col("active")==True)
    .select(col("piece_id"), col("cluster_id").alias("old_cluster_id"), col("cluster_counts"))
    .withColumn("new_cluster_id",
      expr("""
          aggregate(
            map_keys(cluster_counts), 
            (bigint(-1) as cluster_id, bigint(-1) as count, bigint(-1) as same_count),
            (acc, y) ->
              IF(
                acc.count <= cluster_counts[y],
                IF(
                  acc.count < cluster_counts[y],
                  (y as cluster_id, cluster_counts[y] as count, 1 as same_count),
                  IF(
                    rand()<1/(acc.same_count + 1),
                    (y as cluster_id, acc.count as count, acc.same_count + 1 as same_count),
                    (acc.cluster_id as cluster_id, acc.count as count, acc.same_count + 1 as same_count)
                  )
                ),
                acc
              ),
             acc -> (acc.cluster_id as cluster_id, acc.same_count > 1 as same_count_greater_than_1)
          )
      """))
    .select(col("piece_id"), col("old_cluster_id"), col("new_cluster_id.cluster_id").alias("new_cluster_id"), col("new_cluster_id.same_count_greater_than_1"))
    .withColumn("do_update", (col("old_cluster_id") != col("new_cluster_id")) & (rand() <= 0.9))
    .filter((col("same_count_greater_than_1") == True) | (col("do_update") == True))
  ).persist(StorageLevel.MEMORY_AND_DISK)
  if active_count > 512000000:
      partition_count = 4096
  else:
      partition_count = 256
  projected_count_updates = (new_cluster_updates
    .filter(col("do_update")==True)                
    .join(adjacency_list,"piece_id")
    .select(explode("other_piece_ids").alias("piece_id"), col("old_cluster_id"), col("new_cluster_id"))
    .repartition(partition_count, "piece_id")
    .groupBy("piece_id","old_cluster_id","new_cluster_id")
    .count()
    .groupBy("piece_id")
    .agg(expr("""
      aggregate(
        collect_list(struct(old_cluster_id, new_cluster_id, count)),
        cast(map() AS MAP<BIGINT, BIGINT>),
        (acc, updates) -> map_concat(
          map(
            updates.old_cluster_id, coalesce(acc[updates.old_cluster_id], 0) - updates.count,
            updates.new_cluster_id, coalesce(acc[updates.new_cluster_id], 0) + updates.count
          ),
          map_filter(acc, (k,v) -> k != updates.old_cluster_id and k != updates.new_cluster_id)
        )
      )
    """).alias("count_updates"))
  )
  clusters_counts = (clusters_counts
    .join(
      new_cluster_updates.select("piece_id", "do_update", "new_cluster_id", "same_count_greater_than_1"),
      "piece_id", "left")
    .withColumn("cluster_id", expr("IF((isnull(do_update)==false) and (do_update==true), new_cluster_id, cluster_id)"))
    .join(projected_count_updates, "piece_id", "left")
    .withColumn("cluster_counts",
      expr("""
        IF(isnull(count_updates),
          cluster_counts,
          map_filter(
            map_zip_with(cluster_counts,count_updates, (k,v1,v2) -> coalesce(v1,0) + coalesce(v2,0)),
            (k,v) -> v != 0
          )
        )
      """))
    .withColumn("active", ((isnull("same_count_greater_than_1") == False) & (col("same_count_greater_than_1") == True)) | (isnull("count_updates") == False))
    .select(col("piece_id"),col("cluster_id"),col("cluster_counts"),col("active"))
  )
  iter += 1
  write_checkpoint(clusters_counts, f"clusters_counts_{iter%2}", lambda dfw: dfw.partitionBy("active"))
  new_cluster_updates.unpersist()
  clusters_counts = read_checkpoint(f"clusters_counts_{iter%2}")
  active_count = (clusters_counts
    .filter(col("active")==True)
    .count()
  )
pbar.set_description(f"{iter}: done")
pbar.close()
# %%
