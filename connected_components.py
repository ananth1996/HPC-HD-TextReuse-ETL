
#%%
from IPython.core.getipython import get_ipython
from IPython.core.interactiveshell import InteractiveShell
import typing
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    ip: InteractiveShell = typing.cast(InteractiveShell,get_ipython())
    ip.run_line_magic("load_ext", "autoreload")
    ip.run_line_magic("autoreload", "2")
else:
    notebook = False
from spark_utils import *
from pathlib import Path
if notebook:
    project_root = Path.cwd().resolve()
else:
    project_root = Path(__file__).parent.parent.resolve()

from pyspark.sql.functions import coalesce, col, explode, expr, isnull, lit, min
from pyspark.storagelevel import StorageLevel
from pyspark.sql import DataFrameWriter
#%%
def write_checkpoint(df: DataFrame, name: str, alter: Callable[[DataFrameWriter], DataFrameWriter] = lambda df: df):
  (alter(df.write)
  #  .bucketBy(256, "piece_id")
   # .sortBy("piece_id")
  ).saveAsTable(name, mode='overwrite', format='parquet', path=f's3a://{processed_bucket}/{name}.parquet',compression='zstd')

def read_checkpoint(name):
  return (
    spark
      .read
      .option("path", f"s3a://{processed_bucket}/{name}.parquet")
      .table(name)
  )
#%%
iter = 0

adjacency_list = read_checkpoint("adjacency_list")

if iter == 0:
  components = (adjacency_list
    #.select("piece_id")
    .withColumn("component_id", col("piece_id"))
    .withColumn("active", lit(True))
  )
  write_checkpoint(components, "components_0", lambda dfw: dfw.partitionBy("active"))

# %%
components = read_checkpoint(f"components_{iter%2}")#.select("piece_id","component_id","active")
total = components.count()
active_count = components.filter(col("active")==True).count()
# %%
from tqdm.auto import tqdm
spark.conf.set("spark.sql.adaptive.enabled", "false")
pbar = tqdm(initial=total-active_count, total=total, unit="piece", dynamic_ncols=True, unit_scale=True, unit_divisor=1000, smoothing=1)

while active_count > 0 and iter < 1000:
  print()
  pbar.n = total - active_count
  pbar.set_description(f"{iter}")
  projected_components = (components
    .filter(col("active") == True)
    #.join(adjacency_list, "piece_id")
    .select(explode("other_piece_ids").alias("piece_id"), col("component_id").alias("new_component_id"))
    .repartition("piece_id")
    .join(components, "piece_id")
    .filter(col("new_component_id") < col("component_id"))
    .groupBy("component_id")
    .agg(min("new_component_id").alias("new_component_id"))
  ).persist(StorageLevel.MEMORY_ONLY)
  while True:
    old_projected_components = projected_components
    projected_components = (projected_components
      .join(projected_components.select(col("new_component_id").alias("new_new_component_id"),col("component_id").alias("new_component_id")), "new_component_id", "left")
      .withColumn("changed",isnull("new_new_component_id")==False)
      .withColumn("new_component_id", coalesce("new_new_component_id","new_component_id"))
      .select("component_id","new_component_id","changed")
    ).persist(StorageLevel.MEMORY_ONLY)
    projected_components.count()
    stop = (

projected_components.filter(col("changed")==True).isEmpty())
    old_projected_components.unpersist()
    if stop:
      break


                                                
  components = (components
    .join(projected_components, "component_id" , "left")
    .withColumn("active", isnull("new_component_id")==False)
    .withColumn("component_id", coalesce("new_component_id","component_id"))
    .select("piece_id","component_id","active","other_piece_ids")
  )
  iter += 1
  write_checkpoint(components, f"components_{iter%2}", lambda dfw: dfw.partitionBy("active"))
  projected_components.unpersist()
  components = read_checkpoint(f"components_{iter%2}")
  active_count = (components
    .filter(col("active")==True)
    .count()
  )
pbar.set_description(f"{iter}: done")
pbar.close()
# %%
