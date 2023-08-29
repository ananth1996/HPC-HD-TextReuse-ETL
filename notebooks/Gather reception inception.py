# %%
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

# %%
data_dir = project_root/"data"

# %%
tables = [["textreuse_ids",processed_bucket,False],
    ["manifestation_ids",processed_bucket,False],
    ["edition_ids",processed_bucket,False],
    ["work_ids",processed_bucket,False],
    # ["actor_ids",processed_bucket,False],
    ["textreuse_work_mapping",processed_bucket,False],
    ["textreuse_edition_mapping",processed_bucket,False],
    ["work_mapping",processed_bucket,False],
    ["edition_mapping",processed_bucket,False],
    ["edition_publication_date",processed_bucket,False],
    ["work_earliest_publication_date",processed_bucket,False],
    ["textreuse_earliest_publication_date",processed_bucket,False],
    ["newspapers_core",raw_bucket,False],
    # ["edition_authors",processed_bucket,False],
    ["defrag_pieces",processed_bucket,False],
    ["defrag_textreuses",processed_bucket,False],
    ["clustered_defrag_pieces",processed_bucket,False],
    ["earliest_textreuse_by_cluster",processed_bucket,False],
    ["earliest_work_and_pieces_by_cluster",processed_bucket,False],
    ["reception_edges_denorm",denorm_bucket,False]]

for table,bucket,cache in tqdm(tables):
    get_s3(table,bucket,cache=cache)



# %%
work_ids = [
    # "52050-treatise of human nature",
    # "6012-essays moral and political",
    # "36869-grumbling hive or knaves turnd honest",
    # "7060-leviathan",
    # "19472-some fables after easie and familiar method of monsieur de la Fontaine",
    # "2736-modest defence of publick stews",
    # "36871-mischiefs that ought justly to be apprehended from whig-government",
    # "67090-an enquiry into causes of frequent executions at tyburn and proposal for some regulations concerning felons in prison and good effects to be expected from them",
    # "67092-free thoughts with practical applications on following subjects viz i",
    # "67093-pamphleteers satyr",
    # "67094-typhon or wars between gods and giants burlesque poem in imitation of comical mons",
    # "67095-wishes to godson with other miscellany poems",
    # "137686-a letter from gentleman to his friend in edinburgh containing some observations on specimen of principles said to be maintaind in book lately publishd intituled treatise of human nature",
    # "24551-political discourses by david hume",
    # "52051-a true account of behaviour and conduct of archibald stewart lord provost of edinburgh in letter to friend",
    # "7831-dialogues concerning natural religion",
    "7832-two essays",
    "970-essays and treatises on several subjects",
]

newspapers = ["The Female Tatler [A. Baldwin]"]
# %%
old_reception_sql = text("""
SELECT COUNT(*) FROM textreuses_2d_a tda 
INNER JOIN idmap_a ia ON tda.t1_id = ia.t_id
WHERE work_id = :work_id
""")

new_reception_sql = text("""
SELECT 
    COUNT(*)
    FROM reception_edges_denorm re 
    INNER JOIN textreuse_work_mapping src_twm ON src_twm.trs_id = re.src_trs_id 
    INNER JOIN work_ids src_work ON src_twm.work_id_i = src_work.work_id_i
    WHERE work_id = :work_id
""")
                         
spark_reception_edges= """
SELECT /*+ BROADCAST(src_twm) BROADCAST(src_work)*/
    re.*
    FROM reception_edges_denorm re 
    INNER JOIN textreuse_work_mapping src_twm ON src_twm.trs_id = re.src_trs_id 
    INNER JOIN work_ids src_work ON src_twm.work_id_i = src_work.work_id_i
    WHERE work_id = {work_id!r}
"""

spark_reception_csv_query = """
SELECT
    /*+ BROADCAST(src_tem) BROADCAST(src_ed) BROADCAST(src_ti) BROADCAST(src_epd) BROADCAST(dst_ti) BROADCAST(dst_tem) BROADCAST(dst_ed) BROADCAST(dst_twm) BROADCAST(dst_work)    */
    src_ti.manifestation_id AS src_manifestation_id,
    src_ed.edition_id AS src_edition_id,
    year(src_epd.publication_date) AS src_publication_year,
    e.src_trs_id,
    e.src_trs_start,
    e.src_trs_end,
    
    dst_ti.manifestation_id AS dst_manifestation_id,
    dst_ed.edition_id AS dst_edition_id,
    dst_work.work_id AS dst_work_id,
    year(dst_epd.publication_date) AS dst_publication_year,
    e.dst_trs_id,
    e.dst_trs_start,
    e.dst_trs_end
    
    FROM _reception_edges e
    INNER JOIN textreuse_ids src_ti ON src_ti.trs_id = e.src_trs_id
    INNER JOIN textreuse_edition_mapping src_tem ON src_tem.trs_id = e.src_trs_id
    INNER JOIN edition_ids src_ed ON src_ed.edition_id_i = src_tem.edition_id_i
    INNER JOIN edition_publication_date src_epd ON src_epd.edition_id_i = src_ed.edition_id_i
    
    INNER JOIN textreuse_ids dst_ti ON dst_ti.trs_id = e.dst_trs_id
    INNER JOIN textreuse_edition_mapping dst_tem ON dst_tem.trs_id = e.dst_trs_id
    INNER JOIN edition_ids dst_ed ON dst_ed.edition_id_i = dst_tem.edition_id_i
    INNER JOIN textreuse_work_mapping dst_twm ON dst_twm.trs_id = e.dst_trs_id
    INNER JOIN work_ids dst_work ON dst_work.work_id_i = dst_twm.work_id_i
    INNER JOIN edition_publication_date dst_epd ON dst_epd.edition_id_i = dst_ed.edition_id_i
"""

old_inception_sql = text("""
SELECT COUNT(*) FROM textreuses_2d_a tda 
INNER JOIN idmap_a ia ON tda.t2_id = ia.t_id
WHERE work_id = :work_id
""")

new_inception_sql = text("""
SELECT 
    COUNT(*)
    FROM reception_edges_denorm re 
    INNER JOIN textreuse_work_mapping dst_twm ON dst_twm.trs_id = re.dst_trs_id 
    INNER JOIN work_ids dst_work ON dst_twm.work_id_i = dst_work.work_id_i
    WHERE work_id = :work_id
""")

spark_inception_edges = """
        SELECT /*+ BROADCAST(dst_twm) BROADCAST(dst_work)*/
        re.*
        FROM reception_edges_denorm re 
        INNER JOIN textreuse_work_mapping dst_twm ON dst_twm.trs_id = re.dst_trs_id 
        INNER JOIN work_ids dst_work ON dst_twm.work_id_i = dst_work.work_id_i
        WHERE work_id = {work_id!r}"""
      
spark_inception_csv_query= """
    SELECT
    /*+ BROADCAST(src_tem) BROADCAST(src_ed) BROADCAST(src_ti) BROADCAST(src_epd) BROADCAST(dst_ti) BROADCAST(dst_tem) BROADCAST(dst_ed) BROADCAST(src_twm) BROADCAST(src_work)    */
    src_ti.manifestation_id AS src_manifestation_id,
    src_ed.edition_id AS src_edition_id,
    src_work.work_id AS src_work_id,
    year(src_epd.publication_date) AS src_publication_year,
    e.src_trs_id,
    e.src_trs_start,
    e.src_trs_end,
    
    dst_ti.manifestation_id AS dst_manifestation_id,
    dst_ed.edition_id AS dst_edition_id,
    year(dst_epd.publication_date) AS dst_publication_year,
    e.dst_trs_id,
    e.dst_trs_start,
    e.dst_trs_end
    
    FROM _inception_edges e
    INNER JOIN textreuse_ids src_ti ON src_ti.trs_id = e.src_trs_id
    INNER JOIN textreuse_edition_mapping src_tem ON src_tem.trs_id = e.src_trs_id
    INNER JOIN edition_ids src_ed ON src_ed.edition_id_i = src_tem.edition_id_i
    INNER JOIN edition_publication_date src_epd ON src_epd.edition_id_i = src_ed.edition_id_i
    INNER JOIN textreuse_work_mapping src_twm ON src_twm.trs_id = e.src_trs_id
    INNER JOIN work_ids src_work ON src_work.work_id_i = src_twm.work_id_i
    
    INNER JOIN textreuse_ids dst_ti ON dst_ti.trs_id = e.dst_trs_id
    INNER JOIN textreuse_edition_mapping dst_tem ON dst_tem.trs_id = e.dst_trs_id
    INNER JOIN edition_ids dst_ed ON dst_ed.edition_id_i = dst_tem.edition_id_i
    INNER JOIN edition_publication_date dst_epd ON dst_epd.edition_id_i = dst_ed.edition_id_i
"""

newspaper_reception_edges = """
SELECT 
/*+ BROADCAST(src_ti) BROADCAST(nc)*/
re.* FROM reception_edges_denorm re
INNER JOIN textreuse_ids src_ti ON src_ti.trs_id = re.src_trs_id
INNER JOIN newspapers_core nc ON src_ti.manifestation_id = nc.article_id
WHERE newspaper_title = {newspaper_title!r}
"""

newspaper_inception_edges = """
SELECT 
/*+ BROADCAST(dst_ti) BROADCAST(nc)*/
re.* FROM reception_edges_denorm re
INNER JOIN textreuse_ids dst_ti ON dst_ti.trs_id = re.dst_trs_id
INNER JOIN newspapers_core nc ON dst_ti.manifestation_id = nc.article_id
WHERE newspaper_title = {newspaper_title!r}
"""
#%%

def gather_newspaper_data(newspapers):
    for newspaper in newspapers:
        print(f"{newspaper=}")
        reception_edges = spark.sql(newspaper_reception_edges.format(newspaper_title=newspaper)).cache()
        reception_edge_count = reception_edges.count()
        reception_edges.createOrReplaceTempView("_reception_edges")
        reception_csv = spark.sql(spark_reception_csv_query).toPandas()
        reception_csv.to_csv(project_root/"data"/f"{newspaper} reception edges.csv",index=False)
        print(f"\t{reception_edge_count=}")
        print(f"\tReception CSV rows: {len(reception_csv)}")

        inception_edges = spark.sql(newspaper_inception_edges.format(newspaper_title=newspaper)).cache()
        inception_edge_count = inception_edges.count()
        inception_edges.createOrReplaceTempView("_inception_edges")
        inception_csv = spark.sql(spark_inception_csv_query).toPandas()
        inception_csv.to_csv(project_root/"data"/f"{newspaper} inception edges.csv",index=False)
        print(f"\t{inception_edge_count=}")
        print(f"\tinception CSV rows: {len(inception_csv)}")

#%%
def gather_inception_data(work_ids):
    old_db_conn=get_sqlalchemy_connect("OldMariadb")
    new_db_conn=get_sqlalchemy_connect("NewMariadb")

    for work_id in work_ids:
        
        old_count = old_db_conn.execute(old_inception_sql,parameters=dict(work_id=work_id)).fetchall()[0][0]
        new_count = new_db_conn.execute(new_inception_sql,parameters=dict(work_id=work_id)).fetchall()[0][0]
        df  = spark.sql(spark_inception_edges.format(work_id=work_id)).cache()
        edges_count = df.count()
        df.createOrReplaceTempView("_inception_edges")
        csv_df = spark.sql(spark_inception_csv_query)
        pdf = csv_df.toPandas()
        print(f"\n{work_id=}")
        print(f"Number of csv rows: {len(pdf)}")
        print(f"Number of inception edges in:")
        print(f"\tin new Data: {edges_count}")
        print(f"\tin updated tables: {new_count}")
        print(f"\tin old tables (includes reception edges): {old_count}")
        pdf.to_csv(project_root/"data"/f"{work_id[:50]} inception edges.csv",index=False)


    old_db_conn.close()
    new_db_conn.close()

def gather_reception_data(work_ids):
    old_db_conn=get_sqlalchemy_connect("OldMariadb")
    new_db_conn=get_sqlalchemy_connect("NewMariadb")

    for work_id in work_ids:
        df  = spark.sql(spark_reception_edges.format(work_id=work_id)).cache()
        edges_count = df.count()
        df.createOrReplaceTempView("_reception_edges")
        csv_df = spark.sql(spark_reception_csv_query)
        pdf = csv_df.toPandas()
        old_count = old_db_conn.execute(old_reception_sql,parameters=dict(work_id=work_id)).fetchall()[0][0]
        new_count = new_db_conn.execute(new_reception_sql,parameters=dict(work_id=work_id)).fetchall()[0][0]
        print(f"\n{work_id=}")
        print(f"Number of csv rows: {len(pdf)}")
        print(f"Number of reception edges in:")
        print(f"\tin new Data: {edges_count}")
        print(f"\tin updated tables: {new_count}")
        print(f"\tin old tables (includes inception edges): {old_count}")
        pdf.to_csv(project_root/"data"/f"{work_id[:50]} reception edges.csv",index=False)


    old_db_conn.close()
    new_db_conn.close()