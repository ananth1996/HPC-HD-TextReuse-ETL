#%%
from IPython import get_ipython
if get_ipython() is not None and __name__ == "__main__":
    notebook = True
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
else:
    notebook = False
from pathlib import Path
project_root = Path(__file__).parent.parent.resolve()
import sys
sys.path.append(str(project_root))
from plot_utils import log_binning
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import powerlaw
#%%
data_dir = project_root/"data"
samples = []
databases = ["hpc-hd","hpc-hd-newspapers"]
for database in databases:
    _df = pd.read_csv(data_dir/f"{database}-samples.csv")
    _df["database"]=database
    samples.append(_df)

samples = pd.concat(samples)

results = pd.read_csv(data_dir/"reception-queries-results-1.csv")
results = results.merge(samples,left_on=["doc_id","database"],right_on=["manifestation_id","database"])
#%%[markdown]
sns.catplot(x="query_dists_id", y="duration", hue="query_type", col="database", data=results, kind="bar",log=True)
plt.ylabel("Query Bucket Number ")
#%%