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
num_reception_edges = pd.read_csv(data_dir/"num_reception_edges.csv")
reception_query_results = pd.read_csv(data_dir/"reception_query_results.csv")
#%%[markdown]
### Distribution of number of reception edges
#%%
bin_centres,hist = log_binning(num_reception_edges.num_reception_edges,density=False)
plt.loglog(bin_centres,hist)
plt.xlabel("Number of Reception Edges")
plt.ylabel("Number of documents")
plt.title("Power Law distribution")
# %%
reception_query_results
# %%
melt_df = reception_query_results.melt(id_vars=["trs_id","quartile"],value_vars=["denorm_time","standard_time"])
sns.barplot(data=melt_df,x="quartile",y='value',hue="variable")
plt.ylabel("Running Time in seconds")
plt.legend(bbox_to_anchor=(1,0.5),loc="upper left")
plt.title("Spark Results")
# %%
sns.barplot(data=reception_query_results,x="quartile",y="ground_truth",log=True)
plt.ylabel("Number of reception edges")
plt.title("Distribution of samples")
#%%