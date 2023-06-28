import numpy as np
import matplotlib.pyplot as plt
def log_binning(data,n_bins=10,density=True,ret_bins=False):
    min_val = min(data)
    max_val = max(data)
    bins = np.logspace(np.log10(min_val),np.log10(max_val),base=10,num=n_bins)
    bin_centres = (bins[1:]+bins[:-1])/2
    pdf, bins = np.histogram(data,bins,density=density)
    if ret_bins:
        return bins,pdf
    else:
        return bin_centres,pdf

def loglog_hist(data,n_bins=20,ax=None):
    if ax is None:
        fig,ax = plt.subplots()
    min_val = min(data)
    max_val = max(data)
    bins = np.logspace(np.log10(min_val),np.log10(max_val),base=10,num=n_bins)
    ax.hist(data,bins=bins,log=True,density=False,edgecolor="black")
    ax.set_xscale("log")
    ax.set_yscale("log")
    return ax


def linear_binning(data,n_bins=100,desnity=True):
    min_val = min(data)
    max_val = max(data)

    # create linear bins using linspace
    bins = np.linspace(min_val,max_val,n_bins)
    # find bin centres for plotting 
    bin_centres = (bins[1:]+bins[:-1])/2
    # compute the probability desnity of the data after binning 
    pdf, _ = np.histogram(data,bins,density=density)

    return (bin_centres,pdf)