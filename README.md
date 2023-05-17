# Extract Transform Load for Text Reuse data

A streaming approach to extract the contents of the textreuse data. 

## Prerequisites 

The following will be necessary to run the code

### Access to Allas

The zip files will be stored in a s3 bucket on [Pouta](pouta.csc.fi). Follow the [docs](https://docs.csc.fi/data/Allas/using_allas/s3_client/) to configure allas to obtain the s3 credentials. Then in the root directory of this project create a file `s3credentials.toml` with the following format:

```toml
[default]
aws_access_key_id=<access_key>
aws_secret_access_key=<secret_key>
endpoint_url=<endpoint_url>
```
This file will not be version controlled and will be used by `boto3` and `spark` to access the s3 bucktets to read and write data.

### Spark Cluster

There is a spark cluster set up on [Rahti](https://docs.csc.fi/apps/spark/). There are three types of pods that are always present namely, a master, notebook and worker. The master pod has a route to spark UI used to monitor the applications and workers. The notebook pod hosts a driver instance which has jupyter lab installed and attached to a permanent storage volume that is accessible to all the pods in the cluster. The worker pods are are the computational nodes and can be spun up (and down) on demand.

Follow the steps in the [docs](https://docs.csc.fi/cloud/rahti/usage/cli/#the-command-line-tools-page-in-the-openshift-web-ui) to download and install the openshift command line tools on your local machine and use the Rahti web interface to get the login command. Once the command line tools are up and running you can find the spark notebook pod as follows:
```bash
oc get pods | grep notebook
```
Then you can get a remote shell to the notebook pod using:
```bash
oc rsh <notebook-pod-name> bash
```
Now from inside the notebook pod this repository should be cloned. 
**Note**: The repository can also be run on a local spark cluster for development purposes/


**Note**: The exisitng spark cluster has a `spark-conf` set up in the Rahti config maps. If setting up a new cluster, ensure to make (or update) the spark configs.

## Python

Once the repository has been cloned install [poetry](https://python-poetry.org) for python dependency management. Create the virual env and ensure it is in the project directoy by running 
```bash
poetry config virtualenvs.in-project true
poetry install
```
This will install the python library required for the application.


## Zip to Parquet

The first step will extract the raw textreuses from the `.zip` file. The `.zip` file and the resulting `.parquet` files will reside on Allas.

### Format 

A rough structure of the data in the text reuse `.zip` is as follows:
```
txtreuse.zip
|-iter_0_out.tar.gz
|	|-iter_0.json
|-iter_1_out.tar.gz
|	|-iter_0.json
|
```

Each `iter_*,json` file contains an array of textreuses which look like:
```json
[
  {
    "text1_id": "0000100100",
    "text1_text_start": 405269,
    "text1_text_end": 405464,
    "text1_text": "ith some particular Ornament, your\nFace with Beautie, your Head with Wife-\ndome, your Eyes with Majeftie, your\nCountenaunce with Gracefulneffe, your\nLips with Lovelineffe, your Tongue with\nVirori",
    "text2_id": "A12231.headed_1_text",
    "text2_text_start": 383611,
    "text2_text_end": 383795,
    "text2_text": "ith some particular ornament; her face with beautie, her head with wisdome, her eyes with maiestie, her countenance with gracefulnes, her lippes with louelines, her tongue with victori",
    "align_length": 163,
    "positives_percent": 79.14
  },
  ...
]
```
### Streaming S3 zip files 

We use the [`smart_open`](https://github.com/RaRe-Technologies/smart_open) library to stream s3 obejcts in an efficient manner. While `smart_open` allows the [reading and writing of zipfiles](https://github.com/RaRe-Technologies/smart_open), there is a known performance issue in the python `zipfile` module which performs multiple to seeks to current positions of a file stream which causes a s3 buffer to be cleared as seen in this [issue](https://github.com/RaRe-Technologies/smart_open/pull/748). AS of writing this PR was not accepted in to the stable branch of the library. Therefore, we have [forked](https://github.com/HPC-HD/smart_open/tree/s3_ignore_seeks_to_current_position) and applied the patches suggested. We include this forked repo in the poetry dependency at the moment.


## Script 

Run the end-to-end etl job as follows on the notebook pod

```bash
spark-submit zip2parquet.py txtreuse.zip -v
```
You can monitor the progess on the spark master. 


## Transforming the Textreuses

Once the raw textreuses have been converted into parquet, we need to create unique INT ids for each reuse which we call `textreuse_id` and each source document which we call a `textreuse_source_id` or `trs_id` for short. 

The job is in the [unique_ids.py](unique_ids.py) script. This creates two dataframes in the processed s3 bucket on Allas. 

The first dataframe is `textreuse_ids` which has the following attributes:  
|attribute | explanation | example
| --- | --- | --- |
| `trs_id`| The INT id for a textreuse source | 1
|`text_name`| The name from the zip| A45465.headed_1_text_2_body_note_at_9794 
|`manifestation_id`|The id that maps to a metadata table| A45465
|`structure_name`| The structural information (nullable)| headed_1_text_2_body_note_at_9794 

The `structure_name` attribute is for EEBO_TCP documents which can be split into several sources for textreuse like notes, margins etc. The `manifestation_id` maps to either ECCO or EEBO_TCP documents. 

The second dataframe is `textreuses` which reformats the original textreuse rows with the `trs_id` and a unique row number.

|attribute | explanation | example
| --- | --- | --- |
|`textreuse_id`| The INT id for a textreuse row | 1
|`trs1_id`| The INT id for the first textreuse source | 45 
|`trs1_start`| The start offset in the first textreuse source | 12300 
|`trs1_end`| The end offset in the first textreuse source | 14310
|`trs2_id`| The INT id for the second textreuse source | 100
|`trs2_start`| The start offset in the second textreuse source | 29000 
|`trs2_end`| The end offset in the second textreuse source | 31000
|`align_length`| The length of the reuse pieces that aligned in BLAST | 1000
|`positive_percent`| The percentage of characters that are similar in BLAST | 93.7

## Cleaning  Textreuses

### Defragmention 

Due to the nature of BLAST and the quality of the OCT, the textreuse found are highly fragmented. This means that instead of a single hit for a long paragraph in one source document, BLAST reports it as 3 smaller pieces. Such fragmentation leads to a lot of noise in the data.

To clean the data is to merge pieces of text within a source document that overlap a certain amount. Because we want to preserve the textreuse edge information, we also enforce a condition that the pieces merging should be roughly the same size and close to each other in the document. The logic for merging overlapping pieces is from this [repo](https://github.com/HPC-HD/cluster-textreuses):

>  The formula is that for snipptes to be mapped, their start and end offsets need to be within 10 to 180 characters of each other, further limited so that the offset difference can be at a maximum 1/4 of the length of the piece.

The Scala notebook [defragment.ipynb](defragment.ipynb) contains the code to achieve this. It requies [Almond](https://almond.sh) which provides a Jupyter kernel and Spark libraries.

The first step is to normalise the `textereuses` dataframe into a `orig_pieces` and `orig_textreuses` dataframes. A piece is a fragment of the textreuse source that is returned by BLAST. The `orig_pieces` dataframe contains is all unquie pieces identified using the `trs_id`,`trs_start` and `trs_end` attributes and a new INT id called `piece_id`. The `orig_textreuses` dataframe contains the `textreuse_id`, `tr1_id`, `trs2_id` (along with `positive_percent` and `align_length`) to indicate an edge between two pieces of text. 

Next, there is a custom UDF aggregator whcih implements the logic for merging as described above. This produces a `piece_id_mapping` dataframe which maps each original piece denoted by `orig_piece_id` to a new `defrag_piece_id`. This mapping dataframe is then used to create a `defag_pieces` and `defrag_textreuses` which are similar to their `orig_` counterparts. 

While the defragmentation reduces then number of pieces withing a source document, there are still a lot of sparse textreuse edges between pairs of source documents. Therefore, we now cluster the graph resulting from the defragmented pieces. Towards this, we create `defrag_graph_adj_list` a dataframe which is an adjacency list of the graph formed from the edges in `defrag_textreuses`.

### Clustering Defragmented Textreuses
We use the Chinese Whispers algortihm to perform the clustering. The code for this is written in GO and found in [cluster_pieces/main.go](cluster_pieces/main.go). The GO script requires the graph to be loaded into memory. Therefore, we need to run this on Puhti. 

First copy the exact parquet file from the `defrag_graph_adj_list.parquet` folder in cPouta to the folder `data/defrag_graph_adj_list.parquet` in this repository. Then dispatch a SLURM job to run the GO script. **Note:** the script may require upwards of 75GB RAM. The script should run rather quickly in about 10min. 

Once finished the script will write a `data/clusters.parquet` file in this repo. Push that file to the processed s3 Allas bucket.

## Downstream Data Processing 

1. `manifestation_ids`: INT ids for all ECCO and EEBO_TCP ids

2. `edition_mapping` : Maps `manifestation_id_i` to `edition_id_i` . Also has a corresponding `edition_ids` table for INT `edition_id_i` to `edition_id` strings. Creates new `edition_id` when no `estc_id` link exists.
3. `work_mapping` : Maps `manifestation_id_i` to `work_id_i`. Also has a corresponding `work_ids` table for INT `work_id_i` and string `work_id` . Creates new `work_id` when `estc_core` doesn’t have information.
4. `edition_publication_year` : The publication year for each `edition_id_i` . Data pulled from `estc_core` when available and falls back to `ecco_core` or `eebo_core` otherwise
5. `work_earliest_publication_year`: The earliest publication year of all editions for a given `work_id_i`
6. `textreuse_edition_mapping` : Maps `trs_id` to `edition_id_i`
7. `textreuse_work_mapping`: Maps `trs_id` to work_id_i
8. `textreuse_earliest_publication_year`: The earliest year of publication of all editions mapped for a given  `trs_id`
9. `clustered_defrag_pieces`: The clusters of defragmented textreuse pieces from the Chinese Whispers Algorithm
10. `earliest_textreuse_by_cluster`: The `trs_id` with the earliest publication year for each cluster
11. `earliest_work_and_pieces_by_cluster` : The earliest `work_id_i` and the corresponding `piece_id` for each cluster.



