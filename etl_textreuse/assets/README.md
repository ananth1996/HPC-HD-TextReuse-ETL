We describe the Dagster Assets from different groups. 


# Table of Contents
- [Table of Contents](#table-of-contents)
- [`textreuse` Assets](#textreuse-assets)
  - [`raw_textreuses` : Extracting Raw BLAST text reuses from zip file](#raw_textreuses--extracting-raw-blast-text-reuses-from-zip-file)
  - [`textreuse_ids` : Creating INT ids for each unique document](#textreuse_ids--creating-int-ids-for-each-unique-document)
  - [`textreuses`: Creating integer ids for each BLAST hit](#textreuses-creating-integer-ids-for-each-blast-hit)
  - [Normalizing the original BLAST hits](#normalizing-the-original-blast-hits)
  - [Defragmenting raw BLAST hits](#defragmenting-raw-blast-hits)
  - [Clustering the Text Reuse Graph](#clustering-the-text-reuse-graph)
  - [Dependency Graph](#dependency-graph)

# `textreuse` Assets

In this section, we describe all the assets from `textreuse` group. These assets are related to the ETL of the raw BLAST data.

1. Upstream Assets:
   - `BLAST_ZIP_FILE`: Name of zip file with Raw BLAST Text Reuses
2. S3 Materialized Assets
    - [`raw_textreuses`](/etl_textreuse/assets/raw_textreuses.py#L80)
    - [`textreuse_ids`](/etl_textreuse/assets/raw_textreuses.py#L141)
    - [`textreuses`](/etl_textreuse/assets/raw_textreuses.py#L181)
    - [`orig_pieces`](./orig_textreuses.py#L14)
    - [`orig_textreuses`](./orig_textreuses.py#L46)
    - [`piece_id_mappings`](./defragmentation.py#L14)
    - [`defrag_pieces`](./defragmentation.py#L42)
    - [`defrag_textreuses`](./defragmentation.py#L65)
    - [`adjacency_list`](./chinese_label_propagation.py#L32)
    - [`clusters`](./chinese_label_propagation.py#L58)
    - [`clustered_defrag_pieces`](./downstream_clusters.py#L15)

## `raw_textreuses` : Extracting Raw BLAST text reuses from zip file

Indicate the location of the S3 bucket and the name of zip file with the raw BLAST text reuses in the `.env` file:

```bash
RAW_BUCKET=<ALLAS BUCKET NAME>
BLAST_ZIP_FILE=<NAME OF BLAST ZIP FILE>
```

The structure of the zipfile should look like:  
```
tr_data_out_all.zip
|
|-tr_output_267.jsonl
|-tr_output_268.jsonl
|
```

Where each JSONL file has lines of raw text reuses from BLAST and each line looks like the following :

```json
{"text1_id": "0287901000", "text1_text_start": 87858, "text1_text_end": 87966, "text2_id": "0416900101", "text2_text_start": 3535059, "text2_text_end": 3535175, "align_length": 89, "positives_percent": 91.01}
```

The [`raw_textreuses`](/etl_textreuse/assets/raw_textreuses.py#L80) asset streams the ZIP file from the S3 bucket using Boto3, strams the JSON lines into a Parquet file with the following schema:

```bash
raw_textreuses
 |-- align_length: integer (nullable = true)
 |-- positives_percent: float (nullable = true)
 |-- text1_id: string (nullable = true)
 |-- text1_text: string (nullable = true)
 |-- text1_text_end: integer (nullable = true)
 |-- text1_text_start: integer (nullable = true)
 |-- text2_id: string (nullable = true)
 |-- text2_text: string (nullable = true)
 |-- text2_text_end: integer (nullable = true)
 |-- text2_text_start: integer (nullable = true)
```

## `textreuse_ids` : Creating INT ids for each unique document

Once the `raw_textreuses` has been materialized, we begin by creating integer ids for each unique source document of text reuse by looking at the `text1_id` and `text2_id` attributes. The three types of `text_id`s we have based on the document collections:

1. ECCO: 
    - A 0-padded 10-digit ECCO id
    - E.g., `"0000100100"`
2. EEBO-TCP:
    - Two part ID separated by a `.`:
      - First part: `A` or `B` followed by a five-digit number
      - Second Part: A structure name of where in the EEBO-TCP document the raw text is from
    - E.g.,`A00003.headed_1_text_2_body_note_at_6032`
    - 
3. Bl-Newspapers:
   - Newspaper article ID
   - E.g., `NICNF0317-C00000-N0000081-00020-001`

> [!IMPORTANT]  
>
> Due to the two-part ID from EEBO-TCP documents, the raw text source which is sent to BLAST is different from the physical manifestation of a document.

The [`textreuse_ids`](/etl_textreuse/assets/raw_textreuses.py#L141) asset splits the `text_id` (renamed to `text_name` in the asset) attribute into a `manifestation_id` and a `structure_name` which together identify a unique source of text reuse or a **TRS (text reuse source)**. Then the asset creates a unique integer `trs_id` ordered by the `manifestation_id` and `structure_name`. The schema of the asset is as follows:

```bash
textreuse_ids
 |-- trs_id: long (nullable = true)
 |-- text_name: string (nullable = true)
 |-- manifestation_id: string (nullable = true)
 |-- structure_name: string (nullable = true)   
```

## `textreuses`: Creating integer ids for each BLAST hit

Using the `raw_textreuses` and the `textreuse_ids` assets, we now map the `text1_id` and `text2_id` to the integer TRS ids `trs1_id` and `trs2_id`, respectively. Then the asset creates unique integer ids for each BLAST hit ordered by `trs1_id` and `trs2_id`. The asset schema is as follows:

```bash
textreuses
 |-- textreuse_id: long (nullable = true)
 |-- trs1_id: long (nullable = true)
 |-- trs1_start: integer (nullable = true)
 |-- trs1_end: integer (nullable = true)
 |-- trs2_id: long (nullable = true)
 |-- trs2_start: integer (nullable = true)
 |-- trs2_end: integer (nullable = true)
 |-- align_length: integer (nullable = true)
 |-- positives_percent: float (nullable = true)
```

## Normalizing the original BLAST hits

> [!IMPORTANT]  
>
> A **piece** is a unique fragment of text identified by a `trs_id` and a `trs_start` and `trs_end` offsets in the TRS.
>
> A **textreuse** is a pair of pieces identified by BLAST to be lexically similar.

We now normalize the `textreuses` asset into two assets, namely, [`orig_pieces`](./orig_textreuses.py#L14) and [`orig_textreuses`](./orig_textreuses.py#L46).

We collect all the unique pieces from the `textreuse` asset and create unique integer ids called `piece_id` and materialize the `orig_pieces` asset with the following schema:

```bash
org_pieces
 |-- piece_id: long (nullable = true)
 |-- trs_id: long (nullable = true)
 |-- trs_start: integer (nullable = true)
 |-- trs_end: integer (nullable = true)
```

Then using the pieces, we materialize the `orig_textreuses` asset as a mapping between pieces with the following schema:

```bash
orig_textreuses
 |-- piece_id: long (nullable = true)
 |-- trs_id: long (nullable = true)
 |-- trs_start: integer (nullable = true)
 |-- trs_end: integer (nullable = true)
```

## Defragmenting raw BLAST hits

<a name="fragmentation">![Defragmentation Illustration](/docs/images/fragmentation.png)</a>

**Fragmentation** occurs when the same passage of text has slightly different offsets when identified in different documents[^1].
This is caused due to the noisy OCR of digitized documents and the fuzzy nature of the BLAST algorithm.
In the <a href="#fragmentation">Figure</a> above each line depicts
a document from beginning to end, rectangles depict a piece’s start and
end offsets and the colors correspond to the reuse pairs. BLAST detects a
passage of text from document *D* being reused in documents *R1*, *R2* and
*R3*. Due to fragmentation the offsets of the three pieces in *D* differ slightly. The offsets of these fragmented pieces overlap significantly but are identified as different pieces.
Such fragmentation causes issues for downstream analysis tasks.

[^1]: [https://www.utupub.fi/bitstream/handle/10024/146706/Vesanto_Aleksi_opinnayte.pdf?sequence=1](https://www.utupub.fi/bitstream/handle/10024/146706/Vesanto_Aleksi_opinnayte.pdf?sequence=1).

To combat this, we "defragment" the raw hits by conservatively merging the pieces in each TRS. The logic/formula we follow for merging is that pieces to be merged, their start and end offsets need to be within 10 to 180 characters of each other, further limited so that the offset difference can be at a maximum 1/4 of the length of the piece.

This defragmentation logic is implemented as a Spark User Defined Aggregation Function (UDAF) `GetPieceIdMapping` in [piece_id_mapping.ipynb](./piece_id_mappings.ipynb). The UDAF is used on the `orig_pieces` asset dataframe to create a mapping of original pieces to ones which should be merged in the `piece_id_mappings` asset with the following schema:

```bash
piece_id_mappings
 |-- orig_piece_id: long (nullable = true)
 |-- defrag_piece_id: long (nullable = true)
```

The [`piece_id_mappings`](./defragmentation.py#L14) asset executes the Jupyter Notebook which materializes the dataframe in the S3 storage. 
> [!WARNING]  
>
> The `piece_id_mappings.ipynb` is a notebook that uses Spark, Scala 2.12 and Ammonite REPL. Please ensure the Spark driver is configured with the correct dependencies.

Using the materialized `piece_id_mappings` asset, we create the new [`defrag_pieces`](./defragmentation.py#L42) asset by merging the original pieces from BLAST with the following schema similar to `orig_pieces`:

```bash
defrag_pieces
 |-- piece_id: long (nullable = true)
 |-- trs_id: long (nullable = true)
 |-- trs_start: integer (nullable = true)
 |-- trs_end: integer (nullable = true)
```

Then we merge the reuse hits into [`defrag_textreuses`](./defragmentation.py#L65) asset which contains a `num_original_lins` attribute indicating how many original BLAST hits have been merged into this new defragmented text reuse hit. We also create new integer `textreuse_id` to uniquely identify each defragmented hit. Its schema is similar to the `orig_textreuses` schema:

```bash
defrag_textreuses
 |-- textreuse_id: long (nullable = true)
 |-- piece1_id: long (nullable = true)
 |-- piece2_id: long (nullable = true)
 |-- num_orig_links: long (nullable = true)
```

## Clustering the Text Reuse Graph

The next step in the processing is to cluster the graph network formed by the pieces as the nodes and the text reuses as the edges.

First, we materialize the [`adjacency_list`](./chinese_label_propagation.py#L32) asset consisting of each defragmented piece and all the pieces it was linked to via text reuse in the `other_pieces_ids` list, with the following schema:

```bash
adjacency_list
 |-- piece_id: long (nullable = true)
 |-- other_piece_ids: array (nullable = true)
 |    |-- element: long (containsNull = true)
```

Then, the [`clusters`](./chinese_label_propagation.py#L58) asset runs a Map-Reduce version of the Chinese Whispers Algorithm on the graph defined by the `adjacency_list`. The algorithm is run iteratively with each even iteration being saved in the `clusters_counts_0.parquet` file and every odd iteration saved in the `clusters_counts_1.parquet`. The schema of these files are as follows:

```bash
clusters_counts_*.py
 |-- piece_id: long (nullable = true)
 |-- cluster_id: long (nullable = true)
 |-- cluster_counts: map (nullable = true)
 |    |-- key: long
 |    |-- value: long (valueContainsNull = true)
 |-- active: string (nullable = true)
```

> [!WARNING]
> Currently the `clusters` asset might be unstable for large graphs. The asset begins from a hard-coded `iter` variable in the code. If the asset crashes in Dagster, please check the last successful iter and then appropriately update the `iter` variable in the asset and re-materialize the asset.

After the clustering algorithm has finished (typically for an even number of iterations), the [`clustered_defrag_pieces`](./downstream_clusters.py#L15) asset uses the `clusters_counts_0.parquet` file to create a mapping between the clusters and the defragmented pieces with the following schema:

```bash
 |-- piece_id: long (nullable = true)
 |-- cluster_id: long (nullable = true)
```

## Dependency Graph

```mermaid
graph TD;

BLAST_ZIP_FILE --> raw_textreuses;
raw_textreuses --> textreuse_ids;
raw_textreuses --> textreuses;
textreuse_ids --> textreuses;
textreuses --> orig_pieces;
orig_pieces --> orig_textreuses;
textreuses --> orig_textreuses;
orig_pieces --> piece_id_mappings;
piece_id_mappings --> defrag_textreuses;
orig_textreuses --> defrag_textreuses;
orig_pieces --> defrag_pieces;
piece_id_mappings --> defrag_pieces;
defrag_textreuses --> adjacency_list;
adjacency_list --> clusters;
clusters --> clustered_defrag_pieces;
````
