# SciSO: Mining Science in StackOverflow
Dataset: https://doi.org/10.6084/m9.figshare.27967092

SciSO provides an open-source toolkit for identifying academic citations in the form of hyperlinks from any dataset. This is provided as a Python codebase that can be run on any Stack Overflow data dump, and can also be adapted to other datasets. This pipeline involves multiple steps -

1. Preprocessing the data-dump into jsonl format
2. Loading the jsonl file into MongoDB
3. Extracting all URLs from post history
4. Filtering the URLs to discover candidate URLs
5. Verifying candidate URLs to get academic URLs
6. Importing academic URLs into MongoDB
7. Joining academic URLs with candidate URLs to find all academic references
8. Exporting the finished dataset

## 1. Preprocessing the data-dump into jsonl format
The StackOverflow data dump is provided as a zip archive containing XML files, where each XML file represents one database table. We are specifically interested in the `PostHistory.xml` file, which contains information about all posts on SO, including their corresponding edit histories.

Each `<row>` element in this file contains information about one post. The exact schema of the file can be found [here](https://meta.stackexchange.com/a/2678). This XML file is converted into jsonl so that it can be easily imported into a MongoDB database. The `sciso/preprocess/convert.py` file is used to achieve this conversion.

```bash
python convert.py input/path output/path 
```

Here, the input path is the path to the `PostHistory.xml` file, and the output path is the path where you wish to store the jsonl file. For example, if the `PostHistory.xml` file is present in the working directory, and you also wish to put the jsonl file in the working directory, run -

```bash
python convert.py PostHistory.xml post_history.jsonl
```

Since the `PostHistory.xml` file is a large file (~100GB), this takes a while to complete. After this file has finished running, you should have a `post_history.jsonl` file in your working directory.

## 2. Loading the jsonl file into MongoDB
This step assumes you have MongoDB installed on your machine, and you have started the MongoDB server on its default port (27017). The `post_history.jsonl` file is imported into MongoDB using `mongoimport`.

```bash
mongoimport --db sciso --collection post_history --file post_history.jsonl
```

After this step, you should have a collection named `post_history` in your MongoDB database.

## 3. Extracting all URLs from post history
From the `post_history` collection, we can now extract all URLs found in an SO post across its edit history. This is done using the `sciso/preprocess/extract_post_history.py` file. It can be run by passing it the `--db` and the `--op` options -

```bash
python extract_post_history.py --db <database_name> --op import_urls
```

Here, the `<database_name>` refers to the name you have given to your database. After this step, you should have a `urls` collection in your database. This collection contains _every_ URL found on SO. We further filter these URLs based on some heuristics as described in the paper in the next step.

## 4. Filtering the URLs to discover candidate URLs
From the `urls` collection, we can now apply our heuristics to consider only those URLs that are most likely to be academic URLs. This filtering is done by `sciso/extract/extract.py`.

```bash
python extract.py --db <databse_name>
```

Here, the `<database_name>` refers to the name you have given your database. After this step, you should have a `candidate_urls` collection in your database. This collection contains the URLs found on any SO post across its edit history which is likely to be academic.

## 5. Verifying candidate URLs to get academic URLs
We can now verify each URL in the `candidate_urls` collection using academic databases like OpenAlex, SemanticScholar, and Crossref. This is done by the `sciso/harvest_url/run.py` file.

This script should be run from the project root, and takes a "source" name as an input, which refers to the source of the academic URL.

```bash
python -m sciso.harvest_url.run -s <source_name>
```

You can get a list of all the source names present in the database using this simple Python script -

```python
from pymongo import MongoClient

# Connect to your MongoDB instance
client = MongoClient("mongodb://localhost:27017/")
db = client.get_database("your_database_name")  # replace with your database name
collection = db.get_collection("candidate_urls")

# Get all unique values of the 'source' field
unique_sources = collection.distinct("source")

print(unique_sources)
```

The `run.py` script should be run on every source present in your database. This script also requires a few environment variables to be set. 

1. If you plan on using SemanticScholar during the verification process, you will need a SemanticScholar API key. You can request one [here](https://www.semanticscholar.org/product/api#api-key).
2. You will need to set a contact email for OpenAlex as well as Crossref which will be used by the web scraper.

Set these environment variables using a `.env` file placed at the project root -

```
ALEX_EMAIL=<your_email>
CROSSREF_EMAIL=<your_email>
S2_API_KEY=<your_s2_api_key>
```
After the script has been run on all sources, a `catalogs/` folder will be created with jsonl files describing the result of the verification process. A new directory is created inside the `catalogs/` folder for each source, and each new directory has the following jsonl files -

1. `failed.jsonl` - This file contains the URLs that failed the verification process. The bibliographic metadata identified for this URL was invalid, or was not found on any academic database, or for any other reason.
2. `success.jsonl` - This file contains the URLs that passed the verification process. They gave bibliographic metadata that was also found on an academic database, which means that this URL points towards an academic reference.
3. `unmatchable.jsonl` - 
4. `unparsable.jsonl` - This file contains the URLs that did not give valid bibliographic metadata, so were skipped.
5. `unreachable.jsonl` - This file contains the URLs that were broken, and were not reachable, even by using Internet Archive's Wayback Machine.

## 6. Importing academic URLs into MongoDB
Once the `catalogs/` folder has been completed with all sources, the resulting `success.jsonl` files can be imported into a MongoDB collection. This can be done using a simple bash script in `sciso/harvest_url/import_pub_urls.sh`

The `ROOT_DIR` and `DB_NAME` variable in the script should be set to the absolute path to the `catalogs/` folder and the name of your database respectively.

After this step, you should have a collection named `pub_urls` in your database containing all URLs found on SO that have been verified to be academic URLs.

## 7. Joining academic URLs with candidate URLs to find all academic references
We can then perform a join between the `pub_urls` and the `candidate_urls` collections to get a collection of all academic references found on SO. This is done using the `sciso/extract/join.py` script.

```bash
python join.py
```

This script takes no arguments, and simply performs a join between the `candidate_urls` and the `pub_urls` collections while aggregating any metadata found in duplicate entries. After this step, you should have a `pub_refs` collection in your database which contains the final SciSO dataset.

## 8. Exporting the finished dataset
The SciSO dataset is provided as a jsonl file. This jsonl file can be generated from the `pub_refs` collection using `mongoexport`.

```bash
mongoexport --db <database_name> --collection pub_refs --out <output_path>.jsonl
```

This gives the SciSO dataset!

# Dataset Schema

The data is structured in Line-delimited JSON (JSONL) format. Each line contains data about the post which references the academic URL and the metadata of the academic reference itself. Fields in the dataset are described below.

- AddedAt  
  The time when the post was added on SO
- AddedBy
- AnswerCount  
  The number of undeleted answers
- CommentCount
- FavoriteCount
- History  
  Type of edits (e.g., initial, edit title, edit body, etc.). Corresponding to the `PostHistoryTypeId` field in the [`StackOverflow-PostHistory`](https://archive.org/download/stackexchange/stackoverflow.com-PostHistory.7z) table of the official Stack Exchange data dump. See [here](https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede) for details.
- LinkType  
  The type of link ("pdf" or "url")
- metadata
  - title
  - authors
  - normalized_venue
    <small>Normalized to the full official name via Semantic Scholar<br>(e.g., ACL -> Annual Meeting of the Association for Computational Linguistics)</small>
  - open_access  
    <small>Whether the referenced article is publicly accessible</small>
  - citation_count
  - abstract
  - type
  - external_ids
  - year
  - concepts
  - venue
  - year
- PostId  
  ID of the post containing this academic reference. Corresponding to the `Id` field in the [`StackOverflow-posts`](https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z) table of the official Stack Exchange data dump.  
  _e.g.,_ [`74109833`](https://stackoverflow.com/questions/74109833/how-to-set-learning-rate-0-2-when-training-transformer-with-noam-decay) <small>(click to see the original post on Stack Overflow)</small>
- PostTypeId
- RevisionId  
  We collected URLs from every historic version of a post. RevisionId is the ID of the changelog where we found this academic reference. Corresponding to the `Id` field in the [`StackOverflow-PostHistory`](https://archive.org/download/stackexchange/stackoverflow.com-PostHistory.7z) table of the official Stack Exchange data dump.  
  _e.g.,_ `280345137`
- Score
- Source  
  The source on which this URL is hosted
- Url  
  The URL of the academic reference.  
  _e.g.,_ [`https://aclanthology.org/C18-1054.pdf`](https://aclanthology.org/C18-1054.pdf)
