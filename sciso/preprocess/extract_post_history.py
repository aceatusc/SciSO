import argparse
import multiprocessing
from urllib.parse import urlparse
import regex as re
import pymongo
import xurls
import logging
import sys
from tqdm import tqdm

handler = logging.FileHandler("preprocess.log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
extractor = xurls.Relaxed()


def remove_code_blocks(text: str):
    text = re.sub(r"<code.*?>.*?<\/code>", " ", text, flags=re.DOTALL)
    text = re.sub(r"<pre.*?>.*?<\/pre>", " ", text, flags=re.DOTALL)
    text = re.sub(r"<script.*?>.*?<\/script>", " ", text, flags=re.DOTALL)
    text = re.sub(r"<!--.*?-->.*?<!--.*?-->", " ", text, flags=re.DOTALL)
    text = re.sub("```.*?```", " ", text, flags=re.DOTALL)
    text = re.sub("~~~.*?~~~", " ", text, flags=re.DOTALL)
    text = re.sub(
        "^ {4}.*(?:\\r\\n|\\r|\\n)?",
        " ",
        text,
        flags=re.MULTILINE,
    )
    return text


def extract_urls(doc: dict):
    retval = []
    try:
        text = doc.get("Text", None)
        if text is None:
            return retval
        text = remove_code_blocks(text)
        all_urls = set(extractor.findall(text))
        for url in all_urls:
            try:
                parsed_url = urlparse(url)
                if parsed_url.scheme == "":
                    parsed_url = urlparse(f"http://{url}")
                if (
                    parsed_url.path in ["", "/"]
                    and parsed_url.query == ""
                    and parsed_url.params == ""
                    and parsed_url.fragment == ""
                ):
                    continue
                link_type = "pdf" if parsed_url.path.endswith(".pdf") else "url"
                retval.append(
                    {
                        "Url": url,
                        "PostId": doc["PostId"],
                        "History": doc["PostHistoryTypeId"],
                        "LinkType": link_type,
                        "AddedAt": doc["CreationDate"],
                        "AddedBy": doc["UserId"],
                        "RevisionId": doc["Id"],
                    }
                )
            except Exception as e:
                msg = f"Error in {doc['Id']}: {str(e)}"
                logger.error(msg)
                continue
    except Exception as e:
        msg = f"Error in {doc['Id']}: {str(e)}"
        logger.error(msg)
    return retval


def split_tags(doc):
    if "PostTypeId" in doc and doc["PostTypeId"] == "1" and "Tags" in doc:
        if type(doc["Tags"]) == list:
            return doc
        else:
            doc["Tags"] = doc["Tags"][1:-1].split("><")
            return doc
    # try:
    #     if "ContentLicense" in doc:
    #         del doc["ContentLicense"]
    #     if "PostTypeId" in doc and doc["PostTypeId"] == "1":
    #         if type(doc["Tags"]) == str:
    #             doc["Tags"] = doc["Tags"][1:-1].split("><")
    #             print(doc["Tags"])
    # except Exception as e:
    #     print(f"Error in {doc['Id']}: {str(e)}")
    #     print(doc)
    # return doc


def process_cursor(idx, skip_n, limit_n, query):
    # client = pymongo.MongoClient("mongodb://localhost:27017/")
    # db = client[args.db]
    posts = db["posts"]
    cursor = posts.find(query).skip(skip_n).limit(limit_n)
    for doc in tqdm(
        cursor, total=limit_n, desc=f"{skip_n}-{skip_n + limit_n}", position=idx
    ):
        res = split_tags(doc)
        try:
            posts.replace_one({"Id": doc["Id"]}, res)
        except Exception as e:
            msg = f"Error in {doc['Id']}: {str(e)}"
            logger.error(msg)
    cursor.close()


def import_urls(idx, skip_n, limit_n, query):
    # client = pymongo.MongoClient("mongodb://localhost:27017/")
    # db = client[args.db]
    post_history = db["post_history"]
    urls = db["urls"]
    cursor = post_history.find(query).skip(skip_n).limit(limit_n)
    for doc in tqdm(
        cursor, total=limit_n, desc=f"{skip_n}-{skip_n + limit_n}", position=idx
    ):
        res = extract_urls(doc)
        for r in res:
            try:
                urls.insert_one(r)
            except pymongo.errors.DuplicateKeyError:
                pass
            except Exception as e:
                msg = f"Error in {doc['Id']}: {str(e)}"
                logger.error(msg)
    cursor.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--op",
        choices=["split_tags", "import_urls"],
        help='The operation to perform. Can be either "split_tags" or "import_urls"',
    )
    parser.add_argument(
        "--db", help="The name of the database to use", default="sciso-2024-sample"
    )
    args = parser.parse_args()

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[args.db]
    concurrency = 9

    if args.op == "split_tags":
        total = db["posts"].estimated_document_count()
        batch_size = round(total / concurrency + 0.5)
        skips = range(0, concurrency * batch_size, batch_size)
        processes = [
            multiprocessing.Process(
                target=process_cursor, args=(i, skip_n, batch_size, {})
            )
            for i, skip_n in enumerate(skips)
        ]
        for proc in processes:
            proc.start()
        for proc in processes:
            proc.join()
    elif args.op == "import_urls":
        post_history = db["post_history"]
        urls = db["urls"]
        total = db["post_history"].estimated_document_count()
        batch_size = round(total / concurrency + 0.5)
        skips = range(0, concurrency * batch_size, batch_size)
        urls.drop()
        urls.create_index(
            [("Url", pymongo.ASCENDING), ("PostId", pymongo.ASCENDING)], unique=True
        )
        queries = [{"PostHistoryTypeId": "2"}, {"PostHistoryTypeId": "5"}]
        for q in queries:
            processes = [
                multiprocessing.Process(
                    target=import_urls,
                    args=(i, skip_n, batch_size, q),
                )
                for i, skip_n in enumerate(skips)
            ]
            for proc in processes:
                proc.start()
            for proc in processes:
                proc.join()
