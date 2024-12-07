import argparse
from pymongo import MongoClient
import re
import json
import multiprocessing
from urllib.parse import urlparse

with open("/home/ruh/SciSO/sciso/preprocess/publisher_reg.json", "r") as f:
    regexes = json.load(f)

regex_patterns = [(re.compile(r["regex"]), r["source"]) for r in regexes]


def pub_match(url):
    for pattern, source in regex_patterns:
        try:
            if pattern.search(url):
                return source
        except Exception as e:
            print(e)
            continue
    return None


def is_pdf_url(url: str) -> bool:
    """
    Returns true if url points to a pdf document
    :param url:
    :return: True if the URL points to a pdf document
    """
    path = urlparse(url).path
    if ";" in path:
        path = path.split(";")[0]
    return path.endswith((".pdf", ".PDF"))


def process_cursor(skip, limit):
    print(f"starting {skip} {skip + limit}")
    # db = MongoClient("localhost", 27017).get_database(args.db)
    # urls = db["urls"]
    # pub_urls = db["pub_urls"]
    cursor = urls.find({}).skip(skip).limit(limit)
    for doc in cursor:
        try:
            if (source := pub_match(doc["Url"])) is not None:
                doc["Source"] = source
                pub_urls.insert_one(doc)
            elif is_pdf_url(doc["Url"]):
                doc["Source"] = "pdf"
                pub_urls.insert_one(doc)
        except Exception as e:
            print(e)
            continue
    cursor.close()
    print(f"finished {skip} {skip + limit}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db", help="The name of the database to use", default="sciso-2024-sample"
    )
    args = parser.parse_args()
    db = MongoClient("localhost", 27017).get_database(args.db)
    urls = db["urls"]
    pub_urls = db["candidate_urls"]
    total = urls.estimated_document_count()
    print(total)
    concurrency = 11
    batch_size = round(total / concurrency + 0.5)
    skips = range(0, concurrency * batch_size, batch_size)
    processes = [
        multiprocessing.Process(
            target=process_cursor,
            args=(skip_n, batch_size),
        )
        for skip_n in skips
    ]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()
