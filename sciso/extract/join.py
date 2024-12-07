from pymongo import MongoClient
from tqdm import tqdm


def is_str_valid(title: str) -> bool:
    return title is not None and isinstance(title, str) and len(title.strip()) > 0


db = MongoClient("localhost", 27017).get_database("sciso-2024")
db.drop_collection("pub_refs")
pub_urls = db["pub_urls"]
pub_refs = db["pub_refs"]
candidate_urls = db["candidate_urls"]
pub_refs.create_index([("PostId", 1), ("Url", 1)], unique=True)

cursor = candidate_urls.find({})
counter = 0
for doc in tqdm(cursor, total=candidate_urls.count_documents({})):
    url = doc["Url"]
    pub_docs = list(pub_urls.find({"key": url}))
    if len(pub_docs) == 0:
        continue
    pub_doc = pub_docs[0]
    success_tasks = {}
    for d in pub_docs:
        for k, v in d.items():
            if isinstance(v, dict) and "status" in v and v["status"] == "TaskStatus.SUCCESS":
                if success_tasks.get(k, None) is not None:
                    if v['result'] is not None and len(v["result"]) > len(success_tasks[k]["result"]):
                        success_tasks[k] = v
                else:
                    success_tasks[k] = v
    for k, v in success_tasks.items():
        pub_doc[k] = v

    metadata = {}
    if pub_doc["s2"]["status"] == "TaskStatus.SUCCESS":
        metadata["title"] = pub_doc["s2"]["result"]["title"]
        metadata["authors"] = pub_doc["s2"]["result"]["authors"]
        if type(pub_doc["s2"]["result"]["venue"]) == dict:
            metadata["venue"] = pub_doc["s2"]["result"]["venue"]
        elif is_str_valid(pub_doc["s2"]["result"]["venue"]):
            metadata["venue"] = pub_doc["s2"]["result"]["venue"]
        if is_str_valid(pub_doc["s2"]["result"]["year"]):
            metadata["year"] = pub_doc["s2"]["result"]["year"]
        metadata["open_access"] = bool(pub_doc["s2"]["result"]["open_access"])
        metadata["citation_count"] = pub_doc["s2"]["result"]["citation_count"] or 0
        if type(pub_doc["s2"]["result"]["concepts"]) == list:
            metadata["concepts"] = pub_doc["s2"]["result"]["concepts"]
        if type(pub_doc["s2"]["result"]["abstract"]) == str and is_str_valid(
            pub_doc["s2"]["result"]["abstract"]
        ):
            metadata["abstract"] = pub_doc["s2"]["result"]["abstract"]
        if pub_doc["s2"]["result"]["embedding"] is not None:
            metadata["embedding"] = pub_doc["s2"]["result"]["embedding"]
        metadata["type"] = []
        if type(pub_doc["s2"]["result"]["type"]) == list:
            metadata["type"] = pub_doc["s2"]["result"]["type"]
        elif type(pub_doc["s2"]["result"]["type"]) == str and is_str_valid(
            pub_doc["s2"]["result"]["type"]
        ):
            metadata["type"] = [pub_doc["s2"]["result"]["type"]]
        metadata["external_ids"] = {"s2": pub_doc["s2"]["result"]["source_id"]}
        for k, v in pub_doc["s2"]["result"]["external_ids"].items():
            metadata["external_ids"][k] = v

    if pub_doc["openalex"]["status"] == "TaskStatus.SUCCESS":
        if not is_str_valid(metadata.get("title", None)):
            metadata["title"] = pub_doc["openalex"]["result"]["title"]
        if pub_doc["openalex"]["result"].get("author", None) is not None:
            metadata["authors"] = pub_doc["openalex"]["result"]["authors"]
        if metadata.get("venue", None) is None:
            metadata["venue"] = pub_doc["openalex"]["result"]["venue"]
        if metadata.get("year") is not None:
            metadata["year"] = min(
                metadata["year"], pub_doc["openalex"]["result"]["year"]
            )
        else:
            metadata["year"] = pub_doc["openalex"]["result"]["year"]
        metadata["open_access"] = metadata.get("open_access", False) or bool(
            pub_doc["openalex"]["result"]["open_access"]
        )
        metadata["citation_count"] = max(
            metadata.get("citation_count", 0),
            pub_doc["openalex"]["result"]["citation_count"] or 0,
        )
        if type(pub_doc["openalex"]["result"]["abstract"]) == dict:
            metadata["abstract"] = pub_doc["openalex"]["result"]["abstract"]
        elif type(pub_doc["openalex"]["result"]["abstract"]) == str:
            if is_str_valid(metadata["abstract"]):
                if len(pub_doc["openalex"]["result"]["abstract"]) > len(
                    metadata["abstract"]
                ):
                    metadata["abstract"] = pub_doc["openalex"]["result"]["abstract"]
            else:
                metadata["abstract"] = pub_doc["openalex"]["result"]["abstract"]
        elif not is_str_valid(metadata.get("abstract", None)):
            metadata["abstract"] = pub_doc["openalex"]["result"]["abstract"]
        if metadata.get("concepts", None) is None:
            metadata["concepts"] = pub_doc["openalex"]["result"]["concepts"]
        if metadata.get("type", None) is None:
            metadata["type"] = []
        metadata["type"].append(pub_doc["openalex"]["result"]["type"])
        if metadata.get("external_ids", None) is None:
            metadata["external_ids"] = {}
        metadata["external_ids"]["openalex"] = pub_doc["openalex"]["result"][
            "source_id"
        ]
        # for k, v in pub_doc["openalex"]["result"]["external_ids"].items():
        #     if k in metadata["external_ids"]:
        #         v1 = urlparse(v).path.strip("/").strip()
        #         v2 = urlparse(metadata["external_ids"][k]).path.strip("/").strip()
        #         if v1 != v2:
        #             print(f"Conflict: {k} - {v1} vs {v2} for {url}")
        #     else:
        #         metadata["external_ids"][k] = v
    doc["metadata"] = metadata
    pub_refs.insert_one(doc)
    counter += 1

print(
    f"Inserted {counter} documents, pub_urls: {pub_urls.count_documents({})}, pub_refs: {pub_refs.count_documents({})}"
)
