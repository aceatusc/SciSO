from pymongo import MongoClient
from tqdm import tqdm

db = MongoClient("mongodb://localhost:27017/").get_database("sciso")
posts = db.get_collection("posts")

posts.update_many({"tag": {"$exists": True}}, {"$unset": {"tag": 1}})
# total = posts.estimated_document_count()
# for qdoc in tqdm(question_cursor, total=total):
#     if qdoc["PostTypeId"] != "1":
#         continue
#     tag_str = qdoc["Tags"]
#     tag_str_split = tag_str[1:-1].split("><")
#     tag_str_split = [t.lower() for t in tag_str_split]
#     qdoc["tag"] = tag_str_split
#     posts.update_one({"_id": qdoc["_id"]}, {"$set": qdoc})
# question_cursor.close()
