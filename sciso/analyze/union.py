from pymongo import MongoClient


def sync_pub_refs():
    union = 0
    try:
        # Connect to the MongoDB instances
        client = MongoClient("localhost", 27017)

        # Databases and collections
        sciso24 = client["sciso-2024"]
        sciso = client["sciso"]

        sciso_pub_refs = sciso["pub_refs"]
        sciso2024_pub_refs = sciso24["pub_refs_normalized"]
        pub_refs_union = sciso24["pub_refs_normalized_union"]

        pub_refs_union.delete_many({})  # Clear pub_refs_union to start fresh
        pub_refs_union.insert_many(list(sciso2024_pub_refs.find()))

        # Retrieve all documents from sciso.pub_refs
        sciso_docs = list(sciso_pub_refs.find())

        for doc in sciso_docs:
            post_id = doc["PostId"]
            url = doc["Url"]

            # Check if entry exists in sciso2024.pub_refs
            sciso2024_doc = sciso2024_pub_refs.find_one({"PostId": post_id, "Url": url})

            if not sciso2024_doc:
                # Insert the document from sciso.pub_refs
                pub_refs_union.insert_one(doc)
                union += 1

        print("Synchronization completed successfully.")
        print("Union: ", union)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    sync_pub_refs()
