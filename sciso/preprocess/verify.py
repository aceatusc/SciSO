"""
Verifies that the created jsonl file has the expected contents.
"""

import json
import pprint
import sys
import time


def verify(input_path) -> dict:
    """
    Reads through the entire jsonl file and collects statistics
    to verify that it has the expected contents.
    :param input_path: The path to the jsonl file
    :return: A dictionary containing statistics
    """
    statistics = {
        "earliest_post": "9999-12-31",
        "latest_post": "",
        "total_posts": 0,
    }
    count = 0
    start = time.time()
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            statistics["total_posts"] += 1
            if statistics["earliest_post"] > obj["CreationDate"]:
                statistics["earliest_post"] = obj["CreationDate"]
            if statistics["latest_post"] < obj["CreationDate"]:
                statistics["latest_post"] = obj["CreationDate"]

            count += 1
            if count % 100_000 == 0:
                print(f"Processed {count} lines")
    print(f"Took {time.time() - start:.2f} seconds to verify {count} lines.")
    return statistics


if __name__ == "__main__":
    stats = verify(sys.argv[1])
    pprint.pprint(stats)
