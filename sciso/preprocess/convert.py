"""
Converts the stackexchange data dump from xml to jsonl
"""

import json
import pprint
import sys
import time
import xml.etree.ElementTree as ET


def get_jsonl_excerpt(input_path, length=100) -> str:
    """
    Returns a small excerpt of the given jsonl file
    :param input_path: The path to the jsonl file
    :param length: The number of lines to return
    :return: The jsonl excerpt
    """
    count = 0
    read = ""
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            json_obj = json.loads(line)
            read += str(json_obj) + "\n"
            count += 1
            if count > length:
                return read


def get_xml_excerpt(input_path, length=100) -> str:
    """
    Returns a small sample of the given XML file
    :param input_path: The path to the XML file
    :param length: The number of lines to return
    :return: The XML excerpt
    """
    count = 0
    read = ""
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            read += line + "\n"
            count += 1
            if count > length:
                return read


def xml_row_to_dict(xml_row: str, log_path="./xml_to_dict.log") -> dict[str, str]:
    """
    Converts a single XML row to a dictionary
    :param xml_row: A single XML row
    :param log_path: Path to a log file where errors will be written
    :return: A dictionary mapping the XML row's attributes to the corresponding values
    """
    if not xml_row.startswith("<row"):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("WARNING: XML row doesn't start with <row -")
            f.write(xml_row)
        return {}
    try:
        tree = ET.fromstring(xml_row)
        return tree.attrib
    except Exception as e:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("ERROR: Couldn't parse XML row")
            f.write(xml_row)
    return {}


def xml_to_jsonl(
    input_path, output_path, buffer_size=10_000_000, log_path="./xml_to_jsonl.log"
) -> dict:
    """
    Converts an excerpt of the input XML file to jsonl
    :param input_path: The input path for the XML file
    :param output_path: The output path for the jsonl file
    :param buffer_size: How many lines to write at a time. Defaults to 10MB.
    :param log_path: Path to a log file where errors will be written
    :return: A dictionary containing statistics about the written jsonl file
    """
    lines_processed = 0
    statistics = {
        "earliest_post": "9999-12-1",
        "latest_post": "",
        "total_posts": 0,
        "exception_count": 0,
    }
    start = time.time()
    with open(input_path, "r", encoding="utf-8") as f, open(
        output_path, "w", encoding="utf-8", buffering=buffer_size
    ) as outfile:
        for line in f:
            try:
                _ = line.strip()
                if not _.startswith("<row"):
                    continue
                result = xml_row_to_dict(_)
                if result == {}:
                    continue

                if statistics["earliest_post"] > result["CreationDate"]:
                    statistics["earliest_post"] = result["CreationDate"]
                if statistics["latest_post"] < result["CreationDate"]:
                    statistics["latest_post"] = result["CreationDate"]
                statistics["total_posts"] += 1

                outfile.write(json.dumps(result, separators=(",", ":")) + "\n")
                lines_processed += 1
                if lines_processed % 100_000 == 0:
                    print(
                        f"Processed {lines_processed} lines in {time.time() - start} seconds"
                    )
            except Exception as e:
                with open(log_path, "a", encoding="utf-8") as logfile:
                    logfile.write(f"ERROR: {str(e)}")
                    logfile.write(line)
                    statistics["exception_count"] += 1
    print(f"Took {time.time() - start:.2f} seconds")
    return statistics


if __name__ == "__main__":
    stats = xml_to_jsonl(sys.argv[1], sys.argv[2])
    print("Gathered the following statistics:")
    pprint.pprint(stats)
