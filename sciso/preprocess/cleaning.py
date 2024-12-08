import subprocess
import pandas as pd
import numpy as np
import re
from tqdm import tqdm
from concurrent.futures import as_completed, ProcessPoolExecutor, wait
from urllib.parse import urlparse
import argparse

pd.set_option("display.max_columns", None)

q_fields = {
    # "Id": "Int64",
    "OwnerUserId": "Int64",
    "PostTypeId": "Int64",
    "AcceptedAnswerId": "Int64",
    "Score": "Int64",
    "ViewCount": "Int64",
    "AnswerCount": "Int64",
    "CommentCount": "Int64",
    "Title": str,
    "Tags": str,
    "Body": str,
    "CreationDate": str,
    "FavoriteCount": "Int64",
}
a_fields = {
    # "Id": "Int64",
    "OwnerUserId": "Int64",
    "PostTypeId": "Int64",
    "Score": "Int64",
    "CommentCount": "Int64",
    "Body": str,
    "CreationDate": str,
    "ParentId": "Int64",
}
comments_fields = {
    # "Id": "Int64",
    "PostId": "Int64",
    "UserId": "Int64",
    "Text": str,
    "CreationDate": str,
}
fields_to_keep = {**q_fields, **a_fields, **comments_fields}
URL_REG = "((?:https?:\\/\\/)?(?:www\\.)?[-a-zA-Z0-9@:%\\._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+\\.~#?&\\/=]*))"
# HREF_REG = f"href=(?:(?:\"{URL_REG}\")|(?:'{URL_REG}'))"
URL_PAT = re.compile(URL_REG)
# HREF_PAT = re.compile(HREF_REG)


def split_tags(tags):
    if pd.isna(tags):
        return np.nan
    return tags[1:-1].split("><")


def filter_url(url: str):
    if not url.startswith("http"):
        # urlparse treats urls without schema differently, add https by default
        url = f"https://{url}"
    parsed_url = urlparse(url)
    return (
        parsed_url.path in ["", "/"]
        and parsed_url.query == ""
        and parsed_url.params == ""
        and parsed_url.fragment == ""
    )


def remove_code_blocks(post_body: str):
    pattern = r"<code.*?>.*?<\/code>"
    return re.sub(pattern, " ", post_body, flags=re.DOTALL)


def is_pdf(url: str):
    if not url.startswith("http"):
        url = f"https://{url}"
    parsed_url = urlparse(url)
    return parsed_url.path.endswith(".pdf")


def output(df: pd.DataFrame, post_type: int, url_type: str, name: str):
    df = df[df["PostTypeId"] == post_type]
    df = df[df[url_type].apply(lambda x: len(x) > 0)]
    if len(df) == 0:
        return
    df["Urls"] = df[url_type]
    if post_type == 1:
        columns = ["Id", "Urls"] + [col for col in df.columns if col in q_fields.keys()]
    else:
        columns = ["Id", "Urls"] + [col for col in df.columns if col in a_fields.keys()]
    df = df[columns]
    with open(f"{name}.jsonl", "a") as f:
        df.to_json(f, orient="records", lines=True)


def process_row(row: pd.Series):
    pdf_urls = set()
    other_urls = set()
    if row["PostTypeId"] == 1:
        row["Tags"] = row["Tags"][1:-1].split("><")
    post_body = remove_code_blocks(row["Body"])
    for m in URL_PAT.finditer(post_body):
        href_group = m.groups()
        if not any(href_group):
            continue
        if len(href_group) > 1:
            href = href_group[0] or href_group[1]
        else:
            href = href_group[0]
        if filter_url(href):
            # the url contains only domain name, almost definitely not a link to academic resources
            continue
        if is_pdf(href):
            pdf_urls.add(href)
        else:
            other_urls.add(href)
    row["PdfUrls"] = list(pdf_urls)
    row["OtherUrls"] = list(other_urls)
    return row


def process(data: pd.DataFrame):
    data = data.apply(process_row, axis=1)
    output(data, 1, "PdfUrls", "q_pdf")
    output(data, 1, "OtherUrls", "q_all")
    output(data, 2, "PdfUrls", "a_pdf")
    output(data, 2, "OtherUrls", "a_all")


def count_lines(filepath):
    try:
        result = subprocess.run(
            ["wc", "-l", filepath], stdout=subprocess.PIPE, text=True
        )
        return int(result.stdout.split()[0])
    except Exception as e:
        print(e)
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath")
    parser.add_argument("-c", "--concurrent", default=10)
    parser.add_argument("-s", "--chunksize", default=5000)
    parser.add_argument("-t", "--total", default=None)
    args = parser.parse_args()

    if not args.total:
        total = count_lines(args.filepath)
        print(f"Total lines: {total}")
    else:
        total = int(args.total)
    chunks = pd.read_json(
        args.filepath,
        lines=True,
        orient="record",
        chunksize=args.chunksize,
        dtype=fields_to_keep,
    )
    with tqdm(total=total) as pbar:
        with ProcessPoolExecutor(max_workers=int(args.concurrent)) as executor:
            futures = set()
            for chunk in chunks:
                futures.add(executor.submit(process, chunk))

                if len(futures) >= int(args.concurrent):
                    done, _ = wait(futures, return_when="FIRST_COMPLETED")
                    for future in done:
                        future.result()
                    futures -= done
                    pbar.update(args.chunksize)
            for future in as_completed(futures):
                future.result()
                pbar.update(1)
