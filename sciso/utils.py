import asyncio
import logging
from logging import LogRecord
import shutil
import pandas as pd
from requests.structures import CaseInsensitiveDict
from functools import reduce, wraps
import operator
import re
from requests.exceptions import HTTPError
from http import HTTPStatus
from urllib.parse import unquote, urlparse, urlunparse
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
import urllib3
import os
from nltk.corpus import wordnet2021
import nltk

nltk.download("wordnet", quiet=True)
nltk.download("wordnet2021", quiet=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
MAX_PATH_LENGTH = 250
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
if os.path.isdir(LOG_DIR):
    shutil.rmtree(LOG_DIR)
os.mkdir(LOG_DIR)


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record: LogRecord) -> None:
        tqdm.write(self.format(record))


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    # Set up console logging handler
    console_handler = TqdmLoggingHandler()
    console_handler.setLevel(getattr(logging, level))
    console_format = logging.Formatter("%(asctime)s-%(name)s: %(message)s")
    console_handler.setFormatter(console_format)

    # Set up file logging handler
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"{name}.log"))
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter("%(asctime)s-%(name)s(%(levelname)s): %(message)s")
    file_handler.setFormatter(file_format)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set logger level to the lowest level
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def correct_hyphenation(text):
    words = text.split()
    corrected_words = []

    for word in words:
        if "-" in word:
            if word.startswith("-") or word.endswith("-"):
                corrected_words.append(word.replace("-", ""))
                continue
            parts = word.split("-")
            dehyphenated = word.replace("-", "")
            if any(
                not wordnet2021.synsets(part) for part in parts
            ) and wordnet2021.synsets(dehyphenated):
                corrected_words.append("".join(parts))
            else:
                corrected_words.append(word)
        else:
            corrected_words.append(word)

    return " ".join(corrected_words)


def log_and_cont(logger, level="INFO"):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                args_str = ", ".join(repr(arg) for arg in args)
                kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
                all_args = ", ".join(filter(None, [args_str, kwargs_str]))
                logger.log(
                    getattr(logging, level),
                    f"Error in {func.__name__}({all_args}): {e}",
                )

        return wrapper

    return decorator


def log_execution_time(logger, level="DEBUG"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_ts = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            finally:
                end_ts = time.perf_counter()
                arg = (
                    args[1]
                    if args and len(args) > 1
                    else args[0] if args and len(args) else ""
                )
                logger.log(
                    getattr(logging, level),
                    f"{func.__qualname__}({arg}) took {end_ts - start_ts:.2f}s",
                )
            return result

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_ts = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
            finally:
                end_ts = time.perf_counter()
                arg = (
                    args[1]
                    if args and len(args) > 1
                    else args[0] if args and len(args) else ""
                )
                logger.log(
                    getattr(logging, level),
                    f"{func.__qualname__}({arg}) took {end_ts - start_ts:.2f}s",
                )
            return result

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper

    return decorator


def clean_spaces(text: str) -> str:
    if not is_str_valid(text):
        return None
    return re.sub(r"\s+", " ", text).strip()


def normalize_url(url: str, scheme: str = "https"):
    url = url.strip()
    parsed_url = urlparse(url)
    if parsed_url.scheme == "":
        parsed_url = urlparse(f"{scheme}://{url}")
    if parsed_url.scheme not in ["http", "https"] or parsed_url.scheme != scheme:
        parsed_url = parsed_url._replace(scheme=scheme)
    # should not clean query/params/fragment as some urls rely on these information,
    # e.g., https://credible.i3s.unice.fr/lib/exe/fetch.php?media=credible-13-2-v1-rdb2rdf.pdf
    normalized_netloc = unquote(parsed_url.netloc)
    normalized_path = unquote(parsed_url.path)
    normalized_params = unquote(parsed_url.params)
    normalized_query = unquote(parsed_url.query)
    normalized_fragment = unquote(parsed_url.fragment)
    normalized_url = urlunparse(
        (
            parsed_url.scheme,
            normalized_netloc,
            normalized_path,
            normalized_params,
            normalized_query,
            normalized_fragment,
        )
    )
    return normalized_url


def recursive_get(obj: object | dict, keys: str, default=None, insensitive=True):
    if callable(keys):
        return keys(obj)
    keys_list = keys.split(".")

    def get_item(obj, key):
        if isinstance(obj, dict) or hasattr(obj, "get"):
            return (
                CaseInsensitiveDict(obj).get(key, default)
                if insensitive
                else obj.get(key, default)
            )
        return getattr(obj, key, default)

    return reduce(lambda d, k: get_item(d, k), keys_list, obj)


def validate_stm(*args, logger=None, level="DEBUG") -> (bool, str):
    if len(args) % 2 != 0:
        raise ValueError("Arguments must be provided in pairs of (obj, statements).")
    for i in range(0, len(args), 2):
        obj, statements = args[i], args[i + 1]
        for stm in statements:
            left, op, right = stm
            value = None
            reason = ""
            # recursively evaluate the left and right side of the statement
            if isinstance(left, tuple):
                value, left_reason = validate_stm(obj, [left])
                reason += f"{left_reason};"
            if isinstance(right, tuple):
                right, right_reason = validate_stm(obj, [right])
                reason += f"{right_reason};"
            # always assume left is the object and right is the expected value
            value = value if value is not None else recursive_get(obj, left)
            if value is None:
                url = getattr(obj, "url", "")
                logger and logger.debug(f"Cannot access {left} for {url}")
                continue
            # should not convert other types like list, dict, etc.
            if isinstance(right, int):
                value = int(value)
            elif isinstance(right, float):
                value = float(value)
            elif isinstance(right, bool):
                value = bool(value)
            elif isinstance(right, str):
                # hardcode to strip the string
                # (some websites return "content-type: application/pdf; qs=0.01; charset=utf-8")
                value = str(value).strip().split(";")[0]
            # evaluate the statement
            if not getattr(operator, op)(value, right):
                reason += f"{left} = {value}, expected {op} {right}"
                if hasattr(obj, "url"):
                    reason += f" for {obj.url}"
                logger and logger.log(getattr(logging, level), reason)
                return False, reason
    return True, ""


def retrieve_soup(
    method: str,
    res: object | dict,
    res_attr: str,
    soup: BeautifulSoup,
    logger: logging.Logger,
    *args,
    **kwargs,
):
    try:
        soup_method = getattr(soup, method)
        if soup_method:
            tag_result = soup_method(*args, **kwargs)
            if tag_result:
                result_text = tag_result.get_text(" ", strip=True)
                if not is_str_valid(result_text):
                    result_text = tag_result.get("content", None)
                setattr(res, res_attr, result_text)
    except Exception as e:
        logger.debug(f"Error in retrieving {res_attr}: {e}")


NO_RETRY_CODES = {
    HTTPStatus.UNAUTHORIZED,
    HTTPStatus.NOT_FOUND,
    HTTPStatus.METHOD_NOT_ALLOWED,
    HTTPStatus.NO_CONTENT,
    HTTPStatus.GONE,
    HTTPStatus.UNPROCESSABLE_ENTITY,
    HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
    HTTPStatus.NOT_ACCEPTABLE,
}


def should_retry(err: HTTPError) -> bool:
    return err.response.status_code not in NO_RETRY_CODES


def get_domain(url: str) -> str:
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.split(":")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


domains_to_mutate = {
    "github.com",
    "dropbox.com",
    "web.archive.org",
}

archive_url_pattern = re.compile(r"^https?://web.archive.org/web/\d{14}(?:i[\w]_)?/.+$")


def is_archive_url(url: str) -> bool:
    return archive_url_pattern.match(url) is not None


def is_path_exist(path: str) -> bool:
    return path is not None and len(path) <= MAX_PATH_LENGTH and os.path.isfile(path)


def transform_access_url(url: str):
    domain = get_domain(url)
    if domain not in domains_to_mutate:
        return url
    if domain == "github.com":
        return url.replace("github.com", "raw.githubusercontent.com").replace(
            "/blob/", "/"
        )
    if domain == "dropbox.com":
        return url.replace("dropbox.com", "dl.dropboxusercontent.com")
    if domain == "web.archive.org":
        ts_pattern = re.compile(r"(/web/)(\d{14})/")
        return ts_pattern.sub(r"\1\2if_/", url)


def lowercase_dict(d: dict) -> dict:
    return {k.lower(): v for k, v in d.items()}


def is_str_valid(title: str) -> bool:
    return title is not None and isinstance(title, str) and len(title.strip()) > 0


def get_abs_path(path: str, rel_file: str = None) -> str:
    if not os.path.isabs(path):
        return os.path.join(
            os.path.dirname(os.path.realpath(rel_file or __file__)), path
        )
    return path


def ename(e: Exception) -> str:
    return type(e).__name__


def chunker(seq: pd.DataFrame, size):
    for pos in range(0, len(seq), size):
        yield seq.iloc[pos : pos + size]
