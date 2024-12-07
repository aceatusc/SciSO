import asyncio
import html
from itertools import islice
import logging
import os
import re
import ssl
import traceback
import aiohttp
from aiolimiter import AsyncLimiter
import certifi
from dotenv import load_dotenv
from unidecode import unidecode
from .data_model import (
    BiblioResult,
    BiblioTask,
    ExtractResult,
    GrobidResult,
    ScrapeTask,
    ScraperConfig,
    TaskError,
    TaskStatus,
)
from .utils import (
    MAX_PATH_LENGTH,
    clean_spaces,
    ename,
    get_logger,
    is_str_valid,
    lowercase_dict,
    normalize_url,
    validate_stm,
    recursive_get as rg,
)
from wayback import WaybackClient, Memento, WaybackSession
from wayback.exceptions import (
    WaybackException,
    NoMementoError,
    MementoPlaybackError,
    BlockedByRobotsError,
    BlockedSiteError,
    RateLimitError,
)
from requests.exceptions import RequestException, ChunkedEncodingError
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
from urllib.parse import quote_plus
import cloudscraper

TITLE_LENGTH_THRESHOLD = 10
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1"
logger = get_logger("scraper")


class Scraper:
    def __init__(self, config: ScraperConfig, data_path: str, *args, **kwargs):
        self.config = config
        self.data_path = data_path
        if hasattr(self.config, "max_file_size"):
            self.config.max_file_size *= 1024**2

    def _download_path(self, resp_url: str):
        resp_url = normalize_url(resp_url)
        filename = resp_url.rsplit("/", 1)[1]
        filename = filename[: (MAX_PATH_LENGTH - len(self.data_path))]
        filepath = os.path.join(self.data_path, filename)
        counter = 1
        while os.path.exists(filepath):
            filepath = os.path.join(self.data_path, f"{filename}{counter}")
            counter += 1
        return filepath

    async def get(self, url: str) -> ScrapeTask:
        raise NotImplementedError


class HttpScraper(Scraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._init_context()
        self.session = None
        self.is_async = kwargs.get("is_async", True)

    def _init_context(self):
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.ssl_context.set_ciphers("DEFAULT")
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

    async def start(self):
        if self.session is not None:
            await self.stop()
        if self.is_async:
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=self.ssl_context),
                timeout=aiohttp.ClientTimeout(
                    connect=self.config.conn_timeout,
                    total=self.config.conn_timeout + self.config.read_timeout,
                ),
                raise_for_status=True,
                headers={"User-Agent": USER_AGENT},
            )
        else:
            self.session = cloudscraper.create_scraper()

    async def stop(self):
        if self.session is not None and self.is_async:
            await self.session.close()
        self.session = None

    async def handle_response(self, resp: aiohttp.ClientResponse) -> ScrapeTask:
        raise NotImplementedError

    async def get(self, url: str, retry_now: int = 3) -> ScrapeTask:
        try:
            if self.is_async:
                async with self.session.get(url) as resp:
                    return await self.handle_response(resp)
            resp = self.session.get(url, headers={"User-Agent": USER_AGENT})
            return await self.handle_response(resp)
        except aiohttp.InvalidURL:
            return ScrapeTask(TaskStatus.FAILED, TaskError("InvalidURL", url), False)
        except TimeoutError as e:
            return ScrapeTask(TaskStatus.FAILED, TaskError("Timeout", url))
        except (
            aiohttp.ClientSSLError,
            aiohttp.ClientConnectorError,
            aiohttp.ClientConnectorSSLError,
            aiohttp.ClientConnectionError,
            aiohttp.ClientConnectorCertificateError,
        ) as e:
            if url.startswith("https") and not url.startswith(
                "https://web.archive.org"
            ):
                logger and logger.warning(f"Retry with HTTP for {url} due to {e}")
                return await self.get(normalize_url(url, "http"))
            return ScrapeTask(
                TaskStatus.FAILED, TaskError("ConnectionError", f"{url}: {e}")
            )
        except aiohttp.ClientResponseError as e:
            if e.status == 429 and retry_now > 0:
                logger and logger.debug(f"{url} rate limited, wait for 60 seconds")
                await asyncio.sleep(30 * (4 - retry_now))
                return await self.get(url, retry_now - 1)
            # if e.status == 403:
            #     logger and logger.error(f"Error({type(e)}) for {url}: {e}")
            return ScrapeTask(
                TaskStatus.FAILED,
                TaskError(ename(e), f"{e.status} for {url}: {e}"),
            )
        except RecursionError as e:
            return ScrapeTask(TaskStatus.FAILED, TaskError(ename(e), f"{url}: {e}"))
        except Exception as e:
            tb = f"{e}\n{traceback.format_exc()}"
            if not isinstance(e, aiohttp.ClientError):
                logger and logger.error(f"Error({type(e)}) for {url}: {tb}")
            return ScrapeTask(TaskStatus.FAILED, TaskError(ename(e), f"{url}: {tb}"))


class WaybackScraper(Scraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        wayback_session = WaybackSession(
            0,
            timeout=(self.config.conn_timeout, self.config.read_timeout),
            search_calls_per_second=1,
            memento_calls_per_second=8,
            user_agent=USER_AGENT,
        )
        self.wayback = WaybackClient(wayback_session)
        period_no_burst = 1 / max(1, self.config.requests_per_second - 1)
        self.limiter = AsyncLimiter(1, period_no_burst)

    async def _get(self, url: str) -> ScrapeTask:
        raise NotImplementedError

    async def get(self, url: str) -> ScrapeTask:
        async with self.limiter:
            try:
                return await self._get(url)
            except (
                NoMementoError,
                BlockedByRobotsError,
                BlockedSiteError,
                MementoPlaybackError,
                ChunkedEncodingError,
            ) as e:
                return ScrapeTask(TaskStatus.FAILED, TaskError(ename(e), str(e)), False)
            except RateLimitError as e:
                freeze_time = (e.retry_after or 60) + 1
                logger and logger.debug(f"Wait {url} for {freeze_time} seconds")
                await asyncio.sleep(freeze_time)
                return await self.get(url)
            except WaybackException as e:
                logger and logger.warning(f"Wayback error for {url}: {e}")
                retry = "400" not in str(e) and "500" not in str(e)
                return ScrapeTask(TaskStatus.FAILED, TaskError(ename(e), str(e)), retry)
            except Exception as e:
                level = logging.DEBUG
                if e in (RequestException, TimeoutError):
                    msg = str(e)
                else:
                    msg = f"{e}\n{traceback.TracebackException.from_exception(e)}"
                    level = logging.ERROR
                logger and logger.log(level, msg)
                return ScrapeTask(TaskStatus.FAILED, TaskError(ename(e), msg))


class MementoScraper(WaybackScraper):
    def handle_response(self, resp: Memento) -> ScrapeTask:
        is_valid, err = validate_stm(
            resp,
            [
                ("status_code", "eq", 200),
                ("headers.Content-Length", "le", self.config.max_file_size),
            ],
        )
        if is_valid and is_str_valid(resp.text):
            return ScrapeTask(TaskStatus.SUCCESS, result=resp.text)
        return ScrapeTask(TaskStatus.FAILED, TaskError(err, resp.url))

    async def _get(self, url: str) -> ScrapeTask:
        loop = asyncio.get_event_loop()
        async with self.limiter:
            future = loop.run_in_executor(None, self.wayback.get_memento, url)
            timeout = self.config.conn_timeout + self.config.read_timeout + 3
            resp = await asyncio.wait_for(fut=future, timeout=timeout)
            with resp:
                return self.handle_response(resp)


class PatchScraper(WaybackScraper):
    filters = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limiter = AsyncLimiter(1, 1.25)

    async def _get(self, url: str) -> ScrapeTask:
        patch_links = []
        records = self.wayback.search(
            url, limit=200, filter_field=self.filters, collapse="digest"
        )
        for rec in islice(records, 10):
            try:
                if float(rg(rec, "length", "inf")) >= self.config.max_file_size:
                    continue
            except:
                # sometimes archive returns strange length value like a tuple
                pass
            if rec.raw_url is None:
                rec.raw_url = f"https://web.archive.org/web/{rec.timestamp.strftime('%Y%m%d%H%M%S')}if_/{rec.url}"
            patch_links.append(rec.raw_url)
        if patch_links:
            return ScrapeTask(TaskStatus.SUCCESS, result=patch_links)
        return ScrapeTask(TaskStatus.FAILED, TaskError("NoPatch", url), False)


class BiblioScraper(HttpScraper):
    base_url: str = ""
    separator: str = "*"
    pattern: str = r"[^a-zA-z0-9: ']"
    getter: dict = {}
    validations: list = []

    @classmethod
    def source_name(cls) -> str:
        return cls.__name__[:-7].lower()

    @classmethod
    def _build_author_list(cls, entry: dict) -> list:
        raise NotImplementedError

    @classmethod
    def _build_result(cls, data: dict, similarity: float) -> BiblioResult:
        retval = BiblioResult()
        for k, v in cls.getter.items():
            setattr(retval, k, rg(data, v) if isinstance(v, str) else v(data))
        retval.source = cls.source_name()
        retval.similarity = similarity
        retval.authors = cls._build_author_list(data)
        return retval

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = kwargs.get("headers", {})
        period_no_burst = 1 / max(1, self.config.requests_per_second - 1)
        self.limiter = AsyncLimiter(1, period_no_burst)
        self.measure = NormalizedLevenshtein()

    def _iter_data(self, data: dict):
        raise NotImplementedError

    def _clean_title_query(self, title: str) -> str:
        title = clean_spaces(title)
        if is_str_valid(title):
            title = unidecode(title)
            return self.separator.join(re.sub(self.pattern, " ", title).split())
        return None

    def _measure(self, query: str | list, title: str | list) -> float:
        if isinstance(query, list) and isinstance(title, list):
            query = " ".join(query).lower()
            title = " ".join(title).lower()
        if query := self._clean_title_query(query):
            query = query.lower()
            if title := self._clean_title_query(title):
                title = title.lower()
                return self.measure.similarity(query, title)
        return 0

    def _build_doi_query(self, doi: str) -> str:
        raise NotImplementedError

    def _build_title_query(self, title: str) -> str:
        raise NotImplementedError

    def _build_mag_query(self, mag: str) -> str:
        raise NotImplementedError

    def _validate_result(
        self, data: dict, raw_title: str, raw_doi: str
    ) -> BiblioResult:
        if raw_doi:
            if doi := rg(data, self.getter["external_ids"]).get("doi", ""):
                doi = doi.lower()
                raw_doi = raw_doi.lower()
                if doi in raw_doi or raw_doi in doi:
                    return self._build_result(data, 1.0)
        if not is_str_valid(raw_title):
            return self._build_result(data, None)
        resp_title = html.unescape(rg(data, self.getter["title"])).strip(".")
        resp_title_words = resp_title.split()
        raw_title_words = raw_title.split(self.separator)
        min_length = min(len(resp_title_words), len(raw_title_words))
        sim_threshold = (
            self.config.sim_threshold_l2
            if min_length < 5
            else self.config.sim_threshold_l1
        )
        if (sim := self._measure(raw_title_words, resp_title_words)) >= sim_threshold:
            return self._build_result(data, sim)
        if min_length >= TITLE_LENGTH_THRESHOLD:
            raw_head = raw_title_words[:TITLE_LENGTH_THRESHOLD]
            resp_head = resp_title_words[:TITLE_LENGTH_THRESHOLD]
            if (
                hsim := self._measure(raw_head, resp_head)
            ) >= self.config.sim_threshold_l2:
                return self._build_result(data, hsim)
            raw_tail = raw_title_words[-TITLE_LENGTH_THRESHOLD:]
            resp_tail = resp_title_words[-TITLE_LENGTH_THRESHOLD:]
            if (
                tsim := self._measure(raw_tail, resp_tail)
            ) >= self.config.sim_threshold_l2:
                return self._build_result(data, tsim)
            sim = max(sim, hsim, tsim)
        if sim >= 0.6:
            logger and logger.debug(
                f"Similarity between `{raw_title}` and `{resp_title}` ({sim}) is below threshold"
            )
        return None

    def _build_response(self, data: dict, title: str, doi: str) -> BiblioTask:
        for e in self._iter_data(data):
            if (r := self._validate_result(e, title, doi)) is not None:
                return BiblioTask(TaskStatus.SUCCESS, result=r)
        return BiblioTask(TaskStatus.FAILED, TaskError("NotFound", "No match"))

    async def parse_response(
        self, resp: aiohttp.ClientResponse, title: str = None, doi: str = None
    ) -> BiblioResult:
        reason = ""
        data = await resp.json()
        is_valid, reason = validate_stm(
            resp, [("status", "eq", 200)], data, self.validations
        )
        if is_valid:
            return self._build_response(data, title, doi)
        return BiblioTask(TaskStatus.FAILED, TaskError("Invalid Response", reason))

    async def get(self, raw: ExtractResult | GrobidResult) -> BiblioTask:
        if isinstance(raw, ExtractResult):
            if raw.doi:
                doi_res = await self._fetch(
                    self.doi,
                    title=raw.title if raw.doi.is_wildcard else None,
                    doi=raw.doi.value,
                )
                if doi_res.status == TaskStatus.SUCCESS:
                    return doi_res
            if raw.mag:
                mag_res = await self._fetch(self.mag, mag=raw.mag)
                if mag_res.status == TaskStatus.SUCCESS:
                    return mag_res
        if raw.title:
            return await self._fetch(self.title, title=raw.title)
        return BiblioTask(TaskStatus.FAILED, TaskError("No available information", raw))

    async def _fetch(self, func, retry_now: int = 3, *args, **kwargs) -> BiblioTask:
        try:
            return await func(*args, **kwargs)
        except TimeoutError as e:
            return BiblioTask(TaskStatus.FAILED, TaskError("Timeout", kwargs))
        except aiohttp.ClientResponseError as e:
            if e.status == 429 and retry_now > 0:
                logger and logger.debug(f"{kwargs} rate limited, wait for 60 seconds")
                await asyncio.sleep(30 * (4 - retry_now))
                return await self._fetch(func, retry_now - 1, *args, **kwargs)
            logger and logger.info(f"Error({type(e)}) for {kwargs}: {e}")
            return BiblioTask(
                TaskStatus.FAILED,
                TaskError(ename(e), f"{e.status} for {kwargs}: {e}"),
            )
        except Exception as e:
            tb = f"{e}\n{traceback.format_exc()}"
            logger and logger.error(f"Error({type(e)}) for {kwargs}: {tb}")
            return BiblioTask(TaskStatus.FAILED, TaskError(ename(e), f"{kwargs}: {tb}"))

    async def title(self, title: str) -> BiblioTask:
        key = self._clean_title_query(title)
        async with self.limiter:
            async with self.session.get(**self._build_title_query(key)) as resp:
                res = await self.parse_response(resp, key)
                if res.status != TaskStatus.SUCCESS:
                    key_words = key.split(self.separator)
                    if len(key_words) > TITLE_LENGTH_THRESHOLD:
                        head_key = self.separator.join(
                            key_words[:TITLE_LENGTH_THRESHOLD]
                        )
                        head_res = await self.title(head_key)
                        if head_res.status == TaskStatus.SUCCESS:
                            return head_res
                        tail_key = self.separator.join(
                            key_words[-TITLE_LENGTH_THRESHOLD:]
                        )
                        tail_res = await self.title(tail_key)
                        if tail_res.status == TaskStatus.SUCCESS:
                            return tail_res
                return res

    async def doi(self, doi: str, title: str = None) -> BiblioTask:
        if len(doi.split("/")) < 2:
            return BiblioTask(TaskStatus.FAILED, TaskError("InvalidDOI", doi))
        async with self.limiter:
            async with self.session.get(**self._build_doi_query(doi)) as resp:
                res = await self.parse_response(resp, title, doi)
                if res.status != TaskStatus.SUCCESS:
                    shorten_doi = "/".join(doi.split("/")[:-1])
                    return await self.doi(shorten_doi, title)
                return res

    async def mag(self, mag: str) -> BiblioTask:
        async with self.limiter:
            async with self.session.get(**self._build_mag_query(mag)) as resp:
                return await self.parse_response(resp)


class OpenAlexScraper(BiblioScraper):
    base_url = "https://api.openalex.org/works"
    separator = " "
    getter = {
        "source_id": "id",
        "title": "title",
        "venue": "primary_location.source.display_name",
        "year": "publication_year",
        "open_access": "open_access.is_oa",
        "citation_count": "cited_by_count",
        "concepts": "concepts",
        "external_ids": lambda x: lowercase_dict(rg(x, "ids", default={})),
        "type": lambda x: rg(x, "type_crossref") or rg(x, "type"),
        "abstract": "abstract_inverted_index",
    }
    fields = [
        "id",
        "title",
        "authorships",
        "primary_location",
        "publication_year",
        "open_access",
        "cited_by_count",
        "concepts",
        "type",
        "type_crossref",
        "ids",
        "abstract_inverted_index",
    ]
    validations = [("meta.count", "gt", 0)]

    @classmethod
    def _build_author_list(cls, entry: dict) -> list:
        return [
            {
                "name": rg(author, "author.display_name"),
                "id": rg(author, "author.id"),
                "orcid": rg(author, "author.orcid"),
                "countries": rg(author, "countries"),
                "institutions": rg(
                    author,
                    "institutions",
                    default=rg(author, "raw_affiliation_strings"),
                ),
            }
            for author in rg(entry, "authorships", [])
        ]

    def __init__(self, *args, **kwargs):
        load_dotenv()
        if os.getenv("ALEX_EMAIL") is None:
            raise ValueError("OpenAlex email is not set")
        super().__init__(*args, **kwargs)

    def _build_doi_query(self, doi: str) -> str:
        return {
            "url": self.base_url,
            "params": {
                "filter": f"doi:{doi}",
                "select": ",".join(self.fields),
                "mailto": os.getenv("ALEX_EMAIL"),
            },
            "headers": self.headers,
        }

    def _build_title_query(self, title: str) -> str:
        return {
            "url": self.base_url,
            "params": {
                "filter": f"title.search:{title}",
                "select": ",".join(self.fields),
                "mailto": os.getenv("ALEX_EMAIL"),
            },
            "headers": self.headers,
        }

    def _build_mag_query(self, mag: str) -> str:
        return {
            "url": self.base_url,
            "params": {
                "filter": f"mag:{mag}",
                "select": ",".join(self.fields),
                "mailto": os.getenv("ALEX_EMAIL"),
            },
            "headers": self.headers,
        }

    def _iter_data(self, data: dict):
        for entry in rg(data, "results", default=[]):
            yield entry


class S2Scraper(BiblioScraper):
    base_url = "https://api.semanticscholar.org/graph/v1/paper"
    getter = {
        "source_id": "paperId",
        "title": "title",
        "venue": lambda x: rg(x, "publicationVenue") or rg(x, "venue"),
        "year": "year",
        "open_access": "isOpenAccess",
        "citation_count": "citationCount",
        "external_ids": lambda x: lowercase_dict(rg(x, "externalIds", default={})),
        "type": "publicationTypes",
        "abstract": lambda x: rg(x, "abstract") or rg(x, "tldr.text"),
        "embedding": "embedding",
    }
    fields = [
        "title",
        "authors",
        "venue",
        "publicationVenue",
        "year",
        "citationCount",
        "isOpenAccess",
        "externalIds",
        "publicationTypes",
        "abstract",
        "tldr",
    ]
    validations = [("total", "gt", 0)]

    @classmethod
    def _build_author_list(cls, entry: dict) -> list:
        return [
            {"name": author["name"], "id": author["authorId"]}
            for author in rg(entry, "authors", default=[])
        ]

    def __init__(self, *args, **kwargs):
        load_dotenv()
        key = os.getenv("S2_API_KEY")
        if key is None:
            raise ValueError("S2 API key is not set")
        kwargs["headers"] = {"x-api-key": key}
        super().__init__(*args, **kwargs)

    def _build_doi_query(self, doi: str) -> str:
        url = (
            f"{self.base_url}/arXiv:{doi.split('arxiv.')[1]}"
            if "arxiv" in doi
            else f"{self.base_url}/DOI:{doi}"
        )
        return {
            "url": url,
            "params": {"fields": ",".join(self.fields), "limit": 5},
            "headers": self.headers,
        }

    def _build_title_query(self, title: str) -> str:
        return {
            "url": f"{self.base_url}/search",
            "params": {"query": title, "fields": ",".join(self.fields), "limit": 5},
            "headers": self.headers,
        }

    def _build_mag_query(self, mag: str) -> str:
        return {
            "url": f"{self.base_url}/mag:{mag}",
            "params": {"fields": ",".join(self.fields), "limit": 5},
            "headers": self.headers,
        }

    def _iter_data(self, data: dict):
        if "data" not in data:
            yield data
        else:
            for entry in rg(data, "data", default=[]):
                yield entry


class CrossrefScraper(BiblioScraper):
    base_url = "https://api.crossref.org/works"
    getter = {
        "source_id": "DOI",
        "title": lambda x: rg(x, "title")[0] if rg(x, "title") else "",
        "venue": lambda x: (
            rg(x, "container-title")[0]
            if rg(x, "container-title")
            else rg(x, "publisher", default="")
        ),
        "year": lambda x: (
            rg(x, "created.date-parts")[0][0] if rg(x, "created.date-parts") else ""
        ),
        "open_access": "",
        "citation_count": "references-count",
        "concepts": "content-domain",
        "external_ids": lambda x: {"doi": x.get("DOI", "")},
        "type": "type",
        "abstract": "abstract",
    }
    fields = [
        "DOI",
        "prefix",
        "title",
        "event",
        "content-domain",
        "published",
        "abstract",
        "references-count",
        "type",
        "publisher",
        "author",
        "container-title",
        "alternative-id",
        "created",
    ]
    validations = []

    @classmethod
    def _build_author_list(cls, entry):
        return [
            {
                "name": author.get("given", "") + author.get("family", ""),
                "institutions": rg(author, "affiliation.name", default=""),
                "orcid": author.get("orcid", ""),
            }
            for author in entry.get("author", [])
        ]

    def __init__(self, *args, **kwargs):
        load_dotenv()
        if os.getenv("CROSSREF_EMAIL") is None:
            raise ValueError("Crossref email is not set")
        super().__init__(*args, **kwargs)

    def _build_doi_query(self, doi):
        return {
            "url": self.base_url + f"/{doi}",
            "params": {},
            "headers": self.headers,
        }

    def _build_title_query(self, title):
        return {
            "url": self.base_url,
            "params": {
                "select": ",".join(self.fields),
                "mailto": os.getenv("CROSSREF_EMAIL"),
                "query": title,
                "rows": 5,
                "sort": "relevance",
            },
            "headers": self.headers,
        }

    def _build_mag_query(self, mag):
        return {
            "url": self.base_url,
            "params": {
                "select": ",".join(self.fields),
                "mailto": os.getenv("CROSSREF_EMAIL"),
                "query": mag,
                "rows": 5,
                "sort": "relevance",
            },
            "headers": self.headers,
        }

    def _iter_data(self, data):
        for entry in rg(data, "message.items", default=[rg(data, "message", [])]):
            yield entry


class DBLPScraper(BiblioScraper):
    base_url = "https://dblp.org/search/publ/api"
    getter = {
        "source_id": "@id",
        "title": "info.title",
        "venue": "info.venue",
        "year": "info.year",
        "citation_count": "info.citation",
        "type": "info.type",
        "open_access": lambda x: rg(x, "info.access") == "open",
        "external_ids": lambda x: {
            "doi": rg(x, "info.doi"),
            "dblp": rg(x, "info.key"),
        },
    }
    validations = [
        ("result.status.@code", "eq", 200),
        ("result.hits.@total", "gt", 0),
    ]

    def _iter_data(self, data: dict):
        for entry in rg(data, "result.hits.hit", default=[]):
            if "info" in entry and isinstance(entry["info"], dict):
                yield entry

    # def _build_params(self, key: str) -> dict:
    # return {"q": key, "format": "json"}

    @classmethod
    def _build_author_list(self, entry: dict) -> list[dict]:
        if isinstance(rg(entry, "info.authors.author", default=[]), list):
            return [
                {"name": rg(author, "text"), "id": rg(author, "@pid")}
                for author in rg(entry, "info.authors.author", default=[])
            ]
        return [
            {
                "name": rg(entry, "info.authors.author.text"),
                "id": rg(entry, "info.authors.author.@pid"),
            }
        ]

    def _build_doi_query(self, doi: str) -> str:
        return {
            "url": self.base_url,
            "params": {"q": doi, "h": 5, "format": "json"},
            "headers": self.headers,
        }

    def _build_title_query(self, title: str) -> str:
        return {
            "url": self.base_url,
            "params": {"q": title, "h": 5, "format": "json"},
            "headers": self.headers,
        }

    def _build_mag_query(self, mag: str) -> str:
        return {
            "url": self.base_url,
            "params": {"q": mag, "h": 5, "format": "json"},
            "headers": self.headers,
        }
