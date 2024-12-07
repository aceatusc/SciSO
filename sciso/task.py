import asyncio
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import os
import pickle
import re
import traceback
from urllib.parse import urlparse
from PyPDF2 import PdfReader
import aiohttp
from bs4 import BeautifulSoup
import lmdb
import pandas as pd
from tqdm import tqdm

from .scraper import (
    HttpScraper,
    CrossrefScraper,
    OpenAlexScraper,
    DBLPScraper,
    S2Scraper,
    Scraper,
    WaybackScraper,
)
from .data_model import (
    BaseTask,
    BiblioResult,
    BiblioTask,
    ExtractAttr,
    ExtractResult,
    ExtractTask,
    GrobidResult,
    HarvestAgentConfig,
    HarvestArchiveConfig,
    HarvestExtractConfig,
    HarvestGrobidConfig,
    HarvestOriginConfig,
    TaskError,
    TaskStatus,
    UrlEntry,
)
from .utils import (
    clean_spaces,
    correct_hyphenation,
    ename,
    get_abs_path,
    get_logger,
    is_archive_url,
    is_path_exist,
    is_str_valid,
    retrieve_soup,
    recursive_get as rg,
)
from grobid_client.grobid_client import GrobidClient

logger = get_logger("harvest")
grobid_error_status = {
    204: "No content",
    400: "Bad request",
    500: "Internal server error",
    503: "Service unavailable at the moment",
}
DOI_PAT = re.compile(
    r"\b(?P<doi>10\.\d{4,}(?:[.][0-9]+)*/(?:(?![&\"\'<>\s])[\w\-./#])+)\b",
    re.IGNORECASE,
)
MIN_TITLE_LENGTH = 3


class HarvestTask:
    @classmethod
    def task_source(cls) -> str:
        return cls.__name__.lower()[7:]

    @classmethod
    def task_name(cls) -> str:
        return f"{cls.task_source()}_task"

    @classmethod
    def task_config_name(cls) -> str:
        return f"harvest_{cls.task_source()}"

    @classmethod
    def task_result(cls, entry: UrlEntry) -> BaseTask:
        return getattr(entry, cls.task_name())

    @classmethod
    def is_task_done(cls, entry: UrlEntry) -> bool:
        return cls.task_result(entry).status == TaskStatus.SUCCESS

    def has_rate_control(self, entry: UrlEntry) -> bool:
        task_status = self.task_result(entry)
        return hasattr(self.config, "freeze_time") and hasattr(
            task_status, "last_attempt_ts"
        )

    def _should_rate_control(self, entry: UrlEntry, now_ts: pd.Timestamp) -> bool:
        return self.has_rate_control(entry) and (
            now_ts - self.task_result(entry).last_attempt_ts
            <= pd.Timedelta(hours=self.config.freeze_time)
        )

    def _should_retry(self, entry: UrlEntry) -> bool:
        return self.task_result(entry).retry

    def __init__(
        self,
        config_tp: object,
        entries: lmdb.Environment,
        config_path: str,
        data_path: str,
        x_progress: bool,
    ):
        self.entries = entries
        with open(config_path, "r") as f:
            config_json = json.load(f)
        if config_tp:
            self.config = config_tp(**config_json[self.task_config_name()])
        else:
            logger.warning(f"Config for {self.task_config_name()} not found")
            self.config = None
        self.data_path = get_abs_path(data_path, __file__)
        self.x_progress = x_progress

    async def _operation(self, entry: UrlEntry) -> None:
        raise NotImplementedError

    @contextmanager
    def _load(self, row: pd.Series, txn: lmdb.Transaction) -> UrlEntry:
        url = row.Url
        hash_url = hashlib.sha512(url.encode("utf-8")).hexdigest()
        key = row.Key
        try:
            if lmdb_record := txn.get(hash_url.encode("utf-8")):
                try:
                    entry = pickle.loads(lmdb_record)
                except Exception as e:
                    logger.debug(f"Failed to load {url}: {e}, creating new entry")
                    entry = UrlEntry(url, key=key)
            else:
                entry = UrlEntry(url, key=key)
            yield entry
        finally:
            if self.x_progress:
                self.pbar.update(1)
            try:
                txn.put(hash_url.encode("utf-8"), pickle.dumps(entry))
            except Exception as e:
                tb = f"{e}\n{traceback.format_exc()}"
                logger.error(f"Failed to dump {url} to the database: {tb}")

    def should_skip(self, entry: UrlEntry, now_ts: pd.Timestamp) -> tuple[bool, bool]:
        raise NotImplementedError

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def _process(self, row: pd.Series, txn: lmdb.Transaction) -> bool:
        with self._load(row, txn) as entry:
            now_ts = pd.Timestamp.now()
            try:
                is_skipping, skip_value = self.should_skip(entry, now_ts)
                if is_skipping:
                    return skip_value
                await self._operation(entry)
                if self.has_rate_control(entry):
                    self.task_result(entry).last_attempt_ts = now_ts
            except Exception as e:
                tb = f"{e}\n{traceback.format_exc()}"
                logger.error(f"{self.task_name()} when processing {entry.url}: {tb}")
                self.task_result(entry).status = TaskStatus.FAILED
                self.task_result(entry).error = TaskError(ename(e), str(e))
                return False
        return self.is_task_done(entry)

    async def _worker(self, queue: asyncio.Queue, txn: lmdb.Transaction) -> None:
        idx_done = []
        try:
            while True:
                row = await queue.get()
                try:
                    if await self._process(row, txn):
                        idx_done.append(row.Index)
                except asyncio.CancelledError:
                    return idx_done
                except Exception as e:
                    tb = f"{e}\n{traceback.format_exc()}"
                    logger.error(f"{self.task_name()} #{row.Index}({row.Url}): {tb}")
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            pass
        return idx_done

    async def _dispatch(self, df: pd.DataFrame, txn: lmdb.Transaction) -> pd.Index:
        task_queue = asyncio.Queue(maxsize=self.config.concurrent + 1)
        workers = [
            asyncio.create_task(self._worker(task_queue, txn))
            for _ in range(task_queue.maxsize)
        ]
        for row in df.itertuples():
            await task_queue.put(row)
        await task_queue.join()
        for worker in workers:
            worker.cancel()
        res = await asyncio.gather(*workers, return_exceptions=True)
        done_tasks = pd.Index([idx for r in res if isinstance(r, list) for idx in r])
        return done_tasks

    async def __call__(self, df: pd.DataFrame) -> pd.Index:
        if len(df) == 0:
            return pd.Index([])
        if self.x_progress:
            self.pbar = tqdm(total=len(df), desc=self.task_config_name())
        try:
            with self.entries.begin(write=True) as txn:
                retval = await self._dispatch(df, txn)
        except Exception as e:
            tb = f"{e}\n{traceback.format_exc()}"
            logger.error(f"Error when dispatching {self.task_name()}: {tb}")
        if self.x_progress:
            self.pbar.close()
        return retval


class HarvestScraper(HarvestTask):
    def __init__(self, scraper_tp: Scraper, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scraper: Scraper = scraper_tp(self.config, self.data_path)

    def should_skip(self, entry: UrlEntry, now_ts: pd.Timestamp) -> tuple[bool, bool]:
        if self._should_rate_control(entry, now_ts):
            return True, self.is_task_done(entry)
        return self.is_task_done(entry), True


class HarvestHttp(HarvestScraper):
    async def __aenter__(self):
        await self.scraper.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.scraper.stop()


class HarvestOrigin(HarvestHttp):
    def __init__(self, scraper_tp: HttpScraper, *args, **kwargs):
        super().__init__(scraper_tp, HarvestOriginConfig, *args, **kwargs)

    def should_skip(self, entry: UrlEntry, now_ts: pd.Timestamp) -> tuple[bool, bool]:
        is_origin_done = self.is_task_done(entry)
        # # temp, for arxiv
        # if is_origin_done and (
        #     entry.openalex_task.status == TaskStatus.SUCCESS
        #     or entry.crossref_task.status == TaskStatus.SUCCESS
        # ):
        #     return True, True
        # return not self._should_retry(entry), False
        # if successfully downloaded from origin, but failed to match, refer to harvest_dblp
        # to refer, simply skip with True and it will passed to harvest_grobid, which will
        # then skip to harvest_dblp (see `should_skip` in HarvestGrobid)
        if is_origin_done and entry.extract_task.status == TaskStatus.SUCCESS:
            return True, True
        if self._should_rate_control(entry, now_ts):
            # if successfully downloaded from origin, but failed to parse & under rate control
            # do not redownload as it is under rate control, skip with True (the file failed to parse
            # should not have been deleted)
            # if failed to download from origin and under rate control, skip with False
            return True, is_origin_done
        # if successfully downloaded from origin and not under rate control, but failed to parse
        if is_origin_done:
            # if should retry parsing, do not skip, redownload from origin and remove the last downloaded file
            if entry.extract_task.retry and isinstance(
                entry.extract_task.result, GrobidResult
            ):
                # grobid result means the origin_task.result is the filepath to a pdf file, remove it
                if os.path.isfile(entry.origin_task.result):
                    os.remove(entry.origin_task.result)
                return False, None
            # if should not retry parsing, skip with True
            return True, True
        # if failed to download from origin and not under rate control, check if it should be retried
        return not self._should_retry(entry), False

    async def _operation(self, entry: UrlEntry) -> None:
        entry.origin_task = await self.scraper.get(entry.url)


class HarvestWayback(HarvestScraper):
    @classmethod
    def is_task_done(cls, entry: UrlEntry) -> bool:
        return (
            super().is_task_done(entry)
            or entry.origin_task.status == TaskStatus.SUCCESS
        )

    def __init__(self, scraper_tp: WaybackScraper, *args, **kwargs):
        super().__init__(scraper_tp, HarvestArchiveConfig, *args, **kwargs)


class HarvestPatch(HarvestWayback):
    def should_skip(self, entry: UrlEntry, now_ts: pd.Timestamp) -> tuple[bool, bool]:
        if is_archive_url(entry.url):
            return True, True
        # if successfully retrived patch URLs from web archive, skip with True
        if self.is_task_done(entry):
            return True, True
        # if failed to retrieve patch URLs, but under rate control, skip with False
        if self._should_rate_control(entry, now_ts):
            return True, False
        # if failed to retrieve patch URLs, and not under rate control, check if it should be retried
        return not self._should_retry(entry), False

    async def _operation(self, entry: UrlEntry) -> None:
        entry.patch_task = await self.scraper.get(entry.url)


class HarvestMemento(HarvestWayback):
    def should_skip(self, entry: UrlEntry, now_ts: pd.Timestamp) -> tuple[bool, bool]:
        # HarvestMemento is only called if HarvestOrigin succeeds
        # if grobid task is done, skip because all it needs to retry is harvest_biblio
        if entry.extract_task.status == TaskStatus.SUCCESS:
            return True, True
        # if the task is referred to HarvestMemento, do not skip
        if is_archive_url(entry.url):
            # if should retry parsing, do not skip, redownload from archive and remove the last downloaded file
            if entry.extract_task.retry:
                if os.path.isfile(entry.memento_task.result):
                    os.remove(entry.memento_task.result)
                return False, None
            # if should not retry parsing, skip with True
            return True, True
        # if successfully downloaded from archive, but failed to match, refer to harvest_dblp
        # to refer, simply skip with True and it will passed to harvest_grobid, which will
        # then skip to harvest_dblp (see `should_skip` in HarvestGrobid)
        is_archive_done = self.is_task_done(entry)
        if is_archive_done and entry.extract_task.status == TaskStatus.SUCCESS:
            return True, True
        if self._should_rate_control(entry, now_ts):
            # if successfully downloaded from archive, but failed to parse & under rate control
            # skip with True (the file failed to parse should not have been deleted)
            # if failed to download from archive and under rate control, skip with False
            return True, is_archive_done
        # if successfully downloaded from archive and not under rate control, but failed to parse
        # do not skip, redownload from archive and remove the last downloaded file
        if is_archive_done:
            # if should retry parsing, do not skip, redownload from archive and remove the last downloaded file
            if entry.extract_task.retry:
                if os.path.isfile(entry.memento_task.result):
                    os.remove(entry.memento_task.result)
                return False, None
            # if should not retry parsing, skip with True
            return True, True
        # if failed to download from archive and not under rate control, check if it should be retried
        return not self._should_retry(entry), False

    async def _operation(self, entry: UrlEntry) -> None:
        retry_task = None
        if is_archive_url(entry.url):
            entry.memento_task = await self.scraper.get(entry.url)
        elif entry.patch_task.status == TaskStatus.SUCCESS:
            for archive in entry.patch_task.result:
                entry.memento_task = await self.scraper.get(archive)
                if self.is_task_done(entry):
                    return
                if entry.memento_task.retry:
                    retry_task = entry.memento_task
            if retry_task:
                entry.memento_task = retry_task  # you may at least retry


class HarvestExtract(HarvestTask):
    def __init__(self, source: str, *args, **kwargs):
        super().__init__(HarvestExtractConfig, *args, **kwargs)
        self.source = source
        self.metarules = self.config.metarules.get(source, {})
        if not self.metarules:
            logger.warning(
                f"Metarules for {self.source} not found, using wildcards to match DOI and article title"
            )

    def should_skip(self, entry: UrlEntry, now_ts: pd.Timestamp) -> tuple[bool, bool]:
        if self.is_task_done(entry):
            return True, True
        return not self._should_retry(entry), False

    def _extract_from_html(self, content: str) -> ExtractResult:
        retval = ExtractResult()
        try:
            soup = BeautifulSoup(content, "html.parser")
            for meta, rules in self.metarules.items():
                retrieve_soup("find", retval, meta, soup, logger, **rules)
        except Exception as e:
            logger.error(f"Error when parsing: {e}")
        # special case for arxiv id
        if retval.doi is not None:
            matches = DOI_PAT.search(retval.doi)
            retval.doi = ExtractAttr(matches.group("doi"), False) if matches else None
        if retval.doi is None:
            matches = DOI_PAT.search(content)
            retval.doi = ExtractAttr(matches.group("doi"), True) if matches else None
        if not is_str_valid(retval.title):
            matches = soup.find("h1") or soup.find("title") or soup.find("h2")
            # if matches and len(matches.text.split()) > MIN_TITLE_LENGTH:
            if matches:
                retval.title = matches.get_text()
        if is_str_valid(retval.title):
            retval.title = retval.title.split("|", 1)[0].split(" - ", 1)[0].strip()
        return retval

    def _extract_from_pdf(self, filepath: str) -> ExtractResult:
        retval = ExtractResult()
        try:
            with open(filepath, "rb") as f:
                reader = PdfReader(f)
                retval.title = clean_spaces(reader.metadata.title)
        except Exception as e:
            logger.error(f"Error when parsing {filepath}: {e}")
        return retval

    async def _operation(self, entry: UrlEntry):
        entry_task = None
        if entry.origin_task.status == TaskStatus.SUCCESS:
            entry_task = entry.origin_task
        elif entry.memento_task.status == TaskStatus.SUCCESS:
            entry_task = entry.memento_task
        elif self.source == "doi":
            entry_task = entry.origin_task
        if entry_task is None:
            return
        content = entry_task.result
        if content is None:
            logger.error(f"Empty content for {entry.url}")
            return
        try:
            if is_path_exist(content):
                res = self._extract_from_pdf(content)
            else:
                res = self._extract_from_html(content)
                entry_task.result = None
            if self.source == "doi" and res.doi is None:
                res.doi = ExtractAttr(retrieve_doi(entry.url), True)
            if res.doi or res.title:
                entry.extract_task = ExtractTask(TaskStatus.SUCCESS, res)
            else:
                entry.extract_task = ExtractTask(
                    TaskStatus.FAILED, TaskError("N/A", "No available information")
                )
        except Exception as e:
            tb = f"{e}\n{traceback.format_exc()}"
            logger.error(f"Error when parsing: {e}\n{tb.format()}")
            entry.extract_task = ExtractTask(TaskStatus.FAILED, TaskError(ename(e), tb))


class HarvestGrobid(HarvestTask):
    def __init__(self, *args, **kwargs):
        super().__init__(HarvestGrobidConfig, *args, **kwargs)
        self.grobid = GrobidClient(
            grobid_server=self.config.grobid_url,
            sleep_time=self.config.grobid_freeze_time,
            check_server=False,
        )

    def should_skip(self, entry: UrlEntry, now_ts: pd.Timestamp) -> tuple[bool, bool]:
        if self.is_task_done(entry):
            return True, True
        return not self._should_retry(entry), False

    def _clean_title(self, query: str) -> str:
        try:
            retval = correct_hyphenation(query)
            retval_words = retval.split()
            while retval_words and retval_words[0].isdigit():
                retval_words = retval_words[1:]
            while retval_words and retval_words[-1].isdigit():
                retval_words = retval_words[:-1]
            retval = " ".join(retval_words)
        except Exception as e:
            tb = f"{e}\n{traceback.format_exc()}"
            logger.error(f"Error when cleaning title {query}: {e}\n{tb.format()}")
            retval = query
        return retval

    async def _operation(self, entry: UrlEntry) -> None:
        filepath = None
        if entry.origin_task.status == TaskStatus.SUCCESS:
            filepath = entry.origin_task.result
        elif entry.memento_task.status == TaskStatus.SUCCESS:
            filepath = entry.memento_task.result
        if not is_path_exist(filepath):
            return
        retry = True
        try:
            _, status, result = self.grobid.process_pdf(
                "processHeaderDocument",
                filepath,
                generateIDs=False,
                consolidate_header=False,
                include_raw_affiliations=False,
                include_raw_citations=False,
                tei_coordinates=False,
                segment_sentences=False,
                consolidate_citations=False,
            )
            if result is None:
                raise RuntimeError(f"grobid returns none for {filepath}")
            if status in grobid_error_status:
                msg = f"{grobid_error_status[status]} for {filepath}: {result}"
                retry = (status != 204) and ("[NO_BLOCKS]" not in result)
                raise RuntimeError(msg)
        except Exception as e:
            logger.debug(e) if isinstance(e, RuntimeError) else logger.error(e)
            entry.grobid_task = ExtractTask(
                TaskStatus.FAILED, TaskError(ename(e), str(e)), retry
            )
            if not retry:
                os.remove(filepath)
            return
        try:
            soup = BeautifulSoup(result, "lxml-xml")
            pub_tags = [
                ("find", "title", "title", {"type": "main", "level": "a"}),
                ("findall", "identifiers", "idno"),
                ("find", "meeting", "meeting"),
                ("find", "publisher", "publisher"),
            ]
            grobid_result = GrobidResult()
            for method, res_attr, *args in pub_tags:
                retrieve_soup(method, grobid_result, res_attr, soup, logger, *args)
            if is_str_valid(grobid_result.title):
                grobid_result.title = self._clean_title(grobid_result.title)
                if grobid_result.title is None:
                    entry.grobid_task = ExtractTask(
                        TaskStatus.FAILED, TaskError("N/A", "Empty title"), False
                    )
                else:
                    entry.grobid_task = ExtractTask(TaskStatus.SUCCESS, grobid_result)
            else:
                entry.grobid_task = ExtractTask(
                    TaskStatus.FAILED, TaskError("N/A", "Non scholarly content"), False
                )
            os.remove(filepath)
        except Exception as e:
            tb = f"{e}\n{traceback.format_exc()}"
            msg = f"Error occurs when parsing pdf file {filepath}: {e}\n{tb.format()}"
            logger.debug(msg) if isinstance(e, RuntimeError) else logger.error(msg)
            entry.grobid_task = ExtractTask(TaskStatus.FAILED, TaskError(ename(e), msg))


def retrieve_doi(doi: str) -> str:
    try:
        parsed_url = urlparse(doi)
        return parsed_url.path.strip("/")
    except:
        return doi


class HarvestOpenAlex(HarvestHttp):
    def __init__(self, *args, **kwargs):
        super().__init__(OpenAlexScraper, HarvestAgentConfig, *args, **kwargs)

    async def _operation(self, entry: UrlEntry) -> None:
        if (
            entry.extract_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status != TaskStatus.SUCCESS
        ):
            return
        if entry.s2_task.status == TaskStatus.SUCCESS:
            s2_doi = retrieve_doi(entry.s2_task.result.external_ids.get("doi", None))
            if s2_doi:
                entry.extract_task.result.doi = ExtractAttr(s2_doi, False)
            if mag := entry.s2_task.result.external_ids.get("mag", None):
                entry.extract_task.result.mag = mag
        if entry.extract_task.status == TaskStatus.SUCCESS:
            entry.openalex_task = await self.scraper.get(entry.extract_task.result)
        if (
            entry.openalex_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status == TaskStatus.SUCCESS
        ):
            entry.openalex_task = await self.scraper.get(entry.grobid_task.result)


class HarvestCrossref(HarvestHttp):
    def __init__(self, *args, **kwargs):
        super().__init__(CrossrefScraper, HarvestAgentConfig, *args, **kwargs)

    async def _operation(self, entry):
        if (
            entry.extract_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status != TaskStatus.SUCCESS
        ):
            return
        oa_title = None
        if entry.openalex_task.status == TaskStatus.SUCCESS:
            oa_doi = retrieve_doi(
                entry.openalex_task.result.external_ids.get("doi", None)
            )
            if oa_doi:
                entry.extract_task.result.doi = ExtractAttr(oa_doi, False)
            if mag := entry.openalex_task.result.external_ids.get("mag", None):
                entry.extract_task.result.mag = mag
            oa_title = entry.openalex_task.result.title
        if entry.extract_task.status == TaskStatus.SUCCESS:
            if not entry.extract_task.result.title and oa_title:
                entry.extract_task.result.title = oa_title
            entry.crossref_task = await self.scraper.get(entry.extract_task.result)
        if (
            entry.crossref_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status == TaskStatus.SUCCESS
        ):
            if not entry.grobid_task.result.title and oa_title:
                entry.grobid_task.result.title = oa_title
            entry.crossref_task = await self.scraper.get(entry.grobid_task.result)


class HarvestDBLP(HarvestHttp):
    def __init__(self, *args, **kwargs):
        super().__init__(DBLPScraper, HarvestAgentConfig, *args, **kwargs)

    async def _operation(self, entry: UrlEntry) -> None:
        if (
            entry.extract_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status != TaskStatus.SUCCESS
        ):
            return
        # if entry.s2_task.status == TaskStatus.SUCCESS:
        # s2_doi = retrieve_doi(entry.s2_task.result.external_ids.get("doi", None))
        # if s2_doi:
        # entry.extract_task.result.doi = ExtractAttr(s2_doi, False)
        # if mag := entry.s2_task.result.external_ids.get("mag", None):
        # entry.extract_task.result.mag = mag
        if entry.openalex_task.status == TaskStatus.SUCCESS:
            oa_doi = retrieve_doi(
                entry.openalex_task.result.external_ids.get("doi", None)
            )
            if oa_doi:
                entry.extract_task.result.doi = ExtractAttr(oa_doi, False)
            if mag := entry.openalex_task.result.external_ids.get("mag", None):
                entry.extract_task.result.mag = mag
        if entry.extract_task.status == TaskStatus.SUCCESS:
            entry.dblp_task = await self.scraper.get(entry.extract_task.result)
        if (
            entry.dblp_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status == TaskStatus.SUCCESS
        ):
            entry.dblp_task = await self.scraper.get(entry.grobid_task.result)


class HarvestS2(HarvestHttp):
    def __init__(self, *args, **kwargs):
        super().__init__(S2Scraper, HarvestAgentConfig, *args, **kwargs)

    async def _operation(self, entry: UrlEntry) -> None:
        if (
            entry.extract_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status != TaskStatus.SUCCESS
        ):
            return
        if entry.openalex_task.status == TaskStatus.SUCCESS:
            oa_doi = retrieve_doi(
                entry.openalex_task.result.external_ids.get("doi", None)
            )
            if oa_doi:
                entry.extract_task.result.doi = ExtractAttr(oa_doi, False)
            if mag := entry.openalex_task.result.external_ids.get("mag", None):
                entry.extract_task.result.mag = mag
        if entry.extract_task.status == TaskStatus.SUCCESS:
            entry.s2_task = await self.scraper.get(entry.extract_task.result)
        if (
            entry.s2_task.status != TaskStatus.SUCCESS
            and entry.grobid_task.status == TaskStatus.SUCCESS
        ):
            entry.s2_task = await self.scraper.get(entry.grobid_task.result)


class HarvestS2Batch(HarvestTask):
    id_pat = {
        "semanticscholar": re.compile(
            "/(?P<id>[a-z0-9]{40})|(?:(?P<id1>[a-z0-9]{4})/(?P<id2>[a-z0-9]{36})[.])",
            re.IGNORECASE,
        ),
        "arxiv": re.compile("(?P<id>\d{4}[.]\d{4,5})", re.IGNORECASE),
    }

    def __init__(self, id_name: str, *args, **kwargs):
        self.id_name = id_name
        super().__init__(None, *args, **kwargs)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def _dispatch(self, df: pd.DataFrame, txn: lmdb.Transaction) -> pd.Index:
        raise NotImplementedError

    def _retrieve_id(self, url: str) -> str:
        parsed_url = urlparse(url)
        id_match = self.id_pat[self.id_name].search(parsed_url.path)
        if id_match:
            return (
                id_match.group("id")
                or f"{id_match.group('id1')}{id_match.group('id2')}"
            )
        return None

    def _build_query(self, id_list: list[str]) -> dict:
        if self.id_name != "semanticscholar":
            id_list = [f"{self.id_name}:{id}" for id in id_list]
        return {
            "url": "https://api.semanticscholar.org/graph/v1/paper/batch",
            "params": {"fields": ",".join(S2Scraper.fields)},
            "json": {"ids": list(id_list)},
        }

    async def __call__(self, df: pd.DataFrame) -> pd.Index:
        if len(df) == 0:
            return pd.Index([])
        df[self.id_name] = df.Url.apply(self._retrieve_id)
        id_list = df[self.id_name].dropna().unique()
        if len(id_list) == 0:
            return pd.Index([])
        data = None
        if self.x_progress:
            self.pbar = tqdm(total=len(id_list), desc="s2_batch")
        try:
            resp = await self.session.post(**self._build_query(id_list))
            async with resp:
                data = await resp.json()
            if data is None or len(data) != len(id_list):
                raise RuntimeError("Invalid response")
            idx = 0
            with self.entries.begin(write=True) as txn:
                for row in df.itertuples():
                    self.pbar.update(1)
                    row_id = rg(row, self.id_name)
                    if row_id is None:
                        continue
                    key = hashlib.sha512(row.Url.encode("utf-8")).hexdigest()
                    if lmdb_record := txn.get(key.encode("utf-8")):
                        try:
                            entry = pickle.loads(lmdb_record)
                        except Exception as e:
                            entry = UrlEntry(row.Url, key=row.Url)
                    else:
                        entry = UrlEntry(row.Url, key=row.Url)
                    if meta := data[idx]:
                        meta = S2Scraper._build_result(meta, 1)
                        entry.s2_task = BiblioTask(TaskStatus.SUCCESS, meta)
                        entry.extract_task = ExtractTask(
                            TaskStatus.SUCCESS,
                            ExtractResult(
                                title=rg(meta, "title"),
                                doi=rg(meta, "external_ids.doi"),
                                mag=rg(meta, "external_ids.mag"),
                            ),
                        )
                    else:
                        entry.s2_task = BiblioTask(
                            TaskStatus.FAILED, TaskError("N/A", "No metadata")
                        )
                    txn.put(key.encode("utf-8"), pickle.dumps(entry))
                    idx += 1
        except Exception as e:
            logger.error(f"Error ({ename(e)}) when quering {self.id_name}: {e}")
        finally:
            if self.x_progress:
                self.pbar.close()


def dump_entry(
    fp,
    entry: UrlEntry,
    fields: list = [
        "s2",
        "openalex",
        "crossref",
        "extract",
        "grobid",
        "origin",
        "patch",
        "memento",
    ],
) -> None:
    for task in fields:
        task_state = getattr(entry, f"{task}_task", None)
        if isinstance(task_state, BaseTask) and isinstance(
            task_state.result, TaskError
        ):
            task_state.result = TaskError(
                task_state.result.type, task_state.result.reason
            )
    entry_dict = asdict(entry)
    output_dict = {"key": entry_dict["key"]}
    for field in fields:
        field_name = f"{field}_task"
        if field_name in entry_dict:
            output_dict[field] = entry_dict[field_name]
    fp.write(json.dumps(output_dict, default=str, separators=(",", ":")) + "\n")


def dump_catalog(entries, source_name: str = "") -> None:
    folder_dir = get_abs_path("catalogs", __file__)
    if not os.path.isdir(folder_dir):
        os.mkdir(folder_dir)
    dir = os.path.join(folder_dir, source_name)
    if not os.path.isdir(dir):
        os.mkdir(dir)
    with entries.begin(write=False) as txn:
        with open(os.path.join(dir, "unreachable.jsonl"), "w") as unreachable_out, open(
            os.path.join(dir, "unparsable.jsonl"), "w"
        ) as unparsable_out, open(
            os.path.join(dir, "unmatchable.jsonl"), "w"
        ) as unmatchable_out, open(
            os.path.join(dir, "success.jsonl"), "w"
        ) as success_out, open(
            os.path.join(dir, "failed.jsonl"), "w"
        ) as failed_out:
            cursor = txn.cursor()
            for _, value in cursor:
                try:
                    entry: UrlEntry = pickle.loads(value)
                except:
                    continue
                if (
                    entry.s2_task.status == TaskStatus.SUCCESS
                    or entry.openalex_task.status == TaskStatus.SUCCESS
                    or entry.dblp_task.status == TaskStatus.SUCCESS
                    or entry.crossref_task.status == TaskStatus.SUCCESS
                ):
                    dump_entry(success_out, entry)
                    continue
                dump_entry(failed_out, entry)
                if HarvestGrobid.is_task_done(entry) or HarvestExtract.is_task_done(
                    entry
                ):
                    dump_entry(unmatchable_out, entry)
                elif HarvestMemento.is_task_done(entry):
                    dump_entry(unparsable_out, entry)
                elif HarvestOrigin.is_task_done(entry):
                    dump_entry(unparsable_out, entry)
                else:
                    dump_entry(unreachable_out, entry)
