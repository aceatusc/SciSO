import os
import re
from urllib.parse import urlparse
from aiohttp import ClientResponse
from aiolimiter import AsyncLimiter
from dotenv import load_dotenv
from wayback import Memento
from sciso.data_model import ScrapeTask
from ..data_model import ScrapeTask, TaskError, TaskStatus
from ..utils import (
    ename,
    validate_stm,
    recursive_get as rg,
    get_logger,
)
from ..scraper import HttpScraper, MementoScraper
import cloudscraper

logger = get_logger("url_scraper")


class HttpUrlScraper(HttpScraper):
    def _normalize_url(self, url: str) -> str:
        return url

    async def get(self, url: str, *args, **kwargs) -> ScrapeTask:
        nor_url = self._normalize_url(url)
        res = await super().get(nor_url, *args, **kwargs)
        if res.status == TaskStatus.FAILED:
            res = await super().get(url, *args, **kwargs)
        return res

    async def handle_response(self, resp: ClientResponse) -> ScrapeTask:
        is_valid, err = validate_stm(
            resp,
            [
                ("status", "eq", 200),
                ("headers.Content-Length", "le", self.config.max_file_size),
            ],
        )
        if is_valid:
            content_type = rg(resp.headers, "Content-Type", "").lower()
            type_chunk = await resp.content.read(20)
            if "pdf" in content_type or type_chunk.startswith(b"%PDF"):
                filename = self._download_path(str(resp.url))
                try:
                    with open(filename, "wb") as f:
                        f.write(type_chunk)
                        async for data in resp.content.iter_chunked(1024):
                            f.write(data)
                except Exception as e:
                    logger.error(f"Error({ename(e)}) for {resp.url}: {e}")
                    try:
                        os.remove(filename)
                    except Exception:
                        pass
                    return ScrapeTask(TaskStatus.FAILED, TaskError(ename(e), str(e)))
                return ScrapeTask(TaskStatus.SUCCESS, filename)
            type_chunk_str = type_chunk.decode("utf-8", errors="ignore").lower()
            if "html" in content_type or "html" in type_chunk_str:
                rest_chunk = await resp.content.read()
                full_content = (type_chunk + rest_chunk).decode(
                    "utf-8", errors="ignore"
                )
                return ScrapeTask(TaskStatus.SUCCESS, full_content)
            err = "Unknown content type"
        return ScrapeTask(TaskStatus.FAILED, TaskError("InvalidResponse", err))


class HttpUrlSyncScraper(HttpUrlScraper):
    def __init__(self, *args, **kwargs):
        kwargs["is_async"] = False
        super().__init__(*args, **kwargs)

    async def handle_response(self, resp: cloudscraper.requests.Response) -> ScrapeTask:
        is_valid, err = validate_stm(
            resp,
            [
                ("status", "eq", 200),
                ("headers.Content-Length", "le", self.config.max_file_size),
            ],
        )
        if is_valid:
            content_type = rg(resp.headers, "Content-Type", "").lower()
            type_chunk = resp.content[:5]
            if "pdf" in content_type or type_chunk.startswith(b"%PDF"):
                filename = self._download_path(str(resp.url))
                try:
                    with open(filename, "wb") as f:
                        f.write(resp.text)
                except Exception as e:
                    logger.error(f"Error({ename(e)}) for {resp.url}: {e}")
                    try:
                        os.remove(filename)
                    except Exception:
                        pass
                    return ScrapeTask(TaskStatus.FAILED, TaskError(ename(e), str(e)))
                return ScrapeTask(TaskStatus.SUCCESS, filename)
            type_chunk_str = type_chunk.decode("utf-8", errors="ignore").lower()
            if "html" in content_type or "html" in type_chunk_str:
                return ScrapeTask(TaskStatus.SUCCESS, resp.text)
            err = "Unknown content type"
        return ScrapeTask(TaskStatus.FAILED, TaskError("InvalidResponse", err))


class UrlMementoScraper(MementoScraper):
    def handle_response(self, resp: Memento) -> ScrapeTask:
        is_valid, err = validate_stm(
            resp,
            [
                ("status_code", "eq", 200),
                ("headers.Content-Length", "le", self.config.max_file_size),
            ],
        )
        if is_valid:
            content_type = rg(resp.headers, "Content-Type")
            if "html" in content_type or "html" in resp.text[:20]:
                if resp.text and len(resp.text):
                    return ScrapeTask(TaskStatus.SUCCESS, resp.text)
            elif resp.text[:5] == "%PDF-":
                filename = self._download_path(str(resp.url))
                with open(filename, "wb") as f_out:
                    f_out.write(resp.content)
                return ScrapeTask(TaskStatus.SUCCESS, filename)
            err = "Unknown content type"
        return ScrapeTask(TaskStatus.FAILED, TaskError("InvalidResponse", err))


class NatureUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        if parsed_url.path.endswith("pdf"):
            normalized_path = "".join(parsed_url.path.rsplit(".", 1)[:-1])
            return f"{parsed_url.scheme}://{parsed_url.netloc}{normalized_path}"
        return url


class HalUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        if parsed_url.path.endswith("document"):
            normalized_path = "".join(parsed_url.path.rsplit("/", 1)[:-1])
            return f"{parsed_url.scheme}://{parsed_url.netloc}{normalized_path}"
        return url


class AaaiUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        return url.replace("viewArticle", "view")


class HindawiScraper(HttpUrlSyncScraper):
    def _normalize_url(self, url: str) -> str:
        url = url.replace("downloads", "www")
        if url.endswith(".pdf"):
            url = url.replace(".pdf", "")
        return url


class IacrUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        if "eprint" in url:
            return url.replace(".pdf", "").replace(".ps", "")
        if "tches" in url:
            return url.replace("/view", "/download")
        return url


class MdpiUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        return url.replace("/pdf", "")


class OpenreviewUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        return url.replace("/pdf?", "/forum?")


class MlrUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        if parsed_url.path.endswith("pdf"):
            normalized_path = parsed_url.path.replace(".pdf", ".html")
            return f"{parsed_url.scheme}://{parsed_url.netloc}{normalized_path}"
        return url


class AclUrlScraper(HttpUrlScraper):
    def _normalize_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        if parsed_url.path.endswith("pdf"):
            normalized_path = parsed_url.path.replace(".pdf", "")
            return f"{parsed_url.scheme}://{parsed_url.netloc}{normalized_path}"
        return url


class DoNothing:
    def __init__(self, *args, **kwargs):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    async def get(self, url: str, *args, **kwargs) -> ScrapeTask:
        return ScrapeTask(TaskStatus.SUCCESS, url)


class DoiScraper(HttpUrlScraper):
    async def get(self, url: str, *args, **kwargs) -> ScrapeTask:
        res = await super().get(url, *args, **kwargs)
        if res.status == TaskStatus.FAILED:
            res.result = urlparse(url).path.strip("/")
        return res


class ArxivScraper(HttpUrlScraper):
    arxiv_id_pat = re.compile("(?P<id>\d{4}[.]\d{4,5})", re.IGNORECASE)

    async def get(self, url: str, *args, **kwargs) -> ScrapeTask:
        # res = await super().get(url, *args, **kwargs)
        # if res.status == TaskStatus.FAILED:
        #     path = urlparse(url).path
        #     id_match = self.arxiv_id_pat.search(path)
        #     if id_match:
        #         res.result = f"10.48550/arxiv.{id_match.group('id')}"
        # return res
        path = urlparse(url).path
        id_match = self.arxiv_id_pat.search(path)
        if id_match:
            return ScrapeTask(
                TaskStatus.SUCCESS, f"10.48550/arxiv.{id_match.group('id')}"
            )
        return super().get(url, *args, **kwargs)


class ResearchgateUrlScraper(DoNothing):
    pub_name_pat = re.compile("\d{7,}_(?P<title>[^./]+)", re.IGNORECASE)

    async def get(self, url: str, *args, **kwargs) -> ScrapeTask:
        parsed_url = urlparse(url)
        pub_match = self.pub_name_pat.search(parsed_url.path)
        if pub_match:
            pub_name = pub_match.group("title").replace("_", " ").replace("-", " ")
            return ScrapeTask(TaskStatus.SUCCESS, f"<h1 id='title'>{pub_name}</h1>")
        return ScrapeTask(
            TaskStatus.FAILED, TaskError("ValueError", "Unrecognized URL")
        )


class IeeeUrlScraper(HttpUrlScraper):
    pub_id_pat = re.compile("arnumber=(?P<id>\d+)", re.IGNORECASE)

    def _normalize_url(self, url: str) -> str:
        if "arnumber" in url:
            id_match = self.pub_id_pat.search(url)
            if id_match:
                id = id_match.group("id")
                return f"https://ieeexplore.ieee.org/document/{id}"
        return url


class ElsevierUrlScraper(HttpUrlScraper):
    pii_pat = re.compile("pii/(?P<pii>[a-z0-9]{16,})", re.IGNORECASE)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        load_dotenv()
        self.api_key = os.getenv("ELSEVIER_KEY")
        self.limiter = AsyncLimiter(5, 1)

    async def get(self, url: str, *args, **kwargs) -> ScrapeTask:
        parsed_url = urlparse(url)
        pii_match = self.pii_pat.search(parsed_url.path)
        if pii_match:
            pii = pii_match.group("pii")
            async with self.limiter:
                async with self.session.get(
                    f"https://api.elsevier.com/content/abstract/pii/{pii}?apiKey={self.api_key}",
                    headers={"Accept": "application/json"},
                ) as resp:
                    meta = await resp.json()
                    doi = rg(meta, "abstracts-retrieval-response.coredata.prism:doi")
                    title = rg(meta, "abstracts-retrieval-response.coredata.dc:title")
                    return ScrapeTask(
                        TaskStatus.SUCCESS,
                        f"<h1 id='api_title'>{title}</h1><h2 id='api_doi'>{doi}</h2>",
                    )
        return ScrapeTask(TaskStatus.FAILED, TaskError("ValueError", "No PII found"))


url_scrapers = {
    "doi": DoiScraper,
    "acl": AclUrlScraper,
    "aaai": AaaiUrlScraper,
    "mlr": MlrUrlScraper,
    "openreview": OpenreviewUrlScraper,
    "mdpi": MdpiUrlScraper,
    "nature": NatureUrlScraper,
    "hal": HalUrlScraper,
    "iacr": IacrUrlScraper,
    "researchgate": ResearchgateUrlScraper,
    "arxiv": ArxivScraper,
    "semanticscholar": DoNothing,
    "test": ArxivScraper,
    "ieee": IeeeUrlScraper,
    "elsevier": ElsevierUrlScraper,
    "hindawi": HindawiScraper,
}
