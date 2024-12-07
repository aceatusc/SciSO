from dataclasses import dataclass, field
from enum import Enum
import pandas as pd


@dataclass
class OrchestrateConfig:
    data_path: str
    db_name: str
    db_size: int
    chunksize: int


@dataclass
class ScraperConfig:
    conn_timeout: int
    read_timeout: int
    concurrent: int


@dataclass
class HarvestOriginConfig(ScraperConfig):
    max_file_size: int
    freeze_time: int


@dataclass
class HarvestArchiveConfig(ScraperConfig):
    max_file_size: int
    freeze_time: int
    requests_per_second: int


@dataclass
class HarvestAgentConfig(ScraperConfig):
    sim_threshold_l1: float
    sim_threshold_l2: float
    freeze_time: int
    requests_per_second: int


@dataclass
class HarvestBiblioConfig:
    priority: list[dict]
    sources: list[str]
    concurrent: int


@dataclass
class HarvestGrobidConfig:
    grobid_url: str
    grobid_freeze_time: int
    concurrent: int


@dataclass
class HarvestExtractConfig:
    metarules: dict
    concurrent: int


class TaskStatus(Enum):
    INITIAL = "Initial state, not yet processed"
    SUCCESS = "Task succeeded"
    FAILED = "Task failed"


@dataclass
class TaskError:
    type: str = None
    reason: str = None

    def __post_init__(self):
        if isinstance(self.reason, Exception):
            # Convert the exception to its string representation
            self.reason = str(self.reason)


@dataclass
class BaseTask:
    status: TaskStatus = TaskStatus.INITIAL
    result: object = None


@dataclass
class ScrapeTask(BaseTask):
    result: str | TaskError = None
    retry: bool = True
    last_attempt_ts: pd.DataFrame = pd.Timestamp(1970, 1, 1)


@dataclass
class GrobidResult:
    title: str = None
    meta_title: str = None
    identifiers: list[dict] = None
    meeting: str = None
    publisher: str = None


@dataclass
class ExtractAttr:
    value: str
    is_wildcard: bool = False


@dataclass
class ExtractResult:
    doi: ExtractAttr | str = None
    title: str = None
    mag: str = None
    ieee_id: str = None
    elsevier_id: str = None


@dataclass
class ExtractTask(BaseTask):
    result: ExtractResult | GrobidResult | TaskError = None
    retry: bool = True


@dataclass
class BiblioResult:
    source: str = None
    source_id: str = None
    title: str = None
    similarity: float = None
    authors: list[dict] = None
    venue: str | dict = None
    year: int = None
    open_access: bool = None
    citation_count: int = None
    concepts: list[dict] = None
    external_ids: dict = None
    type: str = None
    abstract: str = None
    embedding: list[float] = None


@dataclass
class BiblioTask(BaseTask):
    result: BiblioResult | TaskError = None


@dataclass
class UrlEntry:
    url: str
    key: str
    origin_task: ScrapeTask = field(default_factory=ScrapeTask)
    patch_task: ScrapeTask = field(default_factory=ScrapeTask)
    memento_task: ScrapeTask = field(default_factory=ScrapeTask)
    extract_task: ExtractTask = field(default_factory=ExtractTask)
    grobid_task: ExtractTask = field(default_factory=ExtractTask)
    crossref_task: BiblioTask = field(default=BiblioTask)
    dblp_task: BiblioTask = field(default_factory=BiblioTask)
    s2_task: BiblioTask = field(default_factory=BiblioTask)
    openalex_task: BiblioTask = field(default_factory=BiblioTask)


@dataclass
class VenueInfo:
    s2id: str
    fullname: str
    type: str
    alternate_names: list[str]
    url: str
