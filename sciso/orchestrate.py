import json
import os
import shutil
import lmdb
import pandas as pd
from .task import HarvestTask
from .data_model import OrchestrateConfig
from .utils import get_abs_path, get_logger, normalize_url, transform_access_url

logger = get_logger("orchestrate")


class Orchestrator:
    def __init__(
        self,
        handler: list,
        config_path: str = "config.json",
        x_progress: bool = False,
        fresh_start: bool = False,
        source: str = None,
    ):
        self._load_config(config_path)
        self.fresh_start = fresh_start
        self.source = source
        self._init_lmdb()
        self.task_handlers = handler
        self.args = [
            self.entries,
            self.config_path,
            self.config.data_path,
            x_progress,
        ]

    def _load_config(self, path) -> None:
        self.config_path = get_abs_path(path)
        with open(self.config_path, "r") as f:
            config = json.load(f)
        self.config = OrchestrateConfig(**config["orchestrate"])
        self.config.data_path = get_abs_path(self.config.data_path, self.rel_file)
        if not os.path.isdir(self.config.data_path):
            os.mkdir(self.config.data_path)
        self.config.db_size *= 1024**3

    def _init_lmdb(self) -> None:
        entries_db_path = os.path.join(
            self.config.data_path, f"{self.source}-{self.config.db_name}"
        )
        if self.fresh_start and os.path.isdir(entries_db_path):
            shutil.rmtree(entries_db_path)
        self.entries = lmdb.open(entries_db_path, map_size=self.config.db_size)

    def _handler_enabled(self, handler: HarvestTask) -> bool:
        return handler.task_config_name() in self.task_handlers

    def _clean_url(self, url: str) -> str:
        try:
            return normalize_url(transform_access_url(url))
        except Exception as e:
            logger and logger.error(f"Failed to clean url {url}: {e}")
            return None

    def _clean_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        chunk["Key"] = chunk["Url"]
        chunk["Url"] = chunk["Key"].apply(self._clean_url)
        chunk = chunk[chunk["Url"].notna()]
        chunk = chunk.drop_duplicates(subset="Url", keep="first").reset_index(drop=True)
        # return chunk[["Key", "Url"]]
        return chunk

    async def run(self, filepath: str) -> None:
        raise NotImplementedError
