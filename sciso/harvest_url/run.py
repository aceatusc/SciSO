import argparse
import asyncio
import json
import os
import shutil
import pandas as pd
from ..scraper import PatchScraper
from ..utils import get_logger
from .scrapers import url_scrapers, HttpUrlScraper, UrlMementoScraper
from ..task import (
    HarvestExtract,
    HarvestGrobid,
    HarvestMemento,
    HarvestOpenAlex,
    HarvestCrossref,
    HarvestOrigin,
    HarvestPatch,
    HarvestS2,
    HarvestDBLP,
    HarvestS2Batch,
    dump_catalog,
)
from ..orchestrate import Orchestrator
from pymongo import MongoClient

logger = get_logger("harvest_url")

# sources = [
#   'aaai',            'acl',      'ams',
#   'archive',         'arxiv',    'cambridge',
#   'cellpress',       'ceur',     'cvf',
#   'doi',             'elsevier', 'hal',
#   'hindawi',         'iacr',     'iclr',
#   'icml',            'ieee',     'igi-global',
#   'ijcai',           'ingenta',  'iospress',
#   'jmlr',            'mdpi',     'mitpress',
#   'mlr',             'nature',   'ndss',
#   'nih',             'nips',     'nowpublishers',
#   'openreview',      'oxford',   'paperswithcode',
#   'pdf',             'plos',     'pnas',
#   'researchgate',    'ronpub',   'sciendo',
#   'semanticscholar', 'usenix',   'vldb',
#   'worldscientific'
# ]


class HarvestPubUrl(Orchestrator):
    rel_file = __file__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = MongoClient("mongodb://localhost:27017/").get_database("sciso-2024")
        self.dataset = self.db.get_collection("candidate_urls")

    async def run(self, filepath: str = None) -> None:
        if not filepath:
            filepath = f"{self.source}.jsonl"
            with open(filepath, "w") as fp:
                for row in self.dataset.find({"Source": self.source}):
                    fp.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
        data = pd.read_json(filepath, lines=True, chunksize=self.config.chunksize)
        if self.source not in url_scrapers:
            logger.warning(f"Using default HTTP scraper for {self.source}")
        origin_url_scraper = url_scrapers.get(self.source, HttpUrlScraper)
        try:
            async with HarvestOrigin(
                origin_url_scraper, *self.args
            ) as origin, HarvestPatch(
                PatchScraper, *self.args
            ) as patch, HarvestMemento(
                UrlMementoScraper, *self.args
            ) as memento, HarvestExtract(
                self.source, *self.args
            ) as extract, HarvestGrobid(
                *self.args
            ) as grobid, HarvestOpenAlex(
                *self.args
            ) as openalex, HarvestCrossref(
                *self.args
            ) as crossref, HarvestS2(
                *self.args
            ) as s2, HarvestDBLP(
                *self.args
            ) as dblp, HarvestS2Batch(
                self.source, *self.args
            ) as s2batch:
                for i, chunk in enumerate(data):
                    logger.info(f"Processing chunk {i}")
                    chunk = self._clean_chunk(chunk)
                    available = chunk.index
                    if self._handler_enabled(HarvestS2Batch):
                        await s2batch(chunk.loc[available])
                    else:
                        if self._handler_enabled(origin):
                            available = await origin(chunk)
                            if self._handler_enabled(patch):
                                unreachable = chunk.index.difference(available)
                                patchable = await patch(chunk.loc[unreachable])
                                if self._handler_enabled(memento):
                                    patched = await memento(chunk.loc[patchable])
                                    available = available.union(patched)
                            if self.source == "doi":
                                available = chunk.index
                        if self._handler_enabled(HarvestExtract):
                            await extract(chunk.loc[available])
                        if self._handler_enabled(HarvestGrobid):
                            await grobid(chunk.loc[available])
                        if self._handler_enabled(HarvestOpenAlex):
                            await openalex(chunk.loc[available])
                        # if self._handler_enabled(HarvestCrossref):
                        #     await crossref(chunk.loc[available])
                        if self._handler_enabled(HarvestDBLP):
                            await dblp(chunk.loc[available])
                        if self._handler_enabled(HarvestS2):
                            await s2(chunk.loc[available])
                            if self._handler_enabled(HarvestOpenAlex):
                                await openalex(chunk.loc[available])
        finally:
            dump_catalog(self.entries, self.source)


if __name__ == "__main__":
    tasks = [
        "origin",
        # "patch",
        # "memento",
        "extract",
        "grobid",
        "openalex",
        # "crossref",
        "s2",
    ]  # Add "s2"
    handler = [f"harvest_{task}" for task in tasks]

    parser = argparse.ArgumentParser()
    parser.add_argument("-f")
    parser.add_argument("-s")
    parser.add_argument("-x", action="store_true", default=False)
    args = parser.parse_args()

    orchestrator = HarvestPubUrl(
        handler=handler,
        config_path="config.json",
        x_progress=True,
        fresh_start=args.x,
        source=args.s,
    )
    asyncio.run(orchestrator.run(args.f))
