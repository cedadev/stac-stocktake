""" Module for checking the STAC Asset index is upto date with the FBI """

__author__ = "Rhys Evans"
__date__ = "2022-08-24"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD"


import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator, Union

import yaml
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Date, Document, Integer, Object, Search, connections
from stac_generator.scripts.stac_generator import load_generator

from stac_stocktake.rabbit import RabbitProducer

log = logging.getLogger(__name__)


class StacStocktake:
    """
    Class to poll Elasticsearch for unaggregated assets or items and add
    the nessasary messages to the correct RabbitMQ queue
    """

    def __init__(self) -> None:

        # Load configuration
        config_file = os.environ.get("STAC_STOCKTAKE_CONFIGURATION_FILE")
        if not config_file:
            config_file = os.path.join(
                Path(__file__).parent,
                ".stac_stocktake.yml",
            )

        with open(config_file, encoding="utf-8") as reader:
            conf = yaml.safe_load(reader)

        general_conf = conf.get("GENERAL")
        log_conf = conf.get("LOGGING")
        es_conf = conf.get("ELASTICSEARCH")

        logging.basicConfig(
            format="%(asctime)s @%(name)s [%(levelname)s]:    %(message)s",
            level=logging.getLevelName(log_conf.get("LEVEL")),
        )

        # Create either a rabbit producer or local stac generator
        if "RABBIT" in conf:
            rabbit_conf = conf.get("RABBIT")
            self.producer = RabbitProducer(rabbit_conf.get("SESSION_KWARGS"))

        else:
            generator_conf = conf.get("GENERATOR")
            self.generator = load_generator(generator_conf)

        connections.create_connection(alias="es", **es_conf.get("SESSION_KWARGS"))

        state_search = self.State()
        state_search._index._using = "es"
        state_search._index._name = general_conf.get("STATE_INDEX")

        self.state = self.get_initial_state()

        self.fbi_index = general_conf.get("FBI_INDEX")
        self.stac_index = general_conf.get("STAC_INDEX")

        # scan fbi and stac asset catalog
        self.fbi_records = self.get_fbi_records(self.state.fbi_record["path"])
        self.stac_assets = self.get_stac_assets(
            self.state.stac_asset["properties"]["uri"]
        )

        # read first record and asset
        self.next_fbi_record()
        self.next_stac_asset()

    class State(Document):
        """
        State of the stocktake store in elasticsearch
        """

        run = Integer()
        fbi_record = Object()
        stac_asset = Object()
        count = Integer()
        new = Integer()
        deleted = Integer()
        same = Integer()
        start_time = Date()
        last_save_time = Date()

        def save(self, **kwargs):
            self.last_save_time = datetime.now()
            return super().save(**kwargs)

    def get_initial_state(self) -> dict:
        """
        Get the current state of the stocktake or
        create it if it doesn't yet exist.

        :return: current stocktake state
        """

        state = None

        try:

            state_search = self.State.search().sort("-run").extra(size=1)

            response = state_search.execute()

            if response.success() and response.hits.total != 0:
                state = response.hits[0]

            if (
                state.stac_asset["properties"]["uri"] != "~"
                or state.fbi_record["path"] != "~"
            ):
                return state

        except NotFoundError:
            pass

        return self.create_new_state(state)

    def create_new_state(self, state: Union[State, None]) -> dict:
        """
        Create the initial state of the stocktake.

        :return: initial stocktake state
        """

        if not state:
            self.State.init()
            run = 1
        else:
            run = state.run + 1

        state = self.State(
            run=run,
            fbi_record={"path": ""},
            stac_asset={"properties": {"uri": ""}},
            count=0,
            new=0,
            deleted=0,
            same=0,
            start_time=datetime.now(),
        )
        state.save()

        return state

    def get_fbi_records(self, search_after: str) -> Iterator[dict]:
        """
        Get the next 10k FBI records with a path after `search_after`.

        :param search_after: path to search after
        :return: relevant fbi records
        """

        log.info("Querying FBI.")

        query = (
            Search(using="es", index=self.fbi_index)
            .source(["path"])
            .filter("term", type="file")
            .sort("path.keyword")
            .extra(size=10000)
        )

        if search_after:
            query = query.extra(search_after=[search_after])

        response = query.execute()

        if response.hits:
            yield from response.hits

        else:
            yield {"path": "~"}

    def get_stac_assets(self, search_after: str) -> Iterator[dict]:
        """
        Get the next 10k STAC Asset with a uri after `search_after`.

        :param search_after: uri to search after
        :return: relevant STAC Assets
        """

        log.info("Querying STAC.")

        query = (
            Search(using="es", index=self.stac_index)
            .source(["properties.uri"])
            .sort("properties.uri.keyword")
            .extra(size=10000)
        )

        if search_after:
            query = query.extra(search_after=[search_after])

        response = query.execute()

        if response.hits:
            yield from response.hits

        else:
            yield {"properties": {"uri": "~"}}

    def next_fbi_record(self):
        """
        Get the next fbi record
        """
        previous_fbi_path = self.fbi_path

        self.state.fbi_record = next(self.fbi_records, {"properties": {"uri": ""}})

        # if we reach the end of the fbi records get the next 10k results
        if not self.fbi_path:
            self.fbi_records = self.get_fbi_records(previous_fbi_path)
            self.next_fbi_record()

        # if the documents' sort is empty you have reached the end of the search
        if (
            hasattr(self.state.fbi_record, "meta")
            and not self.state.fbi_record.meta.sort[0]
        ):
            self.state.fbi_record = {"path": "~"}

    def next_stac_asset(self):
        """
        Get the next stac asset
        """
        previous_stac_path = self.stac_path

        self.state.stac_asset = next(self.stac_assets, {"properties": {"uri": ""}})

        # if we reach the end of the stac assets get the next 10k results
        if not self.stac_path:
            self.stac_assets = self.get_stac_assets(previous_stac_path)
            self.next_stac_asset()

        # if the documents' sort is empty you have reached the end of the search
        if (
            hasattr(self.state.stac_asset, "meta")
            and not self.state.stac_asset.meta.sort[0]
        ):
            self.state.stac_asset = {"properties": {"uri": "~"}}

    @property
    def fbi_path(self):
        """
        Get the path from the fbi record
        """
        return self.state.fbi_record["path"]

    @property
    def stac_path(self):
        """
        Get the path from the stac asset
        """
        return self.state.stac_asset["properties"]["uri"]

    def create_stac_asset(self):
        """
        Insert a new STAC Asset from the fbi.
        """
        self.state.new += 1

        log.info("ADD_MISSING_STAC_ASSET: %s", self.fbi_path)

        if hasattr(self, "producer"):
            message = {"uri": self.fbi_path}

            self.producer.publish(message)

        else:
            self.generator.process(self.fbi_path)

    def delete_stac_asset(self):
        """
        Remove a STAC Asset
        """
        pass

    def run(self):
        """
        Compare the STAC Assets and FBI records
        """
        while True:
            # print and save every 1000 items
            if self.state.count % 1000 == 0:
                log.info(
                    "%s: %s  ---  %s", self.state.count, self.fbi_path, self.stac_path
                )
                self.state.save()
            self.state.count += 1

            # stop if end of both files
            if self.fbi_path == "~" and self.stac_path == "~":
                log.info("FIN")
                self.state.save()
                break

            # if fbi has ended or the fbi record is ahead and there are stac records left,
            # then we need to remove an asset.
            if self.stac_path == "~" or self.stac_path > self.fbi_path:
                self.create_stac_asset()
                self.next_fbi_record()

            # if stac has ended or the stac asset is behind the fbi then we need
            # to creae a new asset.
            elif self.fbi_path == "~" or self.stac_path < self.fbi_path:
                self.delete_stac_asset()
                self.next_stac_asset()

            # if both record and asset are about the same path then move on.
            elif self.fbi_path == self.stac_path:
                self.next_fbi_record()
                self.next_stac_asset()


if __name__ == "__main__":
    print("RUNNING")
    StacStocktake().run()
