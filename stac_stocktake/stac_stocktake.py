""" Module for checking the STAC Asset index is upto date with the FBI """

__author__ = "Rhys Evans"
__date__ = "2022-08-24"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD"


import logging
import os
from datetime import datetime
from pathlib import Path

import yaml
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

        config_file = os.environ.get("STAC_STOCKTAKE_CONFIGURATION_FILE")
        if not config_file:
            config_file = os.path.join(
                Path(__file__).parent,
                ".stac_stocktake.yml",
            )

        with open(config_file, encoding="utf-8") as reader:
            conf = yaml.safe_load(reader)

        es_conf = conf.get("ELASTICSEARCH")
        stock_conf = conf.get("STOCKTAKE")
        rabbit_conf = conf.get("RABBIT")
        generator_conf = conf.get("GENERATOR")
        log_conf = conf.get("LOGGING")

        logging.basicConfig(
            format="%(asctime)s @%(name)s [%(levelname)s]:    %(message)s",
            level=logging.getLevelName(log_conf.get("LEVEL")),
        )

        self.producer = RabbitProducer(rabbit_conf.get("SESSION_KWARGS"))

        self.generator = load_generator(generator_conf)

        connections.create_connection(alias="es", **es_conf.get("SESSION_KWARGS"))

        state_search = self.State()
        state_search._index._using = "es"
        state_search._index._name = stock_conf.get("STATE_INDEX")

        self.state = self.State.get(id=1, ignore=404)

        if not self.state:
            self.state = self.State(
                fbi_record={"path": "/"},
                stac_asset={"properties": {"uri": "/"}},
                new=0,
                deleted=0,
                same=0,
            )
            self.state.save()

        # start iterators to scan fbi and filesystem
        self.fbi_records = self.get_fbi_records(index=stock_conf.get("FBI_INDEX"))
        self.stac_assets = self.get_stac_assets(index=stock_conf.get("STAC_INDEX"))

        # read first records.
        self.next_fbi_record()
        self.next_stac_asset()

    class State(Document):
        """
        State of the stocktake store in elasticsearch
        """

        fbi_record = Object()
        stac_asset = Object()
        new = Integer()
        deleted = Integer()
        same = Integer()
        created_at = Date()

        def save(self, **kwargs):
            self.created_at = datetime.now()
            return super().save(**kwargs)

    def get_fbi_records(self, index: str) -> list:
        """
        Get all the fbi record with a path between after and stop.

        :param after: fbi records after this path will be returned
        :param stop: fbi records before and including this path will be returned
        :return: list of relevant fbi records
        """

        log.info("Querying FBI.")

        query = (
            Search(using="es", index=index)
            .extra(size=10000)
            .source(exclude=["phenomena"])
            .filter("term", type="file")
            .sort("path.keyword")
            .filter("range", path__keyword={"gt": self.fbi_path, "lte": "~"})
        )

        response = query.execute()

        log.info("FBI count: %s", response.hits.total)

        return iter(response.hits)

    def get_stac_assets(self, index: str) -> list:
        """
        Get all the STAC Asset with a path between after and stop.

        :param after: STAC Assets after this path will be returned
        :param stop: STAC Assets before and including this path will be returned
        :return: list of relevant STAC Assets
        """

        log.info("Querying STAC.")

        query = (
            Search(using="es", index=index)
            .extra(size=10000)
            .sort("properties.uri.keyword")
            .filter(
                "range", properties__uri__keyword={"gt": self.stac_path, "lte": "~"}
            )
        )

        response = query.execute()

        log.info("STAC Asset count: %s", response.hits.total)

        return iter(response.hits)

    def next_fbi_record(self):
        """
        Get the next fbi record
        """
        self.state.fbi_record = next(self.fbi_records, {"path": "~"})

    def next_stac_asset(self):
        """
        Get the next stac asset
        """
        self.state.stac_asset = next(self.stac_assets, {"properties": {"uri": "~"}})

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

        # message = {"uri": self.fbi_path}

        # self.producer.publish(self.rabbit_conf.get("ROUTING_KEY"), message)

        self.generator.process(self.fbi_path)

    def delete_stac_asset(self):
        """
        Remove a STAC Asset
        """
        pass

    # Could add compare class for data within STAC

    def run(self):
        """
        Compare the STAC Assets and FBI records
        """
        i = 0
        while True:
            # print and save every 1000 items
            if i % 1000 == 0:
                log.info("%s: %s  ---  %s", i, self.fbi_path, self.stac_path)
                self.state.save()
            i += 1

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
                continue

            # if stac has ended or the stac asset is behind the fbi then we need
            # to creae a new asset.
            if self.fbi_path == "~" or self.stac_path < self.fbi_path:
                self.delete_stac_asset()
                self.next_stac_asset()


if __name__ == "__main__":
    print("RUNNING")
    StacStocktake().run()
