#!/usr/bin/env python

"""
This script takes arguments from the command line and config from file defined by
enviroment variable `STAC_STOCKTAKE_CONFIGURATION_FILE` or `.stac_stocktake_lotus.yml`
submits script `stac_stocktake_chunk.py` to lotus for each of the chunks defined.
"""

__author__ = "Rhys Evans"
__date__ = "2022-08-24"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD"


import logging
import os
from pathlib import Path
from typing import Iterator, Tuple

import click
import yaml
from elasticsearch_dsl import Search, connections
from stac_generator.scripts.stac_generator import load_generator

log = logging.getLogger(__name__)


def get_stac_assets(
    stac_index: str, search_after: str, first: bool = False
) -> Iterator[dict]:
    """
    Get the next 10k STAC Asset with a uri after `search_after`.

    :param stac_index: elasticsearch index to search on
    :param search_after: uri to search after

    :return: iterator of relevant STAC Assets
    """

    log.info("Querying STAC.")

    query = (
        Search(using="es", index=stac_index)
        .source(["properties.uri"])
        .sort("properties.uri.keyword")
        .extra(size=10000)
    )

    # First should include search_after so use range
    if first:
        query = query.filter("range", properties__uri__keyword={"gte": search_after})
    else:
        query = query.extra(search_after=[search_after])

    response = query.execute()

    if response.hits:
        yield from response.hits

    else:
        yield {"properties": {"uri": "~"}}


def next_stac_path(
    stac_path: str, stac_index: str, stac_assets: Iterator
) -> Tuple[str, Iterator]:
    """
    Get the next stac path in the iterator

    :param stac_path: current stac path
    :param stac_index: elasticsearch index to search on
    :param stac_assets: stac assets iterator

    :return: next stac path
    :return: iterator of relevant STAC Assets
    """
    previous_stac_path = stac_path

    stac_asset = next(stac_assets, {"properties": {"uri": ""}})

    # if we reach the end of the stac assets get the next 10k results
    if not stac_asset["properties"]["uri"]:
        stac_assets = get_stac_assets(stac_index, previous_stac_path)
        next_stac_path(previous_stac_path, stac_index, stac_assets)

    # if the documents' sort is empty you have reached the end of the search
    if not stac_asset.meta.sort[0]:
        return "~", stac_assets

    return stac_asset["properties"]["uri"], stac_assets


def create_stac_asset(generator, fbi_path) -> None:
    """
    Insert a new STAC Asset from the fbi.

    :param fbi_path: path to be created
    """

    log.info("ADD_MISSING_STAC_ASSET: %s", fbi_path)

    generator.process(fbi_path)


@click.command()
@click.option("-x", "--config_file", required=True, type=int, help="config file path.")
@click.option("-s", "--slice_id", required=True, type=int, help="Id of slice.")
@click.option("-c", "--chunk_id", required=True, type=int, help="Id of chunk.")
@click.option("-a", "--after", required=True, type=str, help="Search_after for STAC.")
@click.option("-f", "--first", required=True, type=bool, help="If first chunk.")
def run_chunk(
    config_file: str, slice_id: int, chunk_id: int, after: str, first: bool
) -> None:
    """
    Compare list of fbi paths to stac paths and create stac assets where
    necessary

    :param slice_id: id of slice
    :param chunk_id: id of chunk
    :param after: search_after for STAC
    """
    with open(config_file, encoding="utf-8") as reader:
        conf = yaml.safe_load(reader)

    general_conf = conf.get("GENERAL")
    es_conf = conf.get("ELASTICSEARCH")
    generator_conf = conf.get("GENERATOR")
    log_conf = conf.get("LOGGING")

    connections.create_connection(alias="es", **es_conf.get("SESSION_KWARGS"))

    generator = load_generator(generator_conf)

    working_directory = general_conf.get("WORKING_DIRECTORY", os.getcwd())
    data_directory = general_conf.get("DATA_DIRECTORY", f"{working_directory}/data")
    chunk_directory = f"{data_directory}/{slice_id}/{chunk_id}"

    logging.basicConfig(
        filename=f"{chunk_directory}/output/chunk.log",
        format="%(asctime)s @%(name)s [%(levelname)s]:    %(message)s",
        level=logging.getLevelName(log_conf.get("LEVEL")),
    )

    stac_index = general_conf.get("STAC_INDEX")

    with open(f"{chunk_directory}/input/data", "r", encoding="utf-8") as fbi_file:

        fbi_path = fbi_file.readline().strip()
        stac_assets = get_stac_assets(stac_index, after, first)
        stac_path, stac_assets = next_stac_path("", stac_index, stac_assets)

        count = 0
        new = 0
        exists = 0
        removed = 0

        while True:

            # if fbi has ended stop
            if fbi_path == "":
                break

            count += 1

            # print and save every 1000 items
            if count % 10000 == 0:
                log.info(
                    "%s  ---  %s | total: %s | new: %s | exists: %s | removed: %s",
                    fbi_path, stac_path, count, new, exists, removed,
                )

            # if stac has ended or the stac is ahead and there are fbi records left,
            # then we need to create a new asset.
            if stac_path == "~" or stac_path > fbi_path:
                new += 1

                create_stac_asset(generator, fbi_path)
                fbi_path = fbi_file.readline()

            # if stac is behind read next stac line.
            elif stac_path < fbi_path:
                removed += 1

                stac_path, stac_assets = next_stac_path(
                    stac_path, stac_index, stac_assets
                )

            # if both record and asset are the same path then move on.
            elif fbi_path == stac_path:
                exists += 1

                fbi_path = fbi_file.readline()
                stac_path, stac_assets = next_stac_path(
                    stac_path, stac_index, stac_assets
                )

        log.info(
            "CHUNK FINISHED: %s  ---  %s | total: %s | new: %s | exists: %s | removed: %s",
            fbi_path, stac_path, count, new, exists, removed,
        )


if __name__ == "__main__":
    run_chunk()
