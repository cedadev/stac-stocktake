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


import os
import subprocess
from pathlib import Path
from typing import Tuple, Union

import click
import yaml
from elasticsearch_dsl import Search, connections


def create_fbi_file(
    search_size: int,
    slice_id: int,
    max_slices: int,
    pit_id: str,
    search_after: str,
    directory: str,
) -> Tuple[Union[str, None], Union[str, None]]:
    """
    Get the next 10k FBI records with a path after `search_after`
    and append them to the output file.

    :param search_size: number of hits to be returned from search
    :param slice_id: id of slice to be searched
    :param max_slices: maximum number of slices
    :param pit_id: id for point in time
    :param search_after: path to search after
    :param directory: output directory to save to

    :return: search_after for equivalent stac
    :return: search_after for next fbi search
    """

    query = (
        Search(using="es")
        .source(["path"])
        .filter("term", type="file")
        .exclude("exists", field="removed")
        .sort("path.keyword")
        .extra(size=search_size)
        .extra(slice={"id": slice_id, "max": max_slices})
        .extra(pit={"id": pit_id})
    )

    if search_after:
        query = query.extra(search_after=search_after)

    response = query.execute()

    if response.hits:
        first = response.hits[0]["path"]

        with open(f"{directory}/data", "a", encoding="utf-8") as fbi_file:

            for hit in response.hits:

                fbi_file.write(f"{hit['path']}\n")

                if not hit.meta.sort[0]:
                    return first, None

        return first, hit.meta.sort

    else:
        return None, None


def create_directory(directory: str) -> None:
    """
    create a given directory recursively if it does not yet exist.

    :param directory: Directory to be created.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


@click.command()
@click.option("-s", "--slice_id", required=True, type=int, help="Id of slice.")
@click.option(
    "-p", "--pit_id", required=True, type=str, help="Elasticsearch point in time id."
)
def run_batch(slice_id: int, pit_id: str) -> None:
    """
    submits specified chunks to lotus.

    :param slice_id: Id of slice
    :param pit_id: Elasticsearch point in time id
    """

    config_file = os.environ.get("STAC_STOCKTAKE_CONFIGURATION_FILE")
    if not config_file:
        config_file = os.path.join(
            Path(__file__).parent,
            ".stac_stocktake_lotus.yml",
        )

    with open(config_file, encoding="utf-8") as reader:
        conf = yaml.safe_load(reader)

    general_conf = conf.get("GENERAL")
    es_conf = conf.get("ELASTICSEARCH")

    connections.create_connection(alias="es", **es_conf.get("SESSION_KWARGS"))

    search_after = []

    max_slices = general_conf.get("MAX_SLICES", 10)
    search_size = general_conf.get("SEARCH_SIZE", 10000)
    search_per_chunk = general_conf.get("SEARCH_PER_CHUNK", 10)

    working_directory = general_conf.get("WORKING_DIRECTORY", os.getcwd())
    data_directory = general_conf.get("DATA_DIRECTORY", f"{working_directory}/data")
    slice_directory = f"{data_directory}/{slice_id}"

    input_directory_template = f"{slice_directory}/{{}}/input"
    output_directory_template = f"{slice_directory}/{{}}/output"

    count = 1
    chunk = 0

    input_directory = input_directory_template.format(chunk)
    create_directory(input_directory)

    output_directory = output_directory_template.format(chunk)
    create_directory(output_directory)

    stac_after, search_after = create_fbi_file(
        search_size, slice_id, max_slices, pit_id, search_after, input_directory
    )

    first_batch = True

    while True:
        # submit to lotus
        if count % search_per_chunk == 0 or not search_after:

            slurm_command = (
                f"sbatch -p {general_conf.get('QUEUE', 'short-serial')} "
                f"-t {general_conf.get('WALLCLOCK', '00:10')} "
                f"-o {output_directory}/standard.out -e {output_directory}/standard.err"
                f"{working_directory}/stac_stocktake_lotus_chunk.py "
                f"-s {slice_id} -c {chunk} -a {stac_after} -f {first_batch}"
            )

            first_batch = False

            subprocess.call(slurm_command, shell=True, env=os.environ)
            print(f"Batch {slice_id}: chunk {chunk} started")

            if not search_after:
                print(f"Batch {slice_id}: all ({chunk}) chunks started")
                break

            stac_after = (
                search_after if isinstance(search_after, str) else search_after[0]
            )

            chunk += 1

            input_directory = input_directory_template.format(chunk)
            create_directory(input_directory)

            output_directory = output_directory_template.format(chunk)
            create_directory(output_directory)

        _, search_after = create_fbi_file(
            search_size, slice_id, max_slices, pit_id, search_after, input_directory
        )

        count += 1


if __name__ == "__main__":
    run_batch()
