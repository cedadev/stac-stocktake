#!/usr/bin/env python

"""
This script runs script `stac_stocktake_lotus_batch.py` for FBI index slices specified in the
config file defined by enviroment variable `STAC_STOCKTAKE_CONFIGURATION_FILE` or
`.stac_stocktake_lotus.yml`
"""

__author__ = "Rhys Evans"
__date__ = "2022-08-24"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD"


import os
import subprocess
from pathlib import Path

import yaml
from elasticsearch import Elasticsearch


def run_all():
    """
    Create a point in time for the FBI index. Then run batch
    for each slice as specified in the config.
    """

    current_directory = os.getcwd()

    config_file = os.environ.get("STAC_STOCKTAKE_CONFIGURATION_FILE")
    if not config_file:
        config_file = os.path.join(
            Path(__file__).parent,
            ".stac_stocktake_lotus.yml",
        )
        os.environ["STAC_STOCKTAKE_CONFIGURATION_FILE"] = config_file

    with open(config_file, encoding="utf-8") as reader:
        conf = yaml.safe_load(reader)

    general_conf = conf.get("GENERAL")
    es_conf = conf.get("ELASTICSEARCH")

    fbi_index = general_conf.get("FBI_INDEX")
    max_slices = general_conf.get("MAX_SLICES", 10)
    start_slice = general_conf.get("START_SLICE", 0)
    # end_slice can't exceed max_slices
    end_slice = min([general_conf.get("END_SLICE", max_slices) + 1, max_slices])
    pit_keep_alive = general_conf.get("PIT_KEEP_ALIVE", "5m")

    es_client = Elasticsearch(**es_conf.get("SESSION_KWARGS"))
    resp = es_client.open_point_in_time(index=fbi_index, keep_alive=pit_keep_alive)

    pit_id = resp["id"]

    for next_slice in range(start_slice, end_slice):
        cmd = f"{current_directory}/stac_stocktake_lotus_batch.py -s {next_slice} -p {pit_id}"
        print(f"RUNNING BATCH {next_slice} WITH COMMAND: {cmd}")
        subprocess.call(cmd, shell=True)


if __name__ == "__main__":
    print("RUNNING STAC STOCKTAKE LOTUS ALL")
    run_all()
