import ndjson
import json
import os

import sys
sys.path.append("C:\\Users\\SamHume\\git\\ndjsonlib")
from ndjsonlib.ndjson_data_file import NdjsonDataFile


def convert_ndjson_2_json(dataset_name, standard, json_filename):
    ndjson_path = os.path.join(os.getcwd(), "data", standard + "-ndjson")
    ndj = NdjsonDataFile(ds_name=dataset_name, directory=ndjson_path)
    ndj.read_dataset()
    ndj.write_dataset_json(json_filename)


def convert_example_datasets(datasets, standard):
    for dataset in datasets:
        json_filename = os.path.join(os.getcwd(), "data", "roundtrip", standard, dataset + ".json")
        convert_ndjson_2_json(dataset, standard, json_filename)


if __name__ == '__main__':
    with open(".\\data\\dataset-list.json") as f:
        ds_lists = json.load(f)
    for standard, datasets in ds_lists.items():
        convert_example_datasets(datasets, standard)
