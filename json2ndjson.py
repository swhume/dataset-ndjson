import json
import requests
import os
import time

import sys
sys.path.append("C:\\Users\\SamHume\\git\\ndjsonlib")
from ndjsonlib.json_data_file import JsonDataFile


def get_dataset_json(data_file, dsj_ref):
    attempt_count = 0
    while True:
        try:
            r = requests.get(dsj_ref)
            r.raise_for_status()
        except requests.exceptions.ConnectionError as err:
            time.sleep(5)
            if attempt_count >= 4:
                raise SystemExit(err)
            else:
                attempt_count += 1
                continue
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)
        with open(data_file, 'w') as f:
            json.dump(r.json(), f)
            break


def convert_json_2_ndjson(json_filename, nd_json_filename, dataset_oid, dataset_name, standard):
    # read JSON datasets
    dir_path_in = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
    jf_json = JsonDataFile(ds_name=dataset_name, directory=dir_path_in)
    json_dataset = jf_json.read_full_json_dataset()
    # write NDJSON dataset
    dir_path_out = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", standard)
    jf_ndjson = JsonDataFile(ds_name=dataset_name, directory=dir_path_out)
    jf_ndjson.write_full_dataset(json_dataset)


def convert_example_datasets(json_datasets, standard):
    for dataset in json_datasets:
        dataset_oid = "IG." + dataset.upper()
        filename = os.path.join(os.getcwd(), "data", dataset + ".json")
        dsj_ref = f"https://github.com/cdisc-org/DataExchange-DatasetJson/blob/master/examples/{standard}/{dataset}.json?raw=True"
        ndjson_file = os.path.join(os.getcwd(), "data", standard, dataset + ".ndjson")
        get_dataset_json(filename, dsj_ref)
        convert_json_2_ndjson(filename, ndjson_file, dataset_oid, dataset, standard)


if __name__ == '__main__':
    with open(".\\data\\dataset-list.json") as f:
        ds_lists = json.load(f)
    for standard, datasets in ds_lists.items():
        convert_example_datasets(datasets, standard)

