import json
from pathlib import Path
from ndjsonlib.ndjson_data_file import NdjsonDataFile


def convert_ndjson_2_json(dataset_name, standard, json_filename):
    ndjson_path = Path.cwd() / "data" / "ndjson" / f"{standard}-ndjson"
    ndj = NdjsonDataFile(ds_name=dataset_name, directory=ndjson_path)
    ndj.read_dataset()
    ndj.write_dataset_json(json_filename)


def convert_example_datasets(datasets, standard):
    for dataset in datasets:
        json_filename = Path.cwd() / "data" / "json" / f"{standard}-json" / f"{dataset}.json"
        json_filename.parent.mkdir(parents=True, exist_ok=True)
        convert_ndjson_2_json(dataset, standard, json_filename)


if __name__ == '__main__':
    datalist_file_path = Path.cwd() / "data" / "dataset-list.json"
    with open(datalist_file_path) as f:
        ds_lists = json.load(f)
    for standard, datasets in ds_lists.items():
        convert_example_datasets(datasets, standard)
    print(f"Successfully Converted NDJSON datasets to JSON format")