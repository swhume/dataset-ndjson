import json
import pydantic
import requests
import time
import argparse
from pathlib import Path
from ndjsonlib.json_data_file import JsonDataFile


def get_dataset_json(json_datafile, dataset_json_ref):
    attempt_count = 0

    while True:
        try:
            r = requests.get(dataset_json_ref)
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
        with open(json_datafile, 'w') as f:
            json.dump(r.json(), f)
            break


"""
Loading json files from directory 'data/json/**/*'
Convert to ndjson and write data into files under directory 'data/ndjson/**/*'   
"""
def convert_json_2_ndjson(dataset_name, standard):
    try:
        # read JSON datasets
        dir_path_in = Path.cwd() / "data"/ "json" / f"{standard}-json"
        jf_json = JsonDataFile(ds_name=dataset_name, directory=dir_path_in)
        json_dataset = jf_json.read_full_json_dataset()

        # write NDJSON dataset
        dir_path_out = Path.cwd() / "data"/ "ndjson" / f"{standard}-ndjson"
        dir_path_out.mkdir(parents=True, exist_ok=True)

        jf_ndjson = JsonDataFile(ds_name=dataset_name, directory=dir_path_out)
        jf_ndjson.write_full_dataset(json_dataset)

    except pydantic.ValidationError as ve:
        print(f"found error in dataset {dataset_name}: {ve}")

    except NotADirectoryError as error:
        print(f"found error in dataset {dataset_name}: {error}")

    except FileNotFoundError as error:
        print(f"found error in dataset {dataset_name}: {error}")



def convert_example_datasets(json_datasets, standard, args):
    for dataset in json_datasets:
        json_datafile = Path.cwd() / "data"/ "json" / f"{standard}-json" / f"{dataset}.json"
        json_datafile.parent.mkdir(parents=True, exist_ok=True)

        if not args.is_no_retrieval:
            dataset_json_ref = f"https://github.com/cdisc-org/DataExchange-DatasetJson/blob/master/examples/{standard}/{dataset}.json?raw=True"
            get_dataset_json(json_datafile, dataset_json_ref)

        convert_json_2_ndjson(dataset, standard)


def set_cmd_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n", "--no_retrieval", 
        help="skip downloading the JSON datasets",
        const=True, 
        nargs='?', 
        dest="is_no_retrieval", 
        default=False
    )
    
    default_path = Path(__file__).resolve().parent / 'data'
    parser.add_argument(
        "-p",
        "--path",
        help="Directory containing Dataset-JSON datasets",
        required=False,
        dest="path",
        default=str(default_path)
    )
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = set_cmd_line_args()
    datalist_file_path = Path.cwd() / "data" / "dataset-list.json"
    with open(datalist_file_path) as f:
        ds_lists = json.load(f)
    for standard, datasets in ds_lists.items():
        convert_example_datasets(datasets, standard, args)
    print(f"Successfully Converted JSON datasets to NDJSON format")
        