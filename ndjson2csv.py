import csv
import ndjson
import json
from pathlib import Path


def convert_ndjson_2_csv(ndjson_filename, csv_filename):
    with open(ndjson_filename, 'r') as f_in, open(csv_filename, 'w', newline='') as f_out:
        dataset_writer = csv.writer(f_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        reader = ndjson.reader(f_in)
        for line_num, json_line in enumerate(reader, 1):
            if line_num >= 2:
                dataset_writer.writerow(json_line)
            elif line_num == 1:
                header_row = [item['name'] for item in json_line['columns']]
                dataset_writer.writerow(header_row)


def convert_example_datasets(datasets, standard):
    for dataset in datasets:
        ndjson_filename = Path.cwd() / "data" / "ndjson" / f"{standard}-ndjson" / f"{dataset}.ndjson"

        csv_filename = Path.cwd() / "data" / "csv" / f"{standard}-csv" / f"{dataset}.csv"
        csv_filename.parent.mkdir(parents=True, exist_ok=True)

        convert_ndjson_2_csv(ndjson_filename,csv_filename)


if __name__ == '__main__':
    datalist_file_path = Path.cwd() / "data" / "dataset-list.json"
    with open(datalist_file_path) as f:
        ds_lists = json.load(f)
    for standard, datasets in ds_lists.items():
        convert_example_datasets(datasets, standard)
    print(f"Successfully Converted NDJSON datasets to CSV format")