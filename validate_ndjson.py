from linkml.validator import validate
import ndjson
import json
from pathlib import Path

"""
Validates all Dataset-JSON v1.1 NDJSON examples. Because it validates 1 line at a time this runs slowly. 
"""

def validate_dataset(dataset_filename, standard, batch_size=100) -> None:
    with open(dataset_filename, mode='r', encoding='utf-8') as f:
        invalid_lines = []
        reader = ndjson.reader(f)
        batch = []
        for line_num, json_line in enumerate(reader, 1):
            if line_num == 1:
                # Validate the first line separately
                report = validate(json_line, schema="dataset-ndjson.yaml", target_class="DatasetMetadata")
                if report.results:
                    invalid_lines.append(line_num)
                    for result in report.results:
                        print(result.message)
            else:
                # Collect rows into a batch
                batch.append(json_line)
                if len(batch) >= batch_size:
                    invalid_lines.extend(validate_batch(batch, line_num - batch_size + 1))
                    batch = []
        
        # Validate any remaining lines in the batch
        if batch:
            invalid_lines.extend(validate_batch(batch, line_num - len(batch) + 1))

        if not invalid_lines:
            print(f"The DSJ NDJSON {standard} file {dataset_filename} is valid!")
        else:
            print(f"The following DSJ NDJSON lines from {standard} file {dataset_filename} are invalid {invalid_lines}")
            

def validate_example_datasets(datasets, standard) -> None:
    for dataset in datasets:
        dataset_filename = Path.cwd() / "data" / "ndjson" /f"{standard}-ndjson" / f"{dataset}.ndjson"
        validate_dataset(dataset_filename, standard)


def validate_batch(batch, start_line_num):
    """Validate a batch of NDJSON lines."""
    invalid_lines = []
    data_row = {"rows": batch}  # LinkML expects a list in 'rows'
    report = validate(data_row, schema="dataset-ndjson.yaml", target_class="RowData")
    if report.results:
        for result in report.results:
            print(result.message)
        invalid_lines.extend(range(start_line_num, start_line_num + len(batch)))
    return invalid_lines


if __name__ == '__main__':
    datalist_file_path = Path.cwd() / "data" / "dataset-list.json"
    with open(datalist_file_path) as f:
        ds_lists = json.load(f)
    for standard, datasets in ds_lists.items():
        validate_example_datasets(datasets, standard)
