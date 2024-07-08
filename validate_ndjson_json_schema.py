from linkml.validator import validate
import ndjson
import json
import os
from jsonschema import validate

"""
Validates all Dataset-JSON v1.1 NDJSON examples. Because it validates 1 line at a time this runs slowly. 
"""


def validate_dataset(dataset_filename, standard, json_schema) -> None:
    with open(dataset_filename, mode='r', encoding='utf-8') as f:
        error_count = 0
        data_row = {}
        reader = ndjson.reader(f)

        for line_num, json_line in enumerate(reader, 1):
            try:
                if line_num > 2:
                    data_row["rows"] = json_line
                    validate(instance=data_row, schema=json_schema["$defs"]["RowData"])
                elif line_num == 2:
                    validate(instance=json_line, schema=json_schema["$defs"]["ColumnMetadata"])
                elif line_num == 1:
                    validate(instance=json_line, schema=json_schema["$defs"]["DatasetMetadata"])
                else:
                    raise ValueError(f"Error: unknown line number in ndjson: {line_num}")
            except json.decoder.JSONDecodeError as ve:
                error_count += 1
                print(f"Invalid json on line {line_num} for dataset {dataset_filename}.\n{ve}")
        if not error_count:
            print(f"NDJSON dataset {dataset_filename} is valid based on the JSON schema.")

def convert_example_datasets(datasets, standard, json_schema) -> None:
    for dataset in datasets:
        dataset_filename = os.path.join(os.getcwd(), "data", standard, dataset + ".ndjson")
        validate_dataset(dataset_filename, standard, json_schema)


def get_ndjson_json_schema(schema_file):
    schema_filename = os.path.join(os.getcwd(), schema_file)
    with open(schema_filename, mode='rb') as f:
        schema = json.load(f)
    return schema


if __name__ == '__main__':
    # with open(".\\data\\dataset-list.json") as f:
    #     ds_lists = json.load(f)
    # for standard, datasets in ds_lists.items():
    #     convert_example_datasets(datasets, standard)
    json_schema = get_ndjson_json_schema("dataset-ndjson-schema.json")
    # convert_example_datasets(["dd"], "sdtm", json_schema)
    convert_example_datasets(["ae", "cm", "relrec", "suppdm", "fa", "tv", "vs", "dd"], "sdtm", json_schema)
