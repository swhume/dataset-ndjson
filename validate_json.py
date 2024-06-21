from linkml.validator import validate
import json
import os


def validate_dataset(dataset_filename, standard) -> None:
    with open(dataset_filename, mode="r", encoding="utf-8") as f:
        dataset = json.load(f)
    report = validate(dataset, schema="dataset-json.yaml", target_class="Dataset")
    if report.results:
        print(f"Error: the following DSJ JSON {standard} dataset {dataset_filename} is invalid.")
        for result in report.results:
            print(result.message)
    else:
        print(f"The DSJ JSON {standard} dataset {dataset_filename} is valid!")


def convert_example_datasets(datasets, standard) -> None:
    for dataset in datasets:
        dataset_filename = os.path.join(os.getcwd(), "data", "roundtrip", standard, dataset + ".json")
        validate_dataset(dataset_filename, standard)


if __name__ == '__main__':
    with open(".\\data\\dataset-list.json") as f:
        ds_lists = json.load(f)
    for standard, datasets in ds_lists.items():
        convert_example_datasets(datasets, standard)
    # convert_example_datasets(["ae"], "sdtm")
