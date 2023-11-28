import json
import requests
import ndjson
import os
import time


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


def convert_json_2_ndjson(json_filename, nd_json_filename, dataset_oid):
    with open(json_filename, 'r') as f:
        json_data = json.loads(f.read())

    with open(nd_json_filename, 'w') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        writer.writerow(create_odm_metadata(json_data))
        writer.writerow(create_dataset_metadata(json_data, dataset_oid))
        writer.writerow({"items": json_data["clinicalData"]["itemGroupData"][dataset_oid]["items"]})
        for row in json_data["clinicalData"]["itemGroupData"][dataset_oid]["itemData"]:
            writer.writerow(row)


def create_dataset_metadata(json_data, dataset_oid):
    keys = ["studyOID", "metaDataVersionOID", "metaDataRef", "records", "name", "label"]
    line = {"datasetType": "clinicalData", "OID": dataset_oid}
    for key in keys:
        # TODO only works for clinicalData currently - will need to add logic for referenceData
        if key in json_data["clinicalData"]:
            line[key] = json_data["clinicalData"][key]
        elif key in json_data["clinicalData"]["itemGroupData"][dataset_oid]:
            line[key] = json_data["clinicalData"]["itemGroupData"][dataset_oid][key]
    return line


def create_odm_metadata(json_data):
    keys = ["creationDateTime", "datasetJSONVersion", "fileOID", "asOfDateTime", "originator", "sourceSystem", "sourceSystemVersion"]
    line = {}
    for key in keys:
        if key in json_data:
            line[key] = json_data[key]
    return line


if __name__ == '__main__':
    json_datasets = ["vs", "ae", "cm", "dd", "dm", "ds", "ec", "ex", "fa", "ie", "lb", "vs"]
    for dataset in json_datasets:
        dataset_oid = "IG." + dataset.upper()
        filename = os.path.join(os.getcwd(), "data", dataset + ".json")
        dsj_ref = 'https://github.com/cdisc-org/DataExchange-DatasetJson/blob/master/examples/sdtm/' + dataset + '.json?raw=True'
        ndjson_file = os.path.join(os.getcwd(), "data", dataset + ".ndjson")
        get_dataset_json(filename, dsj_ref)
        convert_json_2_ndjson(filename, ndjson_file, dataset_oid)
