import ndjson
import json
import os


def convert_ndjson_2_json(json_filename, nd_json_filename):
    dataset_type = ""
    with open(nd_json_filename, 'r') as f_in, open(json_filename, 'w', newline='') as f_out:
        dataset_dict = {}
        OID = ""
        reader = ndjson.reader(f_in)
        for line_num, json_line in enumerate(reader):
            if line_num >= 3:
                dataset_dict[dataset_type]["itemGroupData"][OID]["itemData"].append(json_line)
            elif line_num == 2:
                dataset_dict[dataset_type]["itemGroupData"][OID]["items"] = json_line["items"]
                dataset_dict[dataset_type]["itemGroupData"][OID]["itemData"] = []
            elif line_num == 1:
                if json_line["datasetType"] == "clinicalData":
                    dataset_type = "clinicalData"
                else:
                    dataset_type = "referenceData"
                dataset_dict[dataset_type] = gen_dataset_attributes(json_line)
                OID = json_line["OID"]
            elif line_num == 0:
                dataset_dict = json_line
        json.dump(dataset_dict, f_out)


def gen_dataset_attributes(json_line):
    if "studyOID" in json_line:
        attributes = {"studyOID": json_line["studyOID"]}
    if "metaDataVersionOID" in json_line:
        attributes["metaDataVersionOID"] = json_line["metaDataVersionOID"]
    if "metaDataRef" in json_line:
        attributes["metaDataRef"] = json_line["metaDataRef"]
    ig_attributes = {"records": json_line["records"], "name": json_line["name"], "label": json_line["label"]}
    attributes["itemGroupData"] = {json_line["OID"]: ig_attributes}
    return attributes


if __name__ == '__main__':
    ndjson_datasets = ["vs", "ae", "cm", "dd", "dm", "ds", "ec", "ex", "fa", "ie", "lb", "vs"]
    for dataset in ndjson_datasets:
        json_file = os.path.join(os.getcwd(), "data", dataset + ".json")
        ndjson_file = os.path.join(os.getcwd(), "data", dataset + ".ndjson")
        convert_ndjson_2_json(json_file, ndjson_file)