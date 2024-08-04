import csv
import ndjson
import os


def convert_ndjson_2_csv(csv_filename, nd_json_filename):
    with open(nd_json_filename, 'r') as f_in, open(csv_filename, 'w', newline='') as f_out:
        dataset_writer = csv.writer(f_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        reader = ndjson.reader(f_in)
        for line_num, json_line in enumerate(reader, 1):
            if line_num >= 2:
                dataset_writer.writerow(json_line)
            elif line_num == 1:
                header_row = [item['name'] for item in json_line['columns']]
                dataset_writer.writerow(header_row)


if __name__ == '__main__':
    ndjson_datasets = ["vs", "ae", "cm", "dd", "dm", "ds", "ec", "ex", "fa", "ie", "lb", "vs"]
    for dataset in ndjson_datasets:
        csv_file = os.path.join(os.getcwd(), "data", "sdtm-csv", dataset + ".csv")
        ndjson_file = os.path.join(os.getcwd(), "data", "sdtm-ndjson", dataset + ".ndjson")
        convert_ndjson_2_csv(csv_file, ndjson_file)