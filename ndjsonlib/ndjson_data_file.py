import json
import ndjsonlib.dataset_name as dn


class NdjsonDataFile:
    def __init__(self, ds_name: str, directory: str, chunk_size: int = 1000):
        """
        reads and writes ndjson datasets as one file
        :param ds_name:
        :param directory:
        :param chunk_size:
        """
        dsn = dn.DatasetName(directory, ds_name)
        self.dataset_filename = dsn.get_full_dataset_filename()
        self.chunk_size = chunk_size
        self.current_row = 0
        self.number_of_rows = 0
        self.metadata = None
        self.rows = []

    def get_row_count(self):
        return len(self.rows)

    def get_metadata(self):
        return self.metadata

    def get_columns(self):
        return self.metadata["columns"]

    def get_rows(self):
        return self.rows

    def read_dataset(self) -> None:
        """ read in an NDJSON dataset and load the content into the metadata and rows attributes """
        with open(self.dataset_filename) as f:
            for line_num, line in enumerate(f):
                dataset_line = json.loads(line)
                if line_num:
                    self.rows.append(dataset_line)
                else:
                    self.metadata = dataset_line

    def read_dataset_chunk(self, start_row: int) -> None:
        """
        reads the ndjson metadata and a chunk of the data rows starting from the start_row record and continuing for
        chunk_size rows
        :start_row: starting row to read
        """
        self.rows = []
        with open(self.dataset_filename) as f:
            for line_num, line in enumerate(f):
                dataset_line = json.loads(line)
                if line_num == 0:
                    self.metadata = dataset_line
                elif line_num >= start_row:
                    self.rows.append(dataset_line)
        self.number_of_rows = len(self.rows)
        return self.rows

    def read_dataset_iterator(self) -> json:
        """ utility method that reads a large NDJSON dataset and creates an iterator """
        with open(self.dataset_filename, 'r') as file:
            for line in file:
                yield line

    def write_dataset_json(self, dataset_filename: str) -> None:
        """ writes a ndjson dataset as JSON; assumes dataset will fit into memory """
        dataset = self.metadata
        dataset["rows"] = self.rows
        with open(dataset_filename, "w") as f:
            json.dump(dataset, f)

    def write_dataset_ndjson_chunk(self, dataset_filename: str, is_write_metadata: bool = True) -> None:
        """
        writes a chunk of ndjson rows to the dataset as NDJSON; assumes that if is_write_metadata is True then it
        is creating a new dataset, otherwise it just appends NDJSON data rows.
        :param dataset_filename: is the path and filename of the ndjson dataset
        :param is_write_metadata: write a new dataset with the metadata row or append data rows to an existing one
        """
        dataset = self.metadata
        dataset["rows"] = self.rows
        if is_write_metadata:
            mode = 'w'
        else:
            mode = 'a'
        with open(dataset_filename, mode=mode) as f:
            if is_write_metadata:
                f.write(f"{json.dumps(self.metadata)}\n")
            for row in self.rows:
                f.write(f"{json.dumps(row)}\n")
            f.flush()
