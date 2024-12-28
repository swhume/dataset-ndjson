import json
import ndjsonlib.models.dataset as DS


class DataFile:
    def __init__(self, filename: str, chunk_size: int = 1000, row_data: DS.RowData = None):
        """
        Reads and writes to an ndjson file that contains the data part of a Dataset-JSON dataset with the metadata
        stored in a separate file. Provides the reading and writing service to the JsonDataFile class. The MetadataFile
        class complements this one as together they form the metadata and data needed to represent a Dataset-JSON
        dataset.
        :param filename: name of the data file for the Dataset-JSON dataset
        :param chunk_size: number of data rows to read in 1 chunk - allows reading a large dataset incrementally
        :param row_data: data rows to be write or append to a data file portion of the Dataset-JSON dataset
        """
        self.filename = filename
        self.chunk_size = chunk_size
        self.current_row = 0
        self.row_data = row_data

    def read_file(self, is_return_list: bool = False) -> DS.RowData:
        """ reads entire data file into a RowData instance """
        rows = []
        with open(self.filename) as f:
            for line in f:
                rows.append(json.loads(line))
        if is_return_list:
            return rows
        return DS.RowData(rows=rows)

    def read_chunk(self, start_row: int = None, is_return_list: bool = False) -> list:
        """
        reads a chunk_size number of rows into a RowData instance starting from start_row
        :start_row: starting row to read
        :is_return_list: whether to return a list of rows instead of a RowData instance
        """
        rows = []
        if start_row is None:
            start_row = self.current_row + 1
        with open(self.filename, mode='r') as f:
            line_count = 0
            for line in f:
                if line_count >= start_row:
                    rows.append(json.loads(line))
                line_count += 1
                if self.chunk_size and line_count >= self.chunk_size:
                    break
        self.current_row = start_row + len(rows)
        if is_return_list:
            return rows
        return DS.RowData(rows=rows)

    def write_file(self, filename: str = None) -> None:
        """
        writes row_data attribute content to an ndjson data file - assumes object created with row_data
        :param filename: optional filename to write to when not write to instance filename
        """
        if filename is None:
            filename = self.filename
        with open(filename, mode='w') as f:
            for row in self.row_data.rows:
                f.write(f"{json.dumps(row)}\n")
            f.flush()

    def append_file(self, filename: str = None) -> int:
        """
        appends data rows to an existing ndjson data file
        :param filename: optional filename to write to when not write to instance filename
        :return: number of rows appended
        """
        if filename is None:
            filename = self.filename
        row_counter = 0
        with open(filename, mode='a') as f:
            for row in self.row_data.rows:
                f.write(f"{json.dumps(row)}\n")
                row_counter += 1
            f.flush()
        return row_counter

    def show_file(self, show_top: int = 500) -> None:
        """ prints file to stdout to aid in development or debugging """
        with open(self.filename) as f:
            for count, line in enumerate(f):
                if count < show_top:
                    print(json.loads(line))
