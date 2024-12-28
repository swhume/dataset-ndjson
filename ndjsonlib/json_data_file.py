import json
import ijson
import ndjsonlib.metadata_file as MF
import ndjsonlib.data_file as DF
import ndjsonlib.dataset_name as DN
import ndjsonlib.models.dataset as DS
import datetime


class JsonDataFile:

    def __init__(self, ds_name: str, directory: str, chunk_size: int = 1000):
        """
        reads in a ndjson dataset and returns a JSON version or gets a JSON dataset and writes it as ndjson
        """
        dsn = DN.DatasetName(directory, ds_name)
        self.metadata_filename = dsn.get_metadata_filename()
        self.data_filename = dsn.get_data_filename()
        self.dataset_filename = dsn.get_full_dataset_filename()
        self.dataset_json_filename = dsn.get_json_dataset_filename()
        self.creation_date_time = datetime.datetime.utcnow()
        self.chunk_size = chunk_size
        self.current_row = 0
        self.number_of_rows = 0

    def get_creation_date_time(self):
        """ returns the creation date and time in ISO 8601 format """
        return self.creation_date_time

    def get_number_of_rows(self):
        """ returns the number of rows in the dataset """
        return self.number_of_rows

    def read_dataset(self) -> json:
        """ reads the entire ndjson dataset and returns it; includes metadata and data """
        dataset = self._read_metadata()
        df = DF.DataFile(self.data_filename)
        ds = df.read_file()
        dataset['rows'] = ds.model_dump(mode='json')['rows']
        self.number_of_rows = dataset["records"]
        return dataset

    def read_dataset_chunk(self, offset: int) -> json:
        """
        reads the ndjson metadata and a chunk of the data rows starting from the offset record and continuing for
        chunk_size rows
        """
        dataset = self._read_metadata()
        df = DF.DataFile(self.data_filename)
        ds = df.read_chunk(start_row=offset)
        dataset['rows'] = ds.model_dump(mode='json')['rows']
        self.number_of_rows = len(dataset["rows"])
        return dataset

    def read_full_json_dataset(self) -> json:
        """
        Utility method that reads a JSON dataset for conversion to ndjson. This method reads an entire dataset into
        memory and should only be used with smaller datasets. For large datasets, use read_json_dataset_iterator
        """
        with open(self.dataset_json_filename) as f:
            dataset = json.load(f)
        return dataset

    def read_json_dataset_iterator(self) -> json:
        """ utility method that reads a large JSON dataset incrementally for conversion to ndjson """
        with open(self.dataset_json_filename, 'r') as file:
            objects = ijson.items(file, 'item')
            for obj in objects:
                yield obj

    def write_dataset(self, dataset: dict) -> None:
        """ given a dataset as a dictionary this method writes it out an ndjson data and metadata files """
        ds = DS.RowData(rows=dataset["rows"])
        df = DF.DataFile(filename=self.dataset_filename, row_data=ds)
        del dataset['rows']
        self.write_metadata(dataset)
        df.write_file(self.data_filename)

    def write_full_dataset(self, dataset: dict) -> None:
        """ utility method that writes ndjson dataset as 1 combined file, metadata + data """
        ds = DS.RowData(rows=dataset["rows"])
        df = DF.DataFile(filename=self.data_filename, row_data=ds)
        del dataset['rows']
        self.write_metadata(dataset, self.dataset_filename)
        rows_added = df.append_file(self.dataset_filename)
        return rows_added

    def append_dataset(self, dataset: dict) -> int:
        """ append rows to the dataset and then update the metadata creation datetime and records attributes """
        ds = DS.RowData(rows=dataset["rows"])
        df = DF.DataFile(filename=self.data_filename, row_data=ds)
        rows_added = df.append_file(self.data_filename)
        self.update_metadata_on_append(rows_added)
        return rows_added

    def update_metadata_on_append(self, rows_added) -> None:
        """ updates the metadata creation datetime and records attributes after a separate method appends data rows """
        self.number_of_rows = 0
        mf = MF.MetadataFile(self.metadata_filename)
        mf.read_file()
        dataset = mf.get_metadata()
        dataset.records += rows_added
        dataset.datasetJSONCreationDateTime = self.creation_date_time
        mf_out = MF.MetadataFile(self.metadata_filename, dataset)
        mf_out.write_file(self.metadata_filename)

    def read_metadata(self) -> dict:
        """ reads the dataset metadata and returns it """
        dataset = self._read_metadata()
        self.number_of_rows = 0
        return dataset

    def _read_metadata(self) -> dict:
        mf = MF.MetadataFile(self.metadata_filename)
        mf.read_file()
        dataset = mf.get_metadata_json()
        return dataset

    def write_metadata(self, metadata: dict, filename: str = None) -> None:
        if filename is None:
            filename = self.metadata_filename
        metadata["datasetJSONCreateDateTime"] = datetime.datetime.utcnow()
        dataset_metadata = DS.DatasetMetadata(**metadata)
        mf = MF.MetadataFile(filename, dataset_metadata=dataset_metadata)
        mf.write_file()
