import os
from pathlib import Path


class DatasetName:
    """
    Generate filenames with paths based on a dataset name and directory. Supports creating separate filenames for the
    metadata and data ndjson files. Also generates filenames with paths for the full json and ndjson dataset files.
    """
    def __init__(self, directory: str = None, dataset_name: str = None, full_name: str = None):
        """
        create the file names for the data and metadata ndjson files, in addition to the file names for the full json
        and ndjson datasets.
        :param directory: path of the dataset directory
        :param dataset_name: dataset name (e.g. ae) but not the name of the dataset file
        :param full_name: alternatively pass a full path and dataset filename that will be deconstructed
        """
        if directory and dataset_name:
            self.path = directory
            self.ds_name = dataset_name
        elif full_name:
            self.ds_name = Path(full_name).stem
            self.path = str(Path(full_name).parent)
        else:
            raise ValueError('Either a directory and dataset name or a full name (path with filename is required.')
        self.metadata_filename = self._create_metadata_filename()
        self.data_filename = self._create_data_filename()
        self.dataset_filename = self._create_full_dataset_filename()
        self.dataset_json_filename = self._create_json_dataset_filename()

    def get_metadata_filename(self):
        """ returns the filename for the metadata only file - 1 row """
        return self.metadata_filename

    def get_data_filename(self):
        """ returns the filename for the dataset data rows """
        return self.data_filename

    def get_full_dataset_filename(self):
        """ returns the filename for the full ndjson dataset file """
        return self.dataset_filename

    def get_json_dataset_filename(self):
        """ returns the filename for the full json dataset file """
        return self.dataset_json_filename

    def get_path(self):
        """ returns the path to the dataset files """
        return self.path

    def get_ds_name(self):
        """ returns the name of the dataset which is not the same as the dataset file name"""
        return self.ds_name

    def _create_metadata_filename(self) -> str:

        filename = os.path.join(self.path, self.ds_name + "_metadata.ndjson")
        return filename

    def _create_data_filename(self) -> str:
        filename = os.path.join(self.path, self.ds_name + "_data.ndjson")
        return filename

    def _create_full_dataset_filename(self) -> str:
        filename = os.path.join(self.path, self.ds_name + ".ndjson")
        return filename

    def _create_json_dataset_filename(self):
        filename = os.path.join(self.path, self.ds_name + ".json")
        return filename
