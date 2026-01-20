"""
ExternalFileData class to read data from an external file using pandas.
"""

import logging
from typing import override

import pandas as pd

from exd_api_simple import ExdApiSimple


class ExternalFileData(ExdApiSimple):
    """
    Concrete implementation for reading CSV files.
    """

    @classmethod
    @override
    def create(cls, file_path: str, parameters: str) -> ExdApiSimple:
        """Factory method to create a file handler instance."""
        return cls(file_path, parameters)

    def __init__(self, file_path: str, parameters: dict):
        """
        Initialize the ExternalFileData class.
        :param file_path: Path to the external file.
        :param parameters: Parameters for reading the file (e.g., delimiter, header). Check pd.read_csv for details.
        """
        self.file_path: str = file_path
        self.parameters: dict = parameters
        self.df: pd.DataFrame | None = None
        self.log = logging.getLogger(__name__)

    @override
    def close(self) -> None:
        """
        Close the file and release resources.
        """
        if self.df is not None:
            self.log.info("Closing file: %s", self.file_path)
            del self.df
            self.df = None

    @override
    def not_my_file(self) -> bool:
        """
        Check if the file should be read with this plugin.
        :return: True if the file should not be read with this plugin, False otherwise.
        """
        # If the CSV file contains only a single column or all columns have datatype string,
        # we assume that it is not meant to be parsed with this plugin.
        df = self.data()
        if df.empty or len(df.columns) == 1 or all(df.dtypes == "object"):
            self.log.info(
                "File %s is not a valid CSV file for this plugin with parameters '%s'.",
                self.file_path,
                self.parameters,
            )
            return True
        return False

    @override
    def data(self) -> pd.DataFrame:
        """
        Read the data from the file and return it as a pandas DataFrame.
        :return: DataFrame containing the data from the file.
        """
        if self.df is None:
            self.log.info("Reading file: %s", self.file_path)
            try:
                self.df = pd.read_csv(self.file_path, **self.parameters)
            except pd.errors.ParserError as e:
                self.log.info("Not My File: Error reading file %s: %s", self.file_path, e)
                self.df = pd.DataFrame()
        return self.df


if __name__ == "__main__":
    from ods_exd_api_box import serve_plugin
    from exd_api_simple_impl import ExdApiSimpleImpl

    ExdApiSimple.register(ExternalFileData)
    serve_plugin("PANDASCSV", ExdApiSimpleImpl.create, ["*.csv"])
