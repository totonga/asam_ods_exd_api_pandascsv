"""
ExternalFileData class to read data from an external file using pandas.
"""
import logging

import pandas as pd


class ExternalFileData:
    """
    Class to read data from an external file using pandas.
    """

    def __init__(self, file_path: str, parameters: dict):
        """
        Initialize the ExternalFileData class.
        :param file_path: Path to the external file.
        :param parameters: Parameters for reading the file (e.g., delimiter, header). Check pd.read_csv for details.
        """
        self.__file_path: str = file_path
        self.__parameters: dict = parameters
        self.__df: pd.DataFrame | None = None
        self.log = logging.getLogger(__name__)

    def close(self):
        """
        Close the file and release resources.
        """
        if self.__df is not None:
            self.log.info("Closing file: %s", self.__file_path)
            self.__df = None

    def data(self) -> pd.DataFrame:
        """
        Read the data from the file and return it as a pandas DataFrame.
        :return: DataFrame containing the data from the file.
        """
        if self.__df is None:
            self.log.info("Reading file: %s", self.__file_path)
            self.__df = pd.read_csv(self.__file_path, **self.__parameters)
        return self.__df

    def file_attributes(self) -> dict | None:
        """
        Return file attributes. Allows str, int, float datetime64.
        :return: Dictionary containing file attributes.
        """
        return None

    def group_attributes(self) -> dict | None:
        """
        Return file attributes. Allows str, int, float datetime64.
        :return: Dictionary containing file attributes.
        """
        return None

    def column_names(self) -> list[str] | None:
        """
        Alllows to overwrite the column names of the dataframe.
        If None is returned, the original column names are used.
        :return: List of column names.
        """
        return None

    def column_units(self) -> list[str] | None:
        """
        Allows to return column units of the dataframe.
        If None is returned, the units stay empty.
        """
        return None
