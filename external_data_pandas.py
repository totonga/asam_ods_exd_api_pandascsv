"""Abstract base class for reading external data files using pandas."""

from abc import ABC, abstractmethod

import pandas as pd


class ExternalDataPandas(ABC):
    """
    Class to read data from an external file using pandas.
    """

    @classmethod
    @abstractmethod
    def create(cls, file_path: str, parameters: str) -> "ExternalDataPandas":
        """Factory method to create a file handler instance.

        Args:
            file_path: Path to the external data file
            parameters: Optional parameters for file handling

        Returns:
            An instance of the file handler
        """

    @abstractmethod
    def close(self) -> None:
        """
        Close the file and release resources.
        """

    @abstractmethod
    def data(self) -> pd.DataFrame:
        """
        Read the data from the file and return it as a pandas DataFrame.
        :return: DataFrame containing the data from the file.
        """

    def not_my_file(self) -> bool:
        """
        Check if the file should be read with this plugin.
        :return: True if the file should not be read with this plugin, False otherwise.
        """
        return False

    def file_attributes(self) -> dict:
        """
        Return file attributes. Allows str, int, float datetime64.
        :return: Dictionary containing file attributes.
        """
        return {}

    def group_attributes(self) -> dict:
        """
        Return group attributes. Allows str, int, float datetime64.
        :return: Dictionary containing group attributes.
        """
        return {}

    def column_names(self) -> list[str] | None:
        """
        Allows to overwrite the column names of the dataframe.
        If None is returned, the original column names are used.
        :return: List of column names.
        """
        return None

    def column_units(self) -> list[str]:
        """
        Allows to return column units of the dataframe.
        :return: List of column units (empty list for no units).
        """
        return []

    def column_descriptions(self) -> list[str]:
        """
        Allows to return column descriptions of the dataframe.
        :return: List of column descriptions (empty list for no descriptions).
        """
        return []
