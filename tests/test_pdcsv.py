import os

import logging
import pathlib
import unittest

import pandas as pd


class TestPdCsv(unittest.TestCase):
    log = logging.getLogger(__name__)

    def __get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(
            __file__).parent.resolve(), '..', 'data', file_name)
        return pathlib.Path(example_file_path).resolve()

    def test_open(self):
        file_path = self.__get_example_file_path('example.csv')
        self.assertTrue(os.path.isfile(file_path))
        _df = pd.read_csv(file_path)
