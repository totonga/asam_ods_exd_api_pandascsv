import os

import logging
import pathlib
import unittest

import pandas as pd
import tempfile


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
        self.assertFalse(_df.empty)

    def test_not_my_file(self):
        file_path = self.__get_example_file_path('not_my_file1.csv')
        self.assertTrue(os.path.isfile(file_path))
        self.assertRaises(pd.errors.ParserError,
                          lambda: pd.read_csv(file_path))

    def test_not_my_file2(self):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b'\x00\x01\x02\x03\x04\x05')
            temp_file_path = temp_file.name

        self.assertTrue(os.path.isfile(temp_file_path))
        df = pd.read_csv(temp_file_path)
        self.assertTrue(df.empty)

        os.remove(temp_file_path)
