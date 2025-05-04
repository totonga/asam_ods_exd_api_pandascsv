from google.protobuf.json_format import MessageToJson
from external_data_reader import ExternalDataReader
import ods_external_data_pb2 as oed
import ods_pb2 as ods
import unittest
import pathlib
import logging


class TestExdApi(unittest.TestCase):
    log = logging.getLogger(__name__)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(
            __file__).parent.resolve(), '..', 'data', file_name)
        return pathlib.Path(example_file_path).absolute().resolve().as_uri()

    def test_open(self):
        service = ExternalDataReader()
        handle = service.Open(oed.Identifier(
            url=self._get_example_file_path('example.csv'),
            parameters=""), None)
        try:
            pass
        finally:
            service.Close(handle, None)

    def test_structure(self):
        service = ExternalDataReader()
        handle = service.Open(oed.Identifier(
            url=self._get_example_file_path('example.csv'),
            parameters=""), None)
        try:
            structure = service.GetStructure(
                oed.StructureRequest(handle=handle), None)
            self.log.info(MessageToJson(structure))

            self.assertEqual(structure.name, 'example.csv')
            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 3)
            self.assertEqual(len(structure.groups[0].channels), 3)
            self.assertEqual(structure.groups[0].id, 0)
            self.assertEqual(structure.groups[0].channels[0].id, 0)
            self.assertEqual(structure.groups[0].channels[1].id, 1)
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertEqual(
                structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)
        finally:
            service.Close(handle, None)

    def test_get_values(self):
        service = ExternalDataReader()
        handle = service.Open(oed.Identifier(
            url=self._get_example_file_path('example.csv'),
            parameters=""), None)
        try:
            values = service.GetValues(oed.ValuesRequest(handle=handle,
                                                         group_id=0,
                                                         channel_ids=[0, 1],
                                                         start=0,
                                                         limit=4), None)
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 2)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)
            self.log.info(MessageToJson(values))

            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertSequenceEqual(
                values.channels[0].values.longlong_array.values, [1, 2, 3])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     2.1, 2.2, 2.3])

        finally:
            service.Close(handle, None)
