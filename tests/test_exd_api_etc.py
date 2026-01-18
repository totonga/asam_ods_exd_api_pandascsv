import unittest
import pathlib
import logging

import grpc
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api, ods
from external_data_file import ExternalDataFile
from tests.mock_servicer_context import MockServicerContext

# pylint: disable=no-member


class TestExdApiEtc(unittest.TestCase):
    log = logging.getLogger(__name__)

    def setUp(self):
        """Register ExternalDataFile handler before each test."""
        FileHandlerRegistry.register(
            file_type_name="test", factory=ExternalDataFile)
        self.context = MockServicerContext()

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(
            __file__).parent.resolve(), '..', 'data', file_name)
        return pathlib.Path(example_file_path).absolute().resolve().as_uri()

    def test_semicolon_parameters(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example_semicolon.csv'),
            parameters='{"sep":";"}'), self.context)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), self.context)
            self.assertEqual(structure.name, 'example_semicolon.csv')
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

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), self.context)
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 2)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)

            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertSequenceEqual(
                values.channels[0].values.longlong_array.values, [1, 2, 3])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     2.1, 2.2, 2.3])

        finally:
            service.Close(handle, self.context)

    def test_comma_parameters(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example.csv'),
            parameters='{"sep":","}'), self.context)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), self.context)
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

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), self.context)
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 2)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)

            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertSequenceEqual(
                values.channels[0].values.longlong_array.values, [1, 2, 3])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     2.1, 2.2, 2.3])

        finally:
            service.Close(handle, self.context)

    def test_default(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example.csv'),
            parameters=None), self.context)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), self.context)
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

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), self.context)
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 2)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)

            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertSequenceEqual(
                values.channels[0].values.longlong_array.values, [1, 2, 3])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     2.1, 2.2, 2.3])

        finally:
            service.Close(handle, self.context)

    def test_no_header(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example_no_header.csv'),
            parameters='{"header":null}'), self.context)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), self.context)
            self.assertEqual(structure.name, 'example_no_header.csv')
            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 3)
            self.assertEqual(len(structure.groups[0].channels), 3)
            self.assertEqual(structure.groups[0].id, 0)
            self.assertEqual(structure.groups[0].channels[0].id, 0)
            self.assertEqual(structure.groups[0].channels[1].id, 1)
            self.assertEqual(structure.groups[0].channels[0].name, "0")
            self.assertEqual(structure.groups[0].channels[1].name, "1")
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertEqual(
                structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), self.context)
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 2)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)

            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertSequenceEqual(
                values.channels[0].values.longlong_array.values, [1, 2, 3])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     2.1, 2.2, 2.3])

        finally:
            service.Close(handle, self.context)

    def test_no_header2(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example_no_header.csv'),
            parameters='{"header":null,"names":["d", "e", "f"]}'), self.context)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), self.context)
            self.assertEqual(structure.name, 'example_no_header.csv')
            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 3)
            self.assertEqual(len(structure.groups[0].channels), 3)
            self.assertEqual(structure.groups[0].id, 0)
            self.assertEqual(structure.groups[0].channels[0].id, 0)
            self.assertEqual(structure.groups[0].channels[1].id, 1)
            self.assertEqual(structure.groups[0].channels[0].name, "d")
            self.assertEqual(structure.groups[0].channels[1].name, "e")
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertEqual(
                structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), self.context)
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 2)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)

            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertSequenceEqual(
                values.channels[0].values.longlong_array.values, [1, 2, 3])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     2.1, 2.2, 2.3])

        finally:
            service.Close(handle, self.context)

    def test_independent(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example.csv'),
            parameters=None), self.context)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), self.context)
            self.assertEqual(structure.name, 'example.csv')
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_LONGLONG)
            self.assertEqual(
                structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)

            self.assertEqual(
                structure.groups[0].channels[0].attributes.variables.get("independent").long_array.values[0], 1)
            self.assertIsNone(
                structure.groups[0].channels[1].attributes.variables.get("independent"))

        finally:
            service.Close(handle, self.context)

    def test_not_my_file(self):
        context = MockServicerContext()
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example_semicolon.csv'),
            parameters='{"sep":";"}'), context)
        try:
            service.GetStructure(
                exd_api.StructureRequest(handle=handle), context)
        finally:
            service.Close(handle, context)

        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example_semicolon.csv'),
            parameters='{"sep":","}'), context)
        try:
            with self.assertRaises(grpc.RpcError) as e:
                service.GetStructure(
                    exd_api.StructureRequest(handle=handle), context)
            self.assertEqual(
                context.code(), grpc.StatusCode.FAILED_PRECONDITION)
        finally:
            service.Close(handle, context)

    def test_not_my_file1(self):
        context = MockServicerContext()
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('not_my_file1.csv'),
            parameters='{"sep":","}'), context)
        try:
            with self.assertRaises(grpc.RpcError) as _:
                service.GetStructure(
                    exd_api.StructureRequest(handle=handle), context)
            self.assertEqual(
                context.code(), grpc.StatusCode.FAILED_PRECONDITION)
        finally:
            service.Close(handle, context)

    def test_not_my_file2(self):
        context = MockServicerContext()
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('not_my_file2.csv'),
            parameters='{"sep":","}'), context)
        try:
            with self.assertRaises(grpc.RpcError) as _:
                service.GetStructure(
                    exd_api.StructureRequest(handle=handle), context)
            self.assertEqual(
                context.code(), grpc.StatusCode.FAILED_PRECONDITION)
        finally:
            service.Close(handle, context)
