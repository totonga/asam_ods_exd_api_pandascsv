"""EXD API implementation for CSV files"""
import os
import numpy as np
import pandas as pd
from pathlib import Path
import re
import threading
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname
import json

import grpc
import ods_pb2 as ods
import ods_external_data_pb2 as exd_api
import ods_external_data_pb2_grpc

from external_file_data import ExternalFileData


class FileCache:
    def __init__(self, file_path: str, parameters: str | None):
        self.__lock = threading.Lock()
        self.__file_path = file_path
        self.__parameters: dict = json.loads(parameters) if parameters else {}
        self.__efd: ExternalFileData = None
        self.__datatypes = None

    def close(self):
        with self.__lock:
            if self.__efd is not None:
                self.__efd.close()
                self.__efd = None
                self.__datatypes = None

    def column_datatype(self, index: int):
        if self.__datatypes is None:
            self.__datatypes = self.column_datatypes()

        if index >= len(self.__datatypes):
            raise IndexError(f'Column index {index} out of range!')
        return self.__datatypes[index]

    def column_data(self, index: int) -> pd.Series:
        data = self.__data()
        if index >= data.shape[1]:
            raise IndexError(f'Column index {index} out of range!')
        return data.iloc[:, index]

    def not_my_file(self) -> bool:
        if hasattr(self.__external_file_data(), 'not_my_file'):
            return self.__external_file_data().not_my_file()
        return False

    def file_attributes(self) -> dict:
        rv = None
        if hasattr(self.__external_file_data(), 'file_attributes'):
            rv = self.__external_file_data().file_attributes()
        return rv if rv is not None else {}

    def group_attributes(self) -> dict:
        rv = None
        if hasattr(self.__external_file_data(), 'group_attributes'):
            rv = self.__external_file_data().group_attributes()
        return rv if rv is not None else {}

    def column_names(self) -> list[str]:
        rv = None
        if hasattr(self.__external_file_data(), 'column_names'):
            rv = self.__external_file_data().column_names()
        return rv if rv is not None else self.__data().columns.tolist()

    def column_units(self) -> list[str]:
        rv = None
        if hasattr(self.__external_file_data(), 'column_units'):
            rv = self.__external_file_data().column_units()
        return rv if rv is not None else [""] * self.number_of_columns()

    def column_descriptions(self) -> list[str]:
        rv = None
        if hasattr(self.__external_file_data(), 'column_descriptions'):
            rv = self.__external_file_data().column_descriptions()
        return rv if rv is not None else [""] * self.number_of_columns()

    def column_datatypes(self) -> list[ods.DataTypeEnum]:
        return [self.__get_datatype(col_type) for col_type in self.__data().dtypes]

    def number_of_rows(self):
        return int(self.__data().shape[0])

    def number_of_columns(self):
        return int(self.__data().shape[1])

    def leading_independent(self) -> bool:
        column: pd.Series = self.column_data(0)
        return column.is_monotonic_increasing and column.is_unique

    def __data(self) -> pd.DataFrame:
        return self.__external_file_data().data()

    def __external_file_data(self) -> ExternalFileData:
        with self.__lock:
            if self.__efd is None:
                self.__efd = ExternalFileData(
                    self.__file_path, self.__parameters)
            return self.__efd

    def __get_datatype(self, data_type: np.dtype) -> ods.DataTypeEnum:
        if np.issubdtype(data_type, np.complex64):
            return ods.DataTypeEnum.DT_COMPLEX
        elif np.issubdtype(data_type, np.complex128):
            return ods.DataTypeEnum.DT_DCOMPLEX
        elif np.issubdtype(data_type, np.int8):
            return ods.DataTypeEnum.DT_SHORT
        elif np.issubdtype(data_type, np.uint8):
            return ods.DataTypeEnum.DT_BYTE
        elif np.issubdtype(data_type, np.int16):
            return ods.DataTypeEnum.DT_SHORT
        elif np.issubdtype(data_type, np.uint16):
            return ods.DataTypeEnum.DT_LONG
        elif np.issubdtype(data_type, np.int32):
            return ods.DataTypeEnum.DT_LONG
        elif np.issubdtype(data_type, np.uint32):
            return ods.DataTypeEnum.DT_LONGLONG
        elif np.issubdtype(data_type, np.int64):
            return ods.DataTypeEnum.DT_LONGLONG
        elif np.issubdtype(data_type, np.uint64):
            return ods.DataTypeEnum.DT_DOUBLE
        elif np.issubdtype(data_type, np.datetime64):
            return ods.DataTypeEnum.DT_DATE
        elif np.issubdtype(data_type, np.float32):
            return ods.DataTypeEnum.DT_FLOAT
        elif np.issubdtype(data_type, np.float64):
            return ods.DataTypeEnum.DT_DOUBLE
        elif np.issubdtype(data_type, object):
            return ods.DataTypeEnum.DT_STRING
        raise NotImplementedError(f'Unknown type {data_type}!')


class ExternalDataReader(ods_external_data_pb2_grpc.ExternalDataReader):

    def Open(self, identifier: exd_api.Identifier, context: grpc.ServicerContext):
        file_path = Path(self.__get_path(identifier.url))
        if not file_path.is_file():
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"File '{identifier.url}' not found.")

        connection_id = self.__open_file(identifier)

        rv = exd_api.Handle(uuid=connection_id)
        return rv

    def Close(self, handle: exd_api.Handle, context: grpc.ServicerContext):
        self.__close_file(handle)
        return exd_api.Empty()

    def GetStructure(self, request: exd_api.StructureRequest, context: grpc.ServicerContext):

        if request.suppress_channels or request.suppress_attributes or 0 != len(request.channel_names):
            context.abort(grpc.StatusCode.UNIMPLEMENTED,
                          "Method not implemented!")

        identifier = self.connection_map[request.handle.uuid]
        file: FileCache = self.__get_file(request.handle)
        if file.not_my_file():
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Not my file!")

        rv = exd_api.StructureResult(
            identifier=identifier,
            name=Path(identifier.url).name
        )
        self.__add_attributes(file.file_attributes(), rv.attributes)

        new_group = exd_api.StructureResult.Group(
            name="data",
            id=0,
            total_number_of_channels=file.number_of_columns(),
            number_of_rows=file.number_of_rows())
        self.__add_attributes(file.group_attributes(), new_group.attributes)

        channel_names = file.column_names()
        channel_datatypes = file.column_datatypes()
        channel_units = file.column_units()
        channel_descriptions = file.column_descriptions()

        for index, (channel_name, channel_datatype, channel_unit, channel_description) in enumerate(
                zip(channel_names, channel_datatypes, channel_units, channel_descriptions), start=0):

            new_channel = exd_api.StructureResult.Channel(
                name=str(
                    channel_name) if channel_name is not None else f"Ch_{index}",
                id=index,
                data_type=channel_datatype,
                unit_string=channel_unit,
            )
            if 0 == index and file.leading_independent():
                new_channel.attributes.variables["independent"].long_array.values.append(
                    1)
            if channel_description:
                new_channel.attributes.variables["description"].string_array.values.append(
                    channel_description)
            new_group.channels.append(new_channel)

        rv.groups.append(new_group)

        return rv

    def GetValues(self, request: exd_api.ValuesExRequest, context: grpc.ServicerContext):

        file = self.__get_file(request.handle)

        if request.group_id != 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f'Invalid group id {request.group_id}!')

        nr_of_rows = file.number_of_rows()
        if request.start >= nr_of_rows:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f'Channel start index {request.start} out of range!')

        end_index = request.start + request.limit
        if end_index >= nr_of_rows:
            end_index = nr_of_rows

        rv = exd_api.ValuesResult(id=request.group_id)
        for channel_index in request.channel_ids:
            if channel_index >= file.number_of_columns():
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              f'Invalid channel id {channel_index}!')

            column_data_type = file.column_datatype(channel_index)
            channel = file.column_data(channel_index)

            new_channel_values = exd_api.ValuesResult.ChannelValues(
                id=channel_index,
            )
            new_channel_values.values.data_type = column_data_type
            if ods.DataTypeEnum.DT_BYTE == column_data_type:
                new_channel_values.values.byte_array.values = channel[request.start:end_index].tobytes(
                )
            elif ods.DataTypeEnum.DT_SHORT == column_data_type:
                new_channel_values.values.long_array.values[:
                                                            ] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_LONG == column_data_type:
                new_channel_values.values.long_array.values[:
                                                            ] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_LONGLONG == column_data_type:
                new_channel_values.values.longlong_array.values[:
                                                                ] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_FLOAT == column_data_type:
                new_channel_values.values.float_array.values[:
                                                             ] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_DOUBLE == column_data_type:
                new_channel_values.values.double_array.values[:
                                                              ] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_DATE == column_data_type:
                datetime_values = channel[request.start:end_index]
                string_values = []
                for datetime_value in datetime_values:
                    string_values.append(
                        self.__to_asam_ods_time(datetime_value))
                new_channel_values.values.string_array.values[:] = string_values
            elif ods.DataTypeEnum.DT_STRING == column_data_type:
                new_channel_values.values.string_array.values[:
                                                              ] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_COMPLEX == column_data_type:
                complex_values = channel[request.start:end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.float_array.values[:] = real_values
            elif ods.DataTypeEnum.DT_DCOMPLEX == column_data_type:
                complex_values = channel[request.start:end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.double_array.values[:] = real_values
            else:
                context.abort(grpc.StatusCode.UNIMPLEMENTED,
                              f'Not implemented channel type {column_data_type}!')

            rv.channels.append(new_channel_values)

        return rv

    def GetValuesEx(self, request, context: grpc.ServicerContext):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Method not implemented!")

    def __to_asam_ods_time(self, datetime_value):
        return re.sub("[^0-9]", "", str(datetime_value))

    def __add_attributes(self, properties: dict, attributes):
        if properties is None:
            return

        if not isinstance(properties, dict):
            raise Exception(f'Attribute "{properties}" is not a dictionary!')

        for name, value in properties.items():
            if isinstance(value, str):
                attributes.variables[name].string_array.values.append(value)
            elif isinstance(value, float):
                attributes.variables[name].double_array.values.append(value)
            elif isinstance(value, int):
                attributes.variables[name].long_array.values.append(value)
            elif np.issubdtype('datetime64', value.dtype):
                attributes.variables[name].string_array.values.append(
                    self.__to_asam_ods_time(value))
            else:
                raise Exception(
                    f'Attribute "{name}": "{value}" not assignable')

    def __init__(self):
        self.connect_count: int = 0
        self.connection_map: dict[str, exd_api.Identifier] = {}
        self.file_map: dict = {}
        self.lock = threading.Lock()

    def __get_id(self, identifier: exd_api.Identifier) -> str:
        self.connect_count = self.connect_count + 1
        rv = str(self.connect_count)
        self.connection_map[rv] = identifier
        return rv

    def __uri_to_path(self, uri: str) -> str:
        parsed = urlparse(uri)
        host = "{0}{0}{mnt}{0}".format(os.path.sep, mnt=parsed.netloc)
        return os.path.normpath(
            os.path.join(host, url2pathname(unquote(parsed.path)))
        )

    def __get_path(self, file_url) -> str:
        final_path = self.__uri_to_path(file_url)
        return final_path

    def __open_file(self, identifier) -> str:
        with self.lock:
            connection_id = self.__get_id(identifier)
            connection_url = self.__get_path(identifier.url)
            if connection_url not in self.file_map:
                file_cache = FileCache(
                    connection_url, identifier.parameters)
                self.file_map[connection_url] = {
                    "file": file_cache, "ref_count": 0}
            self.file_map[connection_url]["ref_count"] = self.file_map[connection_url]["ref_count"] + 1
            return connection_id

    def __get_file(self, handle) -> FileCache:
        with self.lock:
            identifier = self.connection_map.get(handle.uuid)
            if identifier is None:
                raise ValueError(f'Handle "{handle.uuid}" not found!')
            connection_url = self.__get_path(identifier.url)
            return self.file_map.get(connection_url).get("file")

    def __close_file(self, handle):
        with self.lock:
            identifier = self.connection_map.get(handle.uuid)
            if identifier is None:
                raise ValueError(
                    f'Handle "{handle.uuid}" not found! Unable to close file!')
            connection_url = self.__get_path(identifier.url)
            if self.file_map.get(connection_url).get("ref_count") > 1:
                self.file_map[connection_url]["ref_count"] = self.file_map[connection_url]["ref_count"] - 1
            else:
                self.file_map.get(connection_url).get("file").close()
                del self.file_map[connection_url]
