#!/usr/bin/env python3
"""EXD API implementation for CSV files"""

from __future__ import annotations

import threading
from typing import override

import numpy as np
import pandas as pd
from ods_exd_api_box import ExdFileInterface, NotMyFileError, exd_api, ods
from ods_exd_api_box.utils import ParamParser
from ods_exd_api_box.utils.attribute_helper import AttributeHelper
from ods_exd_api_box.utils.time_helper import TimeHelper

from exd_api_simple import ExdApiSimple

# pylint: disable=no-member


class FileCache:
    def __init__(self, file_path: str, parameters: dict):
        self._lock = threading.Lock()
        self._file_path = file_path
        self._parameters: dict = parameters
        self._edp: ExdApiSimple | None = None
        self._datatypes: list[ods.DataTypeEnum] | None = None

    def close(self):
        with self._lock:
            if self._edp is not None:
                self._edp.close()
                self._edp = None
                self._datatypes = None

    def not_my_file(self) -> bool:
        return self._external_data_pandas().not_my_file()

    def column_datatype(self, index: int) -> ods.DataTypeEnum:
        if self._datatypes is None:
            self._datatypes = self.column_datatypes()

        if index >= len(self._datatypes):
            raise IndexError(f"Column index {index} out of range!")
        return self._datatypes[index]

    def column_data(self, index: int) -> pd.Series:
        data = self.__data()
        if index >= data.shape[1]:
            raise IndexError(f"Column index {index} out of range!")
        return data.iloc[:, index]

    def file_attributes(self) -> dict:
        return self._external_data_pandas().file_attributes()

    def group_attributes(self) -> dict:
        return self._external_data_pandas().group_attributes()

    def column_names(self) -> list[str]:
        result: list[str] | None = self._external_data_pandas().column_names()
        return result if result is not None else self.__data().columns.tolist()

    def column_units(self) -> list[str]:
        return self._external_data_pandas().column_units()

    def column_descriptions(self) -> list[str]:
        return self._external_data_pandas().column_descriptions()

    def column_datatypes(self) -> list[ods.DataTypeEnum]:
        return [self._get_datatype(col_type) for col_type in self.__data().dtypes]

    def number_of_rows(self):
        return int(self.__data().shape[0])

    def number_of_columns(self):
        return int(self.__data().shape[1])

    def leading_independent(self) -> bool:
        column: pd.Series = self.column_data(0)
        return column.is_monotonic_increasing and column.is_unique

    def __data(self) -> pd.DataFrame:
        return self._external_data_pandas().data()

    def _external_data_pandas(self) -> ExdApiSimple:
        with self._lock:
            if self._edp is None:
                self._edp = ExdApiSimple.create(self._file_path, self._parameters)
            return self._edp

    def _get_datatype(self, data_type: np.dtype) -> ods.DataTypeEnum:
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
        raise NotImplementedError(f"Unknown type {data_type}!")


class ExdApiSimpleImpl(ExdFileInterface):
    """Class for handling file content."""

    @classmethod
    @override
    def create(cls, file_path: str, parameters: str) -> ExdFileInterface:
        """Factory method to create a file handler instance."""
        return cls(file_path, parameters)

    def __init__(self, file_path: str, parameters: str = ""):

        self.file: FileCache | None = FileCache(file_path, ParamParser.parse_params(parameters))

    @override
    def close(self) -> None:
        if self.file is not None:
            self.file.close()
            del self.file
            self.file = None

    @override
    def fill_structure(self, structure: exd_api.StructureResult) -> None:

        file = self.file

        if file.not_my_file():
            raise NotMyFileError

        AttributeHelper.add(structure.attributes, file.file_attributes())

        number_of_columns = file.number_of_columns()
        channel_names = file.column_names()
        channel_datatypes = file.column_datatypes()
        channel_units = file.column_units()
        channel_descriptions = file.column_descriptions()

        # Ensure all arrays have exactly number_of_columns entries
        channel_units = (channel_units + [""] * number_of_columns)[:number_of_columns]
        channel_descriptions = (channel_descriptions + [""] * number_of_columns)[:number_of_columns]

        new_group = exd_api.StructureResult.Group(
            name="data", id=0, total_number_of_channels=number_of_columns, number_of_rows=file.number_of_rows()
        )

        AttributeHelper.add(new_group.attributes, file.group_attributes())

        for index, (channel_name, channel_datatype, channel_unit, channel_description) in enumerate(
            zip(channel_names, channel_datatypes, channel_units, channel_descriptions), start=0
        ):

            new_channel = exd_api.StructureResult.Channel(
                name=str(channel_name) if channel_name is not None else f"Ch_{index}",
                id=index,
                data_type=channel_datatype,
                unit_string=channel_unit,
            )
            if 0 == index and file.leading_independent():
                AttributeHelper.add(new_channel.attributes, {"independent": 1})
            if channel_description:
                AttributeHelper.add(new_channel.attributes, {"description": channel_description})
            new_group.channels.append(new_channel)

        structure.groups.append(new_group)

    @override
    def get_values(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:

        file = self.file

        if request.group_id != 0:
            raise ValueError(f"Invalid group id {request.group_id}!")

        nr_of_rows = file.number_of_rows()
        if request.start >= nr_of_rows:
            raise ValueError(f"Channel start index {request.start} out of range!")

        end_index = request.start + request.limit
        if end_index >= nr_of_rows:
            end_index = nr_of_rows

        rv = exd_api.ValuesResult(id=request.group_id)
        for channel_index in request.channel_ids:
            if channel_index >= file.number_of_columns():
                raise ValueError(f"Invalid channel id {channel_index}!")

            column_data_type = file.column_datatype(channel_index)
            channel = file.column_data(channel_index)

            new_channel_values = exd_api.ValuesResult.ChannelValues(
                id=channel_index,
            )
            new_channel_values.values.data_type = column_data_type
            if ods.DataTypeEnum.DT_BYTE == column_data_type:
                new_channel_values.values.byte_array.values = channel[request.start : end_index].tobytes()
            elif ods.DataTypeEnum.DT_SHORT == column_data_type:
                new_channel_values.values.long_array.values[:] = channel[request.start : end_index]
            elif ods.DataTypeEnum.DT_LONG == column_data_type:
                new_channel_values.values.long_array.values[:] = channel[request.start : end_index]
            elif ods.DataTypeEnum.DT_LONGLONG == column_data_type:
                new_channel_values.values.longlong_array.values[:] = channel[request.start : end_index]
            elif ods.DataTypeEnum.DT_FLOAT == column_data_type:
                new_channel_values.values.float_array.values[:] = channel[request.start : end_index]
            elif ods.DataTypeEnum.DT_DOUBLE == column_data_type:
                new_channel_values.values.double_array.values[:] = channel[request.start : end_index]
            elif ods.DataTypeEnum.DT_DATE == column_data_type:
                datetime_values = channel[request.start : end_index]
                string_values = []
                for datetime_value in datetime_values:
                    string_values.append(TimeHelper.to_asam_ods_time(datetime_value))
                new_channel_values.values.string_array.values[:] = string_values
            elif ods.DataTypeEnum.DT_STRING == column_data_type:
                new_channel_values.values.string_array.values[:] = channel[request.start : end_index]
            elif ods.DataTypeEnum.DT_COMPLEX == column_data_type:
                complex_values = channel[request.start : end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.float_array.values[:] = real_values
            elif ods.DataTypeEnum.DT_DCOMPLEX == column_data_type:
                complex_values = channel[request.start : end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.double_array.values[:] = real_values
            else:
                raise NotImplementedError(f"Not implemented channel type {column_data_type}!")

            rv.channels.append(new_channel_values)

        return rv
