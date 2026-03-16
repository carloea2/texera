# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
from typing import Iterator, Optional

import pytest

from core.models.type.large_binary import largebinary
from pytexera import AttributeType, Tuple, TupleLike, UDFOperatorV2
from pytexera.udf.udf_operator import _UiParameterSupport


class InjectedParametersOperator(UDFOperatorV2):
    def _texera_injected_ui_parameters(self):
        return {
            "count": "7",
            "enabled": "yes",
            "created_at": "2024-01-01T00:00:00",
        }

    def open(self):
        self.count_parameter = self.UiParameter("count", AttributeType.INT)
        self.enabled_parameter = self.UiParameter(
            name="enabled", type=AttributeType.BOOL
        )
        self.created_at_parameter = self.UiParameter(
            "created_at", type=AttributeType.TIMESTAMP
        )

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_


class ConflictingParameterOperator(UDFOperatorV2):
    def _texera_injected_ui_parameters(self):
        return {"duplicate": "1"}

    def open(self):
        self.UiParameter("duplicate", AttributeType.INT)
        self.UiParameter("duplicate", AttributeType.STRING)

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_


class FirstIndependentParameterOperator(UDFOperatorV2):
    def _texera_injected_ui_parameters(self):
        return {"count": "1"}

    def open(self):
        self.count_parameter = self.UiParameter("count", AttributeType.INT)

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_


class SecondIndependentParameterOperator(UDFOperatorV2):
    def _texera_injected_ui_parameters(self):
        return {"count": "2"}

    def open(self):
        self.count_parameter = self.UiParameter("count", AttributeType.INT)

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield tuple_


class TestUiParameterSupport:
    def test_injected_values_are_applied_before_open(self):
        operator = InjectedParametersOperator()

        operator.open()

        assert operator.count_parameter.value == 7
        assert operator.enabled_parameter.value is True
        assert operator.created_at_parameter.value == datetime.datetime(
            2024, 1, 1, 0, 0
        )

    def test_duplicate_parameter_names_with_conflicting_types_raise(self):
        operator = ConflictingParameterOperator()

        with pytest.raises(ValueError) as exc_info:
            operator.open()

        assert "Duplicate UiParameter name 'duplicate'" in str(exc_info.value)

    @pytest.mark.parametrize(
        ("raw_value", "attr_type", "expected"),
        [
            ("hello", AttributeType.STRING, "hello"),
            ("7", AttributeType.INT, 7),
            ("99", AttributeType.LONG, 99),
            ("3.14", AttributeType.DOUBLE, 3.14),
            ("yes", AttributeType.BOOL, True),
            ("payload", AttributeType.BINARY, b"payload"),
            (
                "2024-01-01T00:00:00",
                AttributeType.TIMESTAMP,
                datetime.datetime(2024, 1, 1, 0, 0),
            ),
            (
                "s3://bucket/path/to/object",
                AttributeType.LARGE_BINARY,
                largebinary("s3://bucket/path/to/object"),
            ),
        ],
    )
    def test_parse_supported_types(self, raw_value, attr_type, expected):
        assert _UiParameterSupport._parse(raw_value, attr_type) == expected

    def test_parse_unsupported_type_raises_helpful_error(self):
        with pytest.raises(TypeError, match="UiParameter.type .* is not supported"):
            _UiParameterSupport._parse("value", object())

    def test_wrapped_open_uses_instance_local_state(self):
        assert (
            getattr(
                FirstIndependentParameterOperator.open,
                "__texera_ui_params_wrapped__",
                False,
            )
            is True
        )

        first_operator = FirstIndependentParameterOperator()
        second_operator = SecondIndependentParameterOperator()

        first_operator.open()
        second_operator.open()

        assert first_operator.count_parameter.value == 1
        assert second_operator.count_parameter.value == 2
        assert first_operator._ui_parameter_injected_values == {"count": "1"}
        assert second_operator._ui_parameter_injected_values == {"count": "2"}
        assert (
            first_operator._ui_parameter_injected_values
            is not second_operator._ui_parameter_injected_values
        )
