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
from abc import abstractmethod
from typing import Any, Dict, Iterator, Optional, Union

import functools
import datetime
from abc import abstractmethod
from typing import Any, Dict, Iterator, Optional, Union

from pyamber import *
from core.models.schema.attribute_type import AttributeType, TO_PYOBJECT_MAPPING

class _UiParameterSupport:
    _ui_parameter_injected_values: Dict[str, Any] = {}
    _ui_parameter_name_types: Dict[str, AttributeType] = {}

    # Reserved hook name. Backend injector will generate this in the user's class.
    def _texera_injected_ui_parameters(self) -> Dict[str, Any]:
        return {}

    def _texera_apply_injected_ui_parameters(self) -> None:
        values = self._texera_injected_ui_parameters()
        # Write to base class storage (not cls) because UiParameter reads from _UiParameterSupport directly
        _UiParameterSupport._ui_parameter_injected_values = dict(values or {})
        _UiParameterSupport._ui_parameter_name_types = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Wrap only methods defined on this class (not inherited ones)
        original_open = getattr(cls, "open", None)
        if original_open is None:
            return

        # Avoid double wrapping
        if getattr(original_open, "__texera_ui_params_wrapped__", False):
            return

        @functools.wraps(original_open)
        def wrapped_open(self, *args, **kwargs):
            self._texera_apply_injected_ui_parameters()
            return original_open(self, *args, **kwargs)

        setattr(wrapped_open, "__texera_ui_params_wrapped__", True)
        cls.open = wrapped_open

    class UiParameter:
        def __init__(self, name: str, type: AttributeType):
            if not isinstance(type, AttributeType):
                raise TypeError(
                    f"UiParameter.type must be an AttributeType, got {type!r}."
                )

            existing_type = _UiParameterSupport._ui_parameter_name_types.get(name)
            if existing_type is not None and existing_type != type:
                raise ValueError(
                    f"Duplicate UiParameter name '{name}' with conflicting types: "
                    f"{existing_type.name} vs {type.name}."
                )

            _UiParameterSupport._ui_parameter_name_types[name] = type
            raw_value = _UiParameterSupport._ui_parameter_injected_values.get(name)
            self.name = name
            self.type = type
            self.value = _UiParameterSupport._parse(raw_value, type)

    @classmethod
    def set_injected_ui_parameters(cls, values: Dict[str, Any]) -> None:
        # keep for backward compatibility if anything else calls it
        _UiParameterSupport._ui_parameter_injected_values = dict(values or {})
        _UiParameterSupport._ui_parameter_name_types = {}

    @staticmethod
    def _parse(value: Any, attr_type: AttributeType) -> Any:
        if value is None:
            return None

        py_type = TO_PYOBJECT_MAPPING.get(attr_type)
        return py_type(value)

class UDFOperatorV2(_UiParameterSupport, TupleOperatorV2):
    """
    Base class for tuple-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Tuple, a Tuple from an input port to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.

        See .examples/ for example operators.
        """
        yield

    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Callback when one input port is exhausted.

        :param port: int, input port index of the current exhausted port.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFSourceOperator(_UiParameterSupport, SourceOperator):
    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def produce(self) -> Iterator[Optional[Union[TupleLike, TableLike]]]:
        """
        Produce Tuples or Tables. Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFTableOperator(_UiParameterSupport, TableOperator):
    """
    Base class for table-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        """
        Process an input Table from the given link. The Table is represented as
        pandas.DataFrame.

        :param table: Table, a table to be processed.
        :param port: int, input index of the current Table.
        :return: Iterator[Optional[TableLike]], producing one TableLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass


class UDFBatchOperator(_UiParameterSupport, BatchOperator):
    """
    Base class for batch-oriented user-defined operators. A concrete implementation must
    be provided upon using.
    """

    def open(self) -> None:
        """
        Open a context of the operator. Usually can be used for loading/initiating some
        resources, such as a file, a model, or an API client.
        """
        pass

    @abstractmethod
    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        """
        Process an input Batch from the given link. The Batch is represented as
        pandas.DataFrame.

        :param batch: Batch, a batch to be processed.
        :param port: int, input index of the current Batch.
        :return: Iterator[Optional[BatchLike]], producing one BatchLike object at a
            time, or None.
        """
        yield

    def close(self) -> None:
        """
        Close the context of the operator.
        """
        pass
