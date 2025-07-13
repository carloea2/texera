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

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Iterator, List, Mapping, Optional, Union, MutableMapping

import overrides
import pandas


from . import Table, TableLike, Tuple, TupleLike, Batch, BatchLike
from .marker import State
from .table import all_output_to_tuple


class Operator(ABC):
    """
    Abstract base class for all operators.
    """

    __internal_is_source: bool = False

    @property
    @overrides.final
    def is_source(self) -> bool:
        """
        Whether the operator is a source operator. Source operators generate output
        Tuples without having input Tuples.

        :return:
        """
        return self.__internal_is_source

    @is_source.setter
    @overrides.final
    def is_source(self, value: bool) -> None:
        self.__internal_is_source = value


    def process_state(self, state: State, port: int) -> Optional[State]:
        """
        Process an input State from the given link.
        The default implementation is to pass the State to all downstream operators
        if the State has pass_to_all_downstream set to True.
        :param state: State, a State from an input port to be processed.
        :param port: int, input port index of the current exhausted port.
        :return: State, producing one State object
        """
        if state.passToAllDownstream:
            return state

    def produce_state_on_start(self, port: int) -> State:
        """
        Produce a State when the given link started.

        :param port: int, input port index of the current initialized port.
        :return: State, producing one State object
        """
        pass

    def produce_state_on_finish(self, port: int) -> State:
        """
        Produce a State after the input port is exhausted.

        :param port: int, input port index of the current exhausted port.
        :return: State, producing one State object
        """
        pass

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass


class TupleOperatorV2(Operator):
    """
    Base class for tuple-oriented operators. A concrete implementation must
    be provided upon using.
    """

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Tuple, a Tuple from an input port to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
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

    def on_finish_all(self):
        yield


class SourceOperator(TupleOperatorV2):
    __internal_is_source = True


    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        """
        Produce Tuples or Tables. Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        yield

    @overrides.final
    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
        # TODO: change on_finish to output Iterator[Union[TupleLike, TableLike, None]]
        for i in self.produce():
            yield from all_output_to_tuple(i)

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        yield


class BatchOperator(TupleOperatorV2):
    """
    Base class for batch-oriented operators. A concrete implementation must
    be provided upon using.
    """

    BATCH_SIZE: int = 10  # must be a positive integer

    def __init__(self):
        super().__init__()
        self.__batch_data: MutableMapping[int, List[Tuple]] = defaultdict(list)
        self._validate_batch_size(self.BATCH_SIZE)

    @staticmethod
    @overrides.final
    def _validate_batch_size(value):
        if value is None:
            raise ValueError("BATCH_SIZE cannot be None.")
        if type(value) is not int:
            raise ValueError("BATCH_SIZE cannot be {type(value))}.")
        if value <= 0:
            raise ValueError("BATCH_SIZE should be positive.")

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        self.__batch_data[port].append(tuple_)
        if (
            self.BATCH_SIZE is not None
            and len(self.__batch_data[port]) >= self.BATCH_SIZE
        ):
            yield from self._process_batch(port)

    @overrides.final
    def _process_batch(self, port: int) -> Iterator[Optional[BatchLike]]:
        batch = Batch(
            pandas.DataFrame(
                [
                    self.__batch_data[port].pop(0).as_series()
                    for _ in range(min(len(self.__batch_data[port]), self.BATCH_SIZE))
                ]
            )
        )
        for output_batch in self.process_batch(batch, port):
            if output_batch is not None:
                if isinstance(output_batch, pandas.DataFrame):
                    # TODO: integrate into Batch as a helper function.
                    # convert from Batch to Tuple, only supports pandas.DataFrames for
                    # now.
                    for _, output_tuple in output_batch.iterrows():
                        yield output_tuple
                else:
                    yield output_batch

    @overrides.final
    def on_finish(self, port: int) -> Iterator[Optional[BatchLike]]:
        while len(self.__batch_data[port]) != 0:
            yield from self._process_batch(port)

    def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
        """
        Process an input Batch from the given link. The Batch is represented as a
        pandas.DataFrame.

        :param batch: Batch, a batch to be processed.
        :param port: int, input port index of the current Batch.
        :return: Iterator[Optional[BatchLike]], producing one BatchLike object at a
            time, or None.
        """
        yield


class TableOperator(TupleOperatorV2):
    """
    Base class for table-oriented operators. A concrete implementation must
    be provided upon using.
    """

    def __init__(self):
        super().__init__()
        self.__internal_is_source: bool = False
        self.__table_data: Mapping[int, List[Tuple]] = defaultdict(list)

    @overrides.final
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        self.__table_data[port].append(tuple_)
        yield

    def on_finish(self, port: int) -> Iterator[Optional[TableLike]]:
        table = Table(self.__table_data[port])
        method_name = 'process_table_' + str(port)
        process_method = getattr(self, method_name, None)
        yield from process_method(table)

    def on_finish_all(self):
        sorted_values = [self.__table_data[k] for k in sorted(self.__table_data)]
        yield from self.process_tables(*sorted_values)

    def process_tables(self, *args):
        yield from iter([])



class GeneralOperator(Operator):
    """
    A general operator that can handle both tuples and states.
    It is not recommended to use this operator directly, but rather
    extend it to create a specific operator.
    """
    def __init__(self):
        super().__init__()
        self.__internal_is_source: bool = True
        self.TABLE_DATA_INTERNAL: Mapping[int, List[Tuple]] = defaultdict(list)

    def collect(self, tuple_: Tuple, port: int) -> None:
        self.TABLE_DATA_INTERNAL[port].append(tuple_)
        yield

    def process(self):
        yield

    def on_finish_all(self):
        # if self has the "process_tables" method, call it
        if hasattr(self, "process_tables"):
            sorted_values = [Table(self.TABLE_DATA_INTERNAL[k]) for k in sorted(
                self.TABLE_DATA_INTERNAL)]
            yield from self.process_tables(*sorted_values)
        else:
            # otherwise, yield an empty iterator
            yield from iter([])



