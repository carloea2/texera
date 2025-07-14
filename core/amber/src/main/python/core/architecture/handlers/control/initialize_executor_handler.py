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

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.util import get_one_of
from loguru import logger
from proto.edu.uci.ics.amber.core import OpExecWithCode
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    InitializeExecutorRequest,
)


class InitializeExecutorHandler(ControlHandler):

    async def initialize_executor(self, req: InitializeExecutorRequest) -> EmptyReturn:
        logger.info(f"initializing - {self.context.worker_id}")
        op_exec_with_code: OpExecWithCode = get_one_of(req.op_exec_init_info)
        self.context.executor_manager.initialize_executor(
            op_exec_with_code.code, req.is_source, op_exec_with_code.language
        )

        if not hasattr(self.context.executor_manager.executor, 'process_tables'):
            import pickle, glob
            import re
            worker_id = self.context.worker_id
            m = re.search(r'-(\d+)$', worker_id)
            tail_mod = (int(m.group(1)) if m else 0) % 10000
            prefix   = worker_id[:m.start()] if m else worker_id

            # ② Find all matching pickle files:  prefix-*.pkl
            candidates = glob.glob(f'{prefix}-*.pkl')
            if not candidates:
                logger.warning(f"No state files found for prefix '{prefix}-*.pkl'; "
                               "executor starts fresh.")
                self.context.executor_manager.state_loaded = False
                return

            # ③ Extract numeric suffixes, sort them, choose one deterministically
            suffixes = []
            for path in candidates:
                m2 = re.search(rf'{re.escape(prefix)}-(\d+)\.pkl$', path)
                if m2:
                    suffixes.append(int(m2.group(1)))
            suffixes.sort()

            chosen_suffix = suffixes[tail_mod % len(suffixes)]
            pkl_path = f'{prefix}-{chosen_suffix}.pkl'

            # ④ Load executor
            try:
                with open(pkl_path, 'rb') as f:
                    self.context.executor_manager.executor = pickle.load(f)
                self.context.executor_manager.state_loaded = True
                logger.info(f"Operator state reloaded from {pkl_path}")
            except Exception as e:
                logger.error(f"Failed to load executor state from {pkl_path}: {e}. "
                             "The executor will be initialised from scratch.")
                self.context.executor_manager.state_loaded = False


        return EmptyReturn()
