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
        import pickle
        import re
        s = self.context.worker_id
        if re.search(r'-\d+$', s):
            s = s.rsplit('-', 1)[0]
        import os
        pkl_path = f'{s}.pkl'
        if os.path.exists(pkl_path):
            with open(pkl_path, 'rb') as f:
                self.context.executor_manager.executor = pickle.load(f)
                self.context.executor_manager.state_loaded = True
                logger.info(self.context.executor_manager.executor)
                logger.info(f"operator state reloaded from {pkl_path}")
        return EmptyReturn()
