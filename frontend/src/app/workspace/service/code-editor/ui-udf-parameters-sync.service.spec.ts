/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { PYTHON_UDF_V2_OP_TYPE } from "../workflow-graph/model/workflow-graph";
import { UiUdfParametersParseError, UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";
import type { UiUdfParameter } from "./ui-udf-parameters-parser.service";
import { UiUdfParametersSyncService } from "./ui-udf-parameters-sync.service";
import type { Mock } from "vitest";
import { vi as vitest } from "vitest";
import * as Yjs from "yjs";

type MockFunction = Mock;

function createMockFunction(): MockFunction {
  return vitest.fn();
}

describe("UiUdfParametersSyncService", () => {
  const operatorId = "operator-1";
  const code = "self.UiParameter(...)";

  let service: UiUdfParametersSyncService;
  let workflowActionServiceMock: { getTexeraGraph: MockFunction };
  let parserServiceMock: { parse: MockFunction };
  let graphMock: {
    getOperator: MockFunction;
    getSharedOperatorType: MockFunction;
  };
  let operator: {
    operatorType: string;
    operatorProperties: { uiParameters: UiUdfParameter[] };
  };

  beforeEach(() => {
    operator = {
      operatorType: PYTHON_UDF_V2_OP_TYPE,
      operatorProperties: { uiParameters: [] },
    };

    graphMock = {
      getOperator: createMockFunction().mockImplementation((requestedOperatorId: string) =>
        requestedOperatorId === operatorId ? operator : undefined
      ),
      getSharedOperatorType: createMockFunction(),
    };

    workflowActionServiceMock = {
      getTexeraGraph: createMockFunction().mockReturnValue(graphMock),
    };

    parserServiceMock = { parse: createMockFunction() };

    service = new UiUdfParametersSyncService(
      workflowActionServiceMock as unknown as WorkflowActionService,
      parserServiceMock as unknown as UiUdfParametersParserService
    );
  });

  it("should emit parameters that preserve values from current parameter names", () => {
    operator.operatorProperties.uiParameters = [createParameter("count", "integer", "42")];
    parserServiceMock.parse.mockReturnValue([createParameter("count", "integer"), createParameter("name", "string")]);

    const parametersChangedObserver = createMockFunction();
    service.uiParametersChanged$.subscribe(parametersChangedObserver);

    service.syncStructureFromCode(operatorId, code);

    expect(parametersChangedObserver).toHaveBeenCalledWith({
      operatorId,
      parameters: [createParameter("count", "integer", "42"), createParameter("name", "string", "")],
    });
    expect(parametersChangedObserver).toHaveBeenCalledOnce();
  });

  it("should emit updated parameters with preserved values and removed stale parameters", () => {
    operator.operatorProperties.uiParameters = [
      createParameter("count", "integer", "42"),
      createParameter("removed", "string", "stale"),
    ];
    parserServiceMock.parse.mockReturnValue([createParameter("count", "integer"), createParameter("name", "string")]);

    const parametersChangedObserver = createMockFunction();
    service.uiParametersChanged$.subscribe(parametersChangedObserver);

    service.syncStructureFromCode(operatorId, code);

    expect(parametersChangedObserver).toHaveBeenCalledWith({
      operatorId,
      parameters: [createParameter("count", "integer", "42"), createParameter("name", "string", "")],
    });
    expect(parametersChangedObserver).toHaveBeenCalledOnce();
  });

  it("should not emit when the merged parameters are unchanged", () => {
    operator.operatorProperties.uiParameters = [createParameter("count", "integer", "42")];
    parserServiceMock.parse.mockReturnValue([createParameter("count", "integer")]);

    const parametersChangedObserver = createMockFunction();
    service.uiParametersChanged$.subscribe(parametersChangedObserver);

    service.syncStructureFromCode(operatorId, code);

    expect(parametersChangedObserver).not.toHaveBeenCalled();
  });

  it("should emit parser errors without replacing the current parameters", () => {
    operator.operatorProperties.uiParameters = [createParameter("count", "integer", "42")];
    parserServiceMock.parse.mockImplementation(() => {
      throw new UiUdfParametersParseError("Only one Python UDF class can declare UiParameter values.");
    });

    const parametersChangedObserver = createMockFunction();
    const parseErrorObserver = createMockFunction();
    service.uiParametersChanged$.subscribe(parametersChangedObserver);
    service.uiParametersParseError$.subscribe(parseErrorObserver);

    service.syncStructureFromCode(operatorId, code);

    expect(parametersChangedObserver).not.toHaveBeenCalled();
    expect(parseErrorObserver).toHaveBeenCalledWith({
      operatorId,
      message: "Only one Python UDF class can declare UiParameter values.",
    });
  });

  it("should not parse code for non-Python UDF operators", () => {
    operator.operatorType = "Projection";

    const parametersChangedObserver = createMockFunction();
    service.uiParametersChanged$.subscribe(parametersChangedObserver);

    service.syncStructureFromCode(operatorId, code);

    expect(parserServiceMock.parse).not.toHaveBeenCalled();
    expect(parametersChangedObserver).not.toHaveBeenCalled();
  });

  it("should read code from the shared operator property when editor code is omitted", () => {
    const sharedCode = 'self.UiParameter("count", AttributeType.INT)';
    graphMock.getSharedOperatorType.mockReturnValue(createSharedOperatorType(sharedCode));
    parserServiceMock.parse.mockReturnValue([createParameter("count", "integer")]);

    const parametersChangedObserver = createMockFunction();
    service.uiParametersChanged$.subscribe(parametersChangedObserver);

    service.syncStructureFromCode(operatorId);

    expect(parserServiceMock.parse).toHaveBeenCalledWith(sharedCode);
    expect(parametersChangedObserver).toHaveBeenCalledWith({
      operatorId,
      parameters: [createParameter("count", "integer")],
    });
  });

  it("should debounce YText changes and clean up the observer", () => {
    vitest.useFakeTimers();
    try {
      const sharedCodeText = createSharedText('self.UiParameter("count", AttributeType.INT)');
      parserServiceMock.parse.mockReturnValue([createParameter("count", "integer")]);

      const parametersChangedObserver = createMockFunction();
      service.uiParametersChanged$.subscribe(parametersChangedObserver);

      const cleanup = service.attachToYCode(operatorId, sharedCodeText);

      expect(parserServiceMock.parse).toHaveBeenCalledOnce();
      expect(parametersChangedObserver).toHaveBeenCalledOnce();

      sharedCodeText.insert(sharedCodeText.length, "\n# changed");

      vitest.advanceTimersByTime(199);

      expect(parserServiceMock.parse).toHaveBeenCalledOnce();

      vitest.advanceTimersByTime(1);

      expect(parserServiceMock.parse).toHaveBeenCalledTimes(2);
      expect(parametersChangedObserver).toHaveBeenCalledTimes(2);

      cleanup();
      sharedCodeText.insert(sharedCodeText.length, "\n# after cleanup");
      vitest.advanceTimersByTime(200);

      expect(parserServiceMock.parse).toHaveBeenCalledTimes(2);
      expect(parametersChangedObserver).toHaveBeenCalledTimes(2);
    } finally {
      vitest.useRealTimers();
    }
  });
});

function createParameter(name: string, type: UiUdfParameter["attribute"]["attributeType"], value = ""): UiUdfParameter {
  return {
    attribute: {
      attributeName: name,
      attributeType: type,
    },
    value,
  };
}

function createSharedOperatorType(code: string): Yjs.Map<unknown> {
  const yjsDocument = new Yjs.Doc();
  const sharedOperator = yjsDocument.getMap<unknown>("operator");
  const operatorProperties = new Yjs.Map<unknown>();
  const sharedCodeText = new Yjs.Text();

  operatorProperties.set("code", sharedCodeText);
  sharedOperator.set("operatorProperties", operatorProperties);
  sharedCodeText.insert(0, code);

  return sharedOperator;
}

function createSharedText(text: string): Yjs.Text {
  const yjsDocument = new Yjs.Doc();
  const sharedRootMap = yjsDocument.getMap<unknown>("root");
  const sharedText = new Yjs.Text();
  sharedRootMap.set("code", sharedText);
  sharedText.insert(0, text);
  return sharedText;
}
