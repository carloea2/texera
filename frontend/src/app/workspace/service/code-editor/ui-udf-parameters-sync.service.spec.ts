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
import { UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";
import { UiUdfParametersSyncService } from "./ui-udf-parameters-sync.service";
import { UiUdfParameter } from "../../types/workflow-compiling.interface";
import { vi } from "vitest";
import * as Y from "yjs";

describe("UiUdfParametersSyncService", () => {
  const operatorId = "operator-1";
  const code = "self.UiParameter(...)";

  let service: UiUdfParametersSyncService;
  let workflowActionServiceSpy: { getTexeraGraph: ReturnType<typeof vi.fn> };
  let parserServiceSpy: { parse: ReturnType<typeof vi.fn> };
  let graphSpy: {
    getOperator: ReturnType<typeof vi.fn>;
    getSharedOperatorType: ReturnType<typeof vi.fn>;
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

    graphSpy = {
      getOperator: vi.fn().mockImplementation((id: string) => (id === operatorId ? operator : undefined)),
      getSharedOperatorType: vi.fn(),
    };

    workflowActionServiceSpy = {
      getTexeraGraph: vi.fn().mockReturnValue(graphSpy),
    };

    parserServiceSpy = { parse: vi.fn() };

    service = new UiUdfParametersSyncService(
      workflowActionServiceSpy as unknown as WorkflowActionService,
      parserServiceSpy as unknown as UiUdfParametersParserService
    );
  });

  it("should emit parameters that preserve values from current parameter names", () => {
    operator.operatorProperties.uiParameters = [createParameter("count", "integer", "42")];
    parserServiceSpy.parse.mockReturnValue([createParameter("count", "integer"), createParameter("name", "string")]);

    const nextSpy = vi.fn();
    service.uiParametersChanged$.subscribe(nextSpy);

    service.syncStructureFromCode(operatorId, code);

    expect(nextSpy).toHaveBeenCalledWith({
      operatorId,
      parameters: [createParameter("count", "integer", "42"), createParameter("name", "string", "")],
    });
    expect(nextSpy).toHaveBeenCalledOnce();
  });

  it("should emit updated parameters with preserved values and removed stale parameters", () => {
    operator.operatorProperties.uiParameters = [
      createParameter("count", "integer", "42"),
      createParameter("removed", "string", "stale"),
    ];
    parserServiceSpy.parse.mockReturnValue([createParameter("count", "integer"), createParameter("name", "string")]);

    const nextSpy = vi.fn();
    service.uiParametersChanged$.subscribe(nextSpy);

    service.syncStructureFromCode(operatorId, code);

    expect(nextSpy).toHaveBeenCalledWith({
      operatorId,
      parameters: [createParameter("count", "integer", "42"), createParameter("name", "string", "")],
    });
    expect(nextSpy).toHaveBeenCalledOnce();
  });

  it("should not emit when the merged parameters are unchanged", () => {
    operator.operatorProperties.uiParameters = [createParameter("count", "integer", "42")];
    parserServiceSpy.parse.mockReturnValue([createParameter("count", "integer")]);

    const nextSpy = vi.fn();
    service.uiParametersChanged$.subscribe(nextSpy);

    service.syncStructureFromCode(operatorId, code);

    expect(nextSpy).not.toHaveBeenCalled();
  });

  it("should not parse code for non-Python UDF operators", () => {
    operator.operatorType = "Projection";

    const nextSpy = vi.fn();
    service.uiParametersChanged$.subscribe(nextSpy);

    service.syncStructureFromCode(operatorId, code);

    expect(parserServiceSpy.parse).not.toHaveBeenCalled();
    expect(nextSpy).not.toHaveBeenCalled();
  });

  it("should read code from the shared operator property when editor code is omitted", () => {
    const sharedCode = 'self.UiParameter("count", AttributeType.INT)';
    graphSpy.getSharedOperatorType.mockReturnValue(createSharedOperatorType(sharedCode));
    parserServiceSpy.parse.mockReturnValue([createParameter("count", "integer")]);

    const nextSpy = vi.fn();
    service.uiParametersChanged$.subscribe(nextSpy);

    service.syncStructureFromCode(operatorId);

    expect(parserServiceSpy.parse).toHaveBeenCalledWith(sharedCode);
    expect(nextSpy).toHaveBeenCalledWith({
      operatorId,
      parameters: [createParameter("count", "integer")],
    });
  });

  it("should debounce YText changes and clean up the observer", () => {
    vi.useFakeTimers();
    try {
      const yCode = createYText('self.UiParameter("count", AttributeType.INT)');
      parserServiceSpy.parse.mockReturnValue([createParameter("count", "integer")]);

      const nextSpy = vi.fn();
      service.uiParametersChanged$.subscribe(nextSpy);

      const cleanup = service.attachToYCode(operatorId, yCode);

      expect(parserServiceSpy.parse).toHaveBeenCalledOnce();
      expect(nextSpy).toHaveBeenCalledOnce();

      yCode.insert(yCode.length, "\n# changed");

      vi.advanceTimersByTime(199);

      expect(parserServiceSpy.parse).toHaveBeenCalledOnce();

      vi.advanceTimersByTime(1);

      expect(parserServiceSpy.parse).toHaveBeenCalledTimes(2);
      expect(nextSpy).toHaveBeenCalledTimes(2);

      cleanup();
      yCode.insert(yCode.length, "\n# after cleanup");
      vi.advanceTimersByTime(200);

      expect(parserServiceSpy.parse).toHaveBeenCalledTimes(2);
      expect(nextSpy).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
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

function createSharedOperatorType(code: string): Y.Map<unknown> {
  const doc = new Y.Doc();
  const sharedOperator = doc.getMap<unknown>("operator");
  const operatorProperties = new Y.Map<unknown>();
  const yCode = new Y.Text();

  operatorProperties.set("code", yCode);
  sharedOperator.set("operatorProperties", operatorProperties);
  yCode.insert(0, code);

  return sharedOperator;
}

function createYText(text: string): Y.Text {
  const doc = new Y.Doc();
  const yMap = doc.getMap<unknown>("root");
  const yText = new Y.Text();
  yMap.set("code", yText);
  yText.insert(0, text);
  return yText;
}
