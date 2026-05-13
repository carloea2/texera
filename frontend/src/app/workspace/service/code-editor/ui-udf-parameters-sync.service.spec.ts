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
import { UiUdfParameter, UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";
import { UiUdfParametersSyncService } from "./ui-udf-parameters-sync.service";
import { vi } from "vitest";

describe("UiUdfParametersSyncService", () => {
  const operatorId = "operator-1";
  const code = "self.UiParameter(...)";

  let service: UiUdfParametersSyncService;
  let workflowActionServiceSpy: { getTexeraGraph: ReturnType<typeof vi.fn> };
  let parserServiceSpy: { parse: ReturnType<typeof vi.fn> };
  let operator: {
    operatorType: string;
    operatorProperties: { uiParameters: any[] };
  };

  beforeEach(() => {
    operator = {
      operatorType: PYTHON_UDF_V2_OP_TYPE,
      operatorProperties: { uiParameters: [] },
    };

    workflowActionServiceSpy = {
      getTexeraGraph: vi.fn().mockReturnValue({
        getOperator: vi.fn().mockImplementation((id: string) => (id === operatorId ? operator : undefined)),
      }),
    };

    parserServiceSpy = { parse: vi.fn() };

    service = new UiUdfParametersSyncService(
      workflowActionServiceSpy as unknown as WorkflowActionService,
      parserServiceSpy as unknown as UiUdfParametersParserService
    );
  });

  it("should emit parameters that preserve values from current and legacy parameter names", () => {
    operator.operatorProperties.uiParameters = [
      createParameter("count", "integer", "42"),
      { attribute: { name: "legacyName", attributeType: "string" }, value: "saved" },
    ];
    parserServiceSpy.parse.mockReturnValue([
      createParameter("count", "integer"),
      createParameter("legacyName", "string"),
    ]);

    const nextSpy = vi.fn();
    service.uiParametersChanged$.subscribe(nextSpy);

    service.syncStructureFromCode(operatorId, code);

    expect(nextSpy).toHaveBeenCalledWith({
      operatorId,
      parameters: [createParameter("count", "integer", "42"), createParameter("legacyName", "string", "saved")],
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
