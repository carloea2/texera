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

import * as Y from "yjs";
import { vi } from "vitest";
import { PYTHON_UDF_V2_OP_TYPE } from "../workflow-graph/model/workflow-graph";
import { UiUdfParametersSyncService } from "./ui-udf-parameters-sync.service";

describe("UiUdfParametersSyncService", () => {
  const operatorId = "operator-1";
  let service: UiUdfParametersSyncService;
  let parser: any;
  let graph: any;
  let operator: any;

  beforeEach(() => {
    operator = { operatorType: PYTHON_UDF_V2_OP_TYPE, operatorProperties: { uiParameters: [] } };
    graph = {
      getOperator: vi.fn((id: string) => (id === operatorId ? operator : undefined)),
      getSharedOperatorType: vi.fn(),
    };
    parser = { parse: vi.fn() };
    service = new UiUdfParametersSyncService({ getTexeraGraph: () => graph } as any, parser);
  });

  it("should merge parsed parameters with saved values and drop stale ones", () => {
    operator.operatorProperties.uiParameters = [param("count", "integer", "42"), param("removed", "string", "stale")];
    parser.parse.mockReturnValue([param("count", "integer"), param("name", "string")]);

    const next = subscribe();
    service.syncStructureFromCode(operatorId, "code");

    expect(next).toHaveBeenCalledWith({
      operatorId,
      parameters: [param("count", "integer", "42"), param("name", "string")],
    });
  });

  it("should skip non-Python UDF operators", () => {
    operator.operatorType = "Projection";
    service.syncStructureFromCode(operatorId, "code");
    expect(parser.parse).not.toHaveBeenCalled();
  });

  it("should read shared code when editor code is omitted", () => {
    const sharedCode = 'self.UiParameter("count", AttributeType.INT)';
    graph.getSharedOperatorType.mockReturnValue(sharedOperator(sharedCode));
    parser.parse.mockReturnValue([param("count", "integer")]);

    service.syncStructureFromCode(operatorId);

    expect(parser.parse).toHaveBeenCalledWith(sharedCode);
  });

  it("should debounce YText changes and clean up the observer", () => {
    vi.useFakeTimers();
    const yCode = yText('self.UiParameter("count", AttributeType.INT)');
    parser.parse.mockReturnValue([param("count", "integer")]);
    const next = subscribe();
    const cleanup = service.attachToYCode(operatorId, yCode);

    yCode.insert(yCode.length, "\n# changed");
    vi.advanceTimersByTime(199);
    expect(parser.parse).toHaveBeenCalledOnce();

    vi.advanceTimersByTime(1);
    expect(parser.parse).toHaveBeenCalledTimes(2);
    expect(next).toHaveBeenCalledTimes(2);

    cleanup();
    yCode.insert(yCode.length, "\n# after cleanup");
    vi.advanceTimersByTime(200);
    expect(parser.parse).toHaveBeenCalledTimes(2);
    vi.useRealTimers();
  });

  function subscribe() {
    const nextSpy = vi.fn();
    service.uiParametersChanged$.subscribe(nextSpy);
    return nextSpy;
  }
});

function param(name: string, type: string, value = "") {
  return { attribute: { attributeName: name, attributeType: type }, value };
}

function sharedOperator(code: string): Y.Map<unknown> {
  const doc = new Y.Doc();
  const operator = doc.getMap<unknown>("operator");
  const operatorProperties = new Y.Map<unknown>();
  const codeText = new Y.Text();
  operatorProperties.set("code", codeText);
  operator.set("operatorProperties", operatorProperties);
  codeText.insert(0, code);
  return operator;
}

function yText(text: string): Y.Text {
  const doc = new Y.Doc();
  const map = doc.getMap<unknown>("root");
  const codeText = new Y.Text();
  map.set("code", codeText);
  codeText.insert(0, text);
  return codeText;
}
