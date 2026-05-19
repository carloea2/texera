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

import { UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";

describe("UiUdfParametersParserService", () => {
  let service: UiUdfParametersParserService;

  beforeEach(() => {
    service = new UiUdfParametersParserService();
  });

  it.each([
    [
      [
        'self.UiParameter("count", AttributeType.INT); self.UiParameter(type=AttributeType.STRING, name="name")',
        'self.UiParameter(name="age", type=AttributeType.LONG); self.UiParameter("score", AttributeType.DOUBLE)',
        'self.UiParameter("created_at", attr_type=AttributeType.TIMESTAMP)',
      ],
      ["count:integer", "name:string", "age:long", "score:double", "created_at:timestamp"],
    ],
    [
      [
        'self.UiParameter(); self.UiParameter(name="missing_type"); self.UiParameter(type=AttributeType.DOUBLE)',
        'self.UiParameter(AttributeType.INT, "wrong_order"); self.UiParameter("too_many", AttributeType.STRING, "extra")',
        'self.UiParameter(type=AttributeType.DOUBLE, key="legacy"); self.UiParameter("payload", AttributeType.BINARY)',
        'self.UiParameter("blob", AttributeType.LARGE_BINARY); self.UiParameter("valid", AttributeType.STRING)',
        'self.UiParameter("threshold", AttributeType.DOUBLE); self.UiParameter("threshold", AttributeType.STRING)',
      ],
      ["valid:string", "threshold:double"],
    ],
    [
      [
        '# self.UiParameter("commented", AttributeType.INT)',
        '# self.UiParameter(name="commented_multiline", type=AttributeType.INT)',
        '""" self.UiParameter("example", AttributeType.STRING) """',
        'self.UiParameter("active", AttributeType.DOUBLE)  # self.UiParameter("trailing", AttributeType.STRING)',
      ],
      ["active:double"],
    ],
  ])("should parse UDF parameters", (lines, expected) => {
    expect(parsed(udf(...lines))).toEqual(expected);
  });

  it("should support multiline declarations and exact supported class names only", () => {
    expect(
      parsed(udf('self.UiParameter(name=\n            "threshold",\n            type=AttributeType.DOUBLE)'))
    ).toEqual(["threshold:double"]);
    expect(
      parsed(
        'class MyWrappedTupleOp(ProcessTupleOperator):\n def open(self):\n  self.UiParameter("ignored", AttributeType.STRING)'
      )
    ).toEqual([]);
    expect(
      parsed(
        'class GenerateOperator(UDFSourceOperator):\n def open(self):\n  self.UiParameter("batch_size", AttributeType.INT)'
      )
    ).toEqual(["batch_size:integer"]);
  });

  function parsed(code: string): string[] {
    return service
      .parse(code)
      .map(parameter => `${parameter.attribute.attributeName}:${parameter.attribute.attributeType}`);
  }
});

function udf(...lines: string[]): string {
  return [
    "class ProcessTupleOperator(UDFOperatorV2):",
    "    def open(self):",
    ...lines.map(line => `        ${line}`),
  ].join("\n");
}
