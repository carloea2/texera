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

  it("should parse positional and name-based arguments", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter(AttributeType.INT, "count")
              self.UiParameter(type=AttributeType.STRING, name="name")
              self.UiParameter(name="age", type=AttributeType.LONG)
              self.UiParameter(AttributeType.DOUBLE, name="score")
              self.UiParameter("created_at", type=AttributeType.TIMESTAMP)
    `;

    expect(service.parse(code)).toEqual([
      { attribute: { attributeName: "count", attributeType: "INT" }, value: "" },
      { attribute: { attributeName: "name", attributeType: "STRING" }, value: "" },
      { attribute: { attributeName: "age", attributeType: "LONG" }, value: "" },
      { attribute: { attributeName: "score", attributeType: "DOUBLE" }, value: "" },
      { attribute: { attributeName: "created_at", attributeType: "TIMESTAMP" }, value: "" },
    ]);
  });

  it("should ignore calls where name or type is missing", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter(name="a")
              self.UiParameter(type=AttributeType.DOUBLE)
    `;

    expect(service.parse(code)).toEqual([]);
  });

  it("should ignore legacy key= named argument", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter(type=AttributeType.DOUBLE, key="a")
    `;

    expect(service.parse(code)).toEqual([]);
  });

  it("should ignore unsupported classes", () => {
    const code = `
      class RandomClass(ABC):
          def open(self):
              self.UiParameter(type=AttributeType.DOUBLE, name="a")
    `;

    expect(service.parse(code)).toEqual([]);
  });
});
