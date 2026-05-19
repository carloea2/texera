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

import {
  UiUdfParametersParseError,
  UiUdfParametersParserService,
  type UiUdfParameter,
} from "./ui-udf-parameters-parser.service";

describe("UiUdfParametersParserService", () => {
  let service: UiUdfParametersParserService;

  beforeEach(() => {
    service = new UiUdfParametersParserService();
  });

  it("should parse Python-compatible positional and name-based arguments", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter("count", AttributeType.INT)
              self.UiParameter(type=AttributeType.STRING, name="name")
              self.UiParameter(name="age", type=AttributeType.LONG)
              self.UiParameter("score", AttributeType.DOUBLE)
              self.UiParameter("created_at", type=AttributeType.TIMESTAMP)
    `;

    expect(service.parse(code)).toEqual([
      parameter("count", "integer"),
      parameter("name", "string"),
      parameter("age", "long"),
      parameter("score", "double"),
      parameter("created_at", "timestamp"),
    ]);
  });

  it("should parse the attr_type keyword used by the Python API", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter("count", attr_type=AttributeType.INT)
    `;

    expect(service.parse(code)).toEqual([parameter("count", "integer")]);
  });

  it("should parse multiline UiParameter calls with named arguments split across lines", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter(
                  name=
                      "threshold",
                  type=
                      AttributeType.DOUBLE,
              )
              self.UiParameter(
                  "label",
                  type=
                      AttributeType.STRING,
              )
    `;

    expect(service.parse(code)).toEqual([parameter("threshold", "double"), parameter("label", "string")]);
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

  it("should ignore invalid positional argument ordering", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter(AttributeType.INT, "count")
              self.UiParameter(name="valid", type=AttributeType.STRING)
    `;

    expect(service.parse(code)).toEqual([parameter("valid", "string")]);
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

  it("should ignore custom-named subclasses because injection targets template class names", () => {
    const code = `
      class MyTupleOp(UDFOperatorV2):
          def open(self):
              self.UiParameter("threshold", AttributeType.DOUBLE)

      class MyWrappedTupleOp(ProcessTupleOperator):
          def open(self):
              self.UiParameter("label", AttributeType.STRING)
    `;

    expect(service.parse(code)).toEqual([]);
  });

  it("should parse supported UiParameter calls when unsupported classes are present", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter("threshold", AttributeType.DOUBLE)

      class RandomClass(ABC):
          def open(self):
              self.UiParameter("ignored", AttributeType.STRING)
    `;

    expect(service.parse(code)).toEqual([parameter("threshold", "double")]);
  });

  it("should raise an error for multiple supported UDF classes because execution expects one concrete operator", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter("threshold", AttributeType.DOUBLE)

      class GenerateOperator(UDFSourceOperator):
          def open(self):
              self.UiParameter(name="batch_size", type=AttributeType.INT)
    `;

    expect(() => service.parse(code)).toThrow(UiUdfParametersParseError);
    expect(() => service.parse(code)).toThrow("Only one Python UDF class can declare UiParameter values.");
  });

  it("should ignore empty and extra positional arguments", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter()
              self.UiParameter("too_many", AttributeType.STRING, "extra")
              self.UiParameter("valid", AttributeType.STRING)
    `;

    expect(service.parse(code)).toEqual([parameter("valid", "string")]);
  });

  it("should raise an error for duplicate parameter names", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter("threshold", AttributeType.DOUBLE)
              self.UiParameter("threshold", AttributeType.STRING)
              self.UiParameter("label", AttributeType.STRING)
    `;

    expect(() => service.parse(code)).toThrow(UiUdfParametersParseError);
    expect(() => service.parse(code)).toThrow("UiParameter name 'threshold' is declared more than once.");
  });

  it("should ignore commented out UiParameter calls", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              # self.UiParameter("commented", AttributeType.INT)
              self.UiParameter("active", AttributeType.INT)  # self.UiParameter("trailing", AttributeType.STRING)
    `;

    expect(service.parse(code)).toEqual([parameter("active", "integer")]);
  });

  it("should ignore commented out multiline UiParameter sections", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              # self.UiParameter(
              #     name="commented",
              #     type=AttributeType.INT,
              # )
              self.UiParameter(
                  name="active",
                  type=AttributeType.STRING,
              )
    `;

    expect(service.parse(code)).toEqual([parameter("active", "string")]);
  });

  it("should ignore UiParameter examples inside triple-quoted strings", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              """
              self.UiParameter("example", AttributeType.INT)
              """
              self.UiParameter("active", AttributeType.DOUBLE)
    `;

    expect(service.parse(code)).toEqual([parameter("active", "double")]);
  });

  it("should reject binary UiParameter types", () => {
    const code = `
      class ProcessTupleOperator(UDFOperatorV2):
          def open(self):
              self.UiParameter("payload", AttributeType.BINARY)
              self.UiParameter("blob", AttributeType.LARGE_BINARY)
    `;

    expect(service.parse(code)).toEqual([]);
  });
});

function parameter(attributeName: string, attributeType: UiUdfParameter["attribute"]["attributeType"]): UiUdfParameter {
  return { attribute: { attributeName, attributeType }, value: "" };
}
