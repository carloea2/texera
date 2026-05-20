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

import { Injectable } from "@angular/core";
import { parser } from "@lezer/python";
import { AttributeType, SchemaAttribute } from "../../types/workflow-compiling.interface";

// Keep in sync with Python UDF template class names in PythonUDFOpDescV2, DualInputPortsPythonUDFOpDescV2, and PythonUDFSourceOpDescV2.
const SUPPORTED_CLASS_NAMES = new Set([
  "ProcessTupleOperator",
  "ProcessBatchOperator",
  "ProcessTableOperator",
  "GenerateOperator",
]);

type ParserSyntaxNode = ReturnType<typeof parser.parse>["topNode"];
type ParsedArgument = Readonly<{ key?: string; value: ParserSyntaxNode }>;

export type UiUdfParameter = Readonly<{ attribute: SchemaAttribute; value: string }>;

export class UiUdfParametersParseError extends Error {}

// Accept Java enum names (INTEGER, BOOLEAN) and Python enum aliases (INT, BOOL).
const ATTRIBUTE_TYPES_BY_TOKEN: Readonly<Record<string, AttributeType>> = {
  STRING: "string",
  INTEGER: "integer",
  INT: "integer",
  LONG: "long",
  DOUBLE: "double",
  BOOLEAN: "boolean",
  BOOL: "boolean",
  TIMESTAMP: "timestamp",
};

@Injectable({ providedIn: "root" })
export class UiUdfParametersParserService {
  parse(code: string): UiUdfParameter[] {
    if (!code) return [];

    const result: UiUdfParameter[] = [];
    const seen = new Set<string>();
    let supportedClassCount = 0;
    let duplicateName: string | undefined;
    const addParameter = (parameter?: UiUdfParameter): void => {
      const name = parameter?.attribute.attributeName;
      if (parameter && name) {
        if (seen.has(name)) {
          duplicateName = name;
          return;
        }
        seen.add(name);
        result.push(parameter);
      }
    };

    parser.parse(code).iterate({
      enter: ({ name, node }) => {
        const className = node.getChild("VariableName");
        if (
          name !== "ClassDefinition" ||
          !className ||
          !SUPPORTED_CLASS_NAMES.has(code.slice(className.from, className.to))
        )
          return;
        supportedClassCount++;
        node.cursor().iterate(cursorReference => {
          if (cursorReference.name !== "CallExpression") return;
          addParameter(readCall(cursorReference.node, code));
          return false;
        });
        return false;
      },
    });

    if (supportedClassCount > 1)
      throw new UiUdfParametersParseError("Only one Python UDF class can declare UiParameter values.");

    if (duplicateName)
      throw new UiUdfParametersParseError(`UiParameter name '${duplicateName}' is declared more than once.`);

    return result;
  }
}

function readCall(call: ParserSyntaxNode, code: string): UiUdfParameter | undefined {
  const argumentList = call.getChild("ArgList");
  if (!argumentList || code.slice(call.from, argumentList.from).replace(/\s+/g, "") !== "self.UiParameter")
    return undefined;

  let attributeName: string | undefined;
  let attributeType: AttributeType | undefined;
  let positionalIndex = 0;
  let sawNamed = false;

  for (const argument of readArguments(argumentList, code)) {
    const key = argument.key ?? (positionalIndex === 0 ? "name" : positionalIndex === 1 ? "type" : undefined);

    if (argument.key) {
      sawNamed = true;
    } else if (sawNamed) {
      return undefined;
    }

    if (key === "name" && !attributeName) attributeName = readName(argument.value, code);
    else if ((key === "type" || key === "attr_type") && !attributeType) attributeType = readType(argument.value, code);
    else return undefined;

    if ((key === "name" && !attributeName) || ((key === "type" || key === "attr_type") && !attributeType))
      return undefined;

    if (!argument.key) {
      positionalIndex++;
    }
  }

  return attributeName && attributeType ? { attribute: { attributeName, attributeType }, value: "" } : undefined;
}

function readArguments(argumentList: ParserSyntaxNode, code: string): ParsedArgument[] {
  const result: ParsedArgument[] = [];
  const children = getChildren(argumentList).filter(node => !["(", ")", ","].includes(node.name));

  for (let index = 0; index < children.length; index++) {
    const node = children[index];

    if (node.name === "VariableName" && children[index + 1]?.name === "AssignOp") {
      const value = children[index + 2];
      if (!value) return [];
      result.push({ key: code.slice(node.from, node.to), value });
      index += 2;
    } else if (node.name !== "AssignOp") {
      result.push({ value: node });
    } else {
      return [];
    }
  }

  return result;
}

function getChildren(node: ParserSyntaxNode): ParserSyntaxNode[] {
  const children: ParserSyntaxNode[] = [];
  for (let child = node.firstChild; child; child = child.nextSibling) children.push(child);
  return children;
}

function readName(value: ParserSyntaxNode, code: string): string | undefined {
  const name = value.name === "String" ? readString(code.slice(value.from, value.to))?.trim() : undefined;
  return name || undefined;
}

function readType(value: ParserSyntaxNode, code: string): AttributeType | undefined {
  if (value.name !== "MemberExpression") return undefined;
  const token = code
    .slice(value.from, value.to)
    .trim()
    .replace(/\s+/g, "")
    .match(/^AttributeType\.([A-Za-z_]\w*)$/)?.[1]
    .toUpperCase();
  return token ? ATTRIBUTE_TYPES_BY_TOKEN[token] : undefined;
}

function readString(input: string): string | undefined {
  return input
    .trim()
    .match(/^[rRuU]*(?:"""([\s\S]*)"""|'''([\s\S]*)'''|"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)')$/)
    ?.slice(1)
    .find(value => value !== undefined);
}
