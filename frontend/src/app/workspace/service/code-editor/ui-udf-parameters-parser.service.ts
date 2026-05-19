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
type UiParameterArgument = Readonly<{ kind: "name"; value: string }> | Readonly<{ kind: "type"; value: AttributeType }>;

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

    if (supportedClassCount > 1) {
      throw new UiUdfParametersParseError("Only one Python UDF class can declare UiParameter values.");
    }

    if (duplicateName) {
      throw new UiUdfParametersParseError(`UiParameter name '${duplicateName}' is declared more than once.`);
    }

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
    if (argument.key) {
      sawNamed = true;
    } else if (sawNamed) {
      return undefined;
    }

    const parsedArgument = readArgument(argument, positionalIndex, code);
    if (!parsedArgument) {
      return undefined;
    }

    if (parsedArgument.kind === "name") {
      if (attributeName) {
        return undefined;
      }
      attributeName = parsedArgument.value;
    } else {
      if (attributeType) {
        return undefined;
      }
      attributeType = parsedArgument.value;
    }

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
      if (!value) {
        return [];
      }
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
  for (let child = node.firstChild; child; child = child.nextSibling) {
    children.push(child);
  }
  return children;
}

function readArgument(
  argument: ParsedArgument,
  positionalIndex: number,
  code: string
): UiParameterArgument | undefined {
  const key = argument.key;
  const value = code.slice(argument.value.from, argument.value.to);

  if ((key === "name" || (!key && positionalIndex === 0)) && argument.value.name === "String") {
    const name = readString(value)?.trim();
    return name ? { kind: "name", value: name } : undefined;
  }

  if (
    (key === "type" || key === "attr_type" || (!key && positionalIndex === 1)) &&
    argument.value.name === "MemberExpression"
  ) {
    const type = readType(value);
    return type ? { kind: "type", value: type } : undefined;
  }

  return undefined;
}

function readString(input: string): string | undefined {
  return input
    .trim()
    .match(/^[rRuU]*(?:"""([\s\S]*)"""|'''([\s\S]*)'''|"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)')$/)
    ?.slice(1)
    .find(value => value !== undefined);
}

function readType(input: string): AttributeType | undefined {
  const token = input
    .trim()
    .replace(/\s+/g, "")
    .match(/^AttributeType\.([A-Za-z_]\w*)$/)?.[1]
    .toUpperCase();
  if (!token) {
    return undefined;
  }

  return ATTRIBUTE_TYPES_BY_TOKEN[token];
}
