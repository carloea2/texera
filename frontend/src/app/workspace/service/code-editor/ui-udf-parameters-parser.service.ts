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
import { AttributeType, UiUdfParameter } from "../../types/workflow-compiling.interface";

// Keep in sync with Python UDF template class names in PythonUDFOpDescV2, DualInputPortsPythonUDFOpDescV2, and PythonUDFSourceOpDescV2.
const CLASSES = new Set(["ProcessTupleOperator", "ProcessBatchOperator", "ProcessTableOperator", "GenerateOperator"]);

// Java enum constant names (AttributeType.java)
const JAVA_ATTRIBUTE_TYPE_NAMES = [
  "STRING",
  "INTEGER",
  "LONG",
  "DOUBLE",
  "BOOLEAN",
  "TIMESTAMP",
  "BINARY",
  "LARGE_BINARY",
] as const;

type JavaAttributeTypeName = (typeof JAVA_ATTRIBUTE_TYPE_NAMES)[number];

// Python enum constant names (core.models.AttributeType)
const PYTHON_ATTRIBUTE_TYPE_NAMES = [
  "STRING",
  "INT",
  "LONG",
  "DOUBLE",
  "BOOL",
  "TIMESTAMP",
  "BINARY",
  "LARGE_BINARY",
] as const;

type PythonAttributeTypeName = (typeof PYTHON_ATTRIBUTE_TYPE_NAMES)[number];

type ParserAttributeTypeToken = JavaAttributeTypeName | PythonAttributeTypeName;
type UnsupportedParserAttributeTypeToken = "BINARY" | "LARGE_BINARY";
type SupportedParserAttributeTypeToken = Exclude<ParserAttributeTypeToken, UnsupportedParserAttributeTypeToken>;
type ParserSyntaxNode = ReturnType<typeof parser.parse>["topNode"];
type ParsedArgument = Readonly<{ key?: string; value: ParserSyntaxNode }>;
type UiParameterArgument = Readonly<{ kind: "name"; value: string }> | Readonly<{ kind: "type"; value: AttributeType }>;

const TYPES: Readonly<Record<SupportedParserAttributeTypeToken, AttributeType>> = {
  STRING: "string",
  INTEGER: "integer",
  INT: "integer",
  LONG: "long",
  DOUBLE: "double",
  BOOLEAN: "boolean",
  BOOL: "boolean",
  TIMESTAMP: "timestamp",
};

const JAVA_ATTRIBUTE_TYPE_NAME_SET = new Set<string>(JAVA_ATTRIBUTE_TYPE_NAMES);
const PYTHON_ATTRIBUTE_TYPE_NAME_SET = new Set<string>(PYTHON_ATTRIBUTE_TYPE_NAMES);
const SUPPORTED_UI_PARAMETER_ATTRIBUTE_TYPES = new Set<AttributeType>([
  "string",
  "integer",
  "long",
  "double",
  "boolean",
  "timestamp",
]);

@Injectable({ providedIn: "root" })
export class UiUdfParametersParserService {
  parse(code: string): UiUdfParameter[] {
    if (!code) return [];

    const result: UiUdfParameter[] = [];
    const seen = new Set<string>();
    const add = (parameter?: UiUdfParameter): void => {
      const name = parameter?.attribute.attributeName;
      if (parameter && name && !seen.has(name)) {
        seen.add(name);
        result.push(parameter);
      }
    };

    parser.parse(code).iterate({
      enter: ({ name, node }) => {
        const className = node.getChild("VariableName");
        if (name !== "ClassDefinition" || !className || !CLASSES.has(code.slice(className.from, className.to))) return;
        node.cursor().iterate(ref => {
          if (ref.name !== "CallExpression") return;
          add(readCall(ref.node, code));
          return false;
        });
        return false;
      },
    });

    return result;
  }
}

function readCall(call: ParserSyntaxNode, code: string): UiUdfParameter | undefined {
  const args = call.getChild("ArgList");
  if (!args || code.slice(call.from, args.from).replace(/\s+/g, "") !== "self.UiParameter") return undefined;

  let attributeName: string | undefined;
  let attributeType: AttributeType | undefined;
  let positionalIndex = 0;
  let sawNamed = false;

  for (const arg of readArguments(args, code)) {
    if (arg.key) {
      sawNamed = true;
    } else if (sawNamed) {
      return undefined;
    }

    const parsedArg = readArgument(arg, positionalIndex, code);
    if (!parsedArg) {
      return undefined;
    }

    if (parsedArg.kind === "name") {
      if (attributeName) {
        return undefined;
      }
      attributeName = parsedArg.value;
    } else {
      if (attributeType) {
        return undefined;
      }
      attributeType = parsedArg.value;
    }

    if (!arg.key) {
      positionalIndex++;
    }
  }

  return attributeName && attributeType ? { attribute: { attributeName, attributeType }, value: "" } : undefined;
}

function readArguments(args: ParserSyntaxNode, code: string): ParsedArgument[] {
  const result: ParsedArgument[] = [];
  const children = getChildren(args).filter(node => !["(", ")", ","].includes(node.name));

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

function readArgument(arg: ParsedArgument, positionalIndex: number, code: string): UiParameterArgument | undefined {
  const key = arg.key;
  const value = code.slice(arg.value.from, arg.value.to);

  if ((key === "name" || (!key && positionalIndex === 0)) && arg.value.name === "String") {
    const name = readString(value)?.trim();
    return name ? { kind: "name", value: name } : undefined;
  }

  if (
    (key === "type" || key === "attr_type" || (!key && positionalIndex === 1)) &&
    arg.value.name === "MemberExpression"
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

  if (!JAVA_ATTRIBUTE_TYPE_NAME_SET.has(token) && !PYTHON_ATTRIBUTE_TYPE_NAME_SET.has(token)) {
    return undefined;
  }

  if (token === "BINARY" || token === "LARGE_BINARY") {
    return undefined;
  }

  const canonical = TYPES[token as SupportedParserAttributeTypeToken];
  return SUPPORTED_UI_PARAMETER_ATTRIBUTE_TYPES.has(canonical) ? canonical : undefined;
}
