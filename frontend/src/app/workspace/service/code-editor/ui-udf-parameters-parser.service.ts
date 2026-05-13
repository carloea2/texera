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

export interface UiUdfParameter {
  attribute: SchemaAttribute;
  value: string;
}

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
type ParserSyntaxNode = ReturnType<typeof parser.parse>["topNode"];

const TYPES: Readonly<Record<ParserAttributeTypeToken, AttributeType>> = {
  STRING: "string",
  INTEGER: "integer",
  INT: "integer",
  LONG: "long",
  DOUBLE: "double",
  BOOLEAN: "boolean",
  BOOL: "boolean",
  TIMESTAMP: "timestamp",
  BINARY: "binary",
  LARGE_BINARY: "large_binary",
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
        node
          .cursor()
          .iterate(ref => (ref.name === "CallExpression" ? (add(readCall(ref.node, code)), false) : undefined));
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
  let index = 0;
  let sawNamed = false;

  for (const arg of splitArgs(code.slice(args.from + 1, args.to - 1))) {
    const match = arg.match(/^([A-Za-z_]\w*)\s*=\s*([\s\S]+)$/);
    const key = match?.[1];
    const value = match?.[2] ?? arg;

    if (match) sawNamed = true;
    else if (sawNamed || index > 1) return undefined;

    if ((match ? key === "name" : index === 0) && !attributeName) attributeName = readString(value)?.trim();
    else if ((match ? key === "type" || key === "attr_type" : index === 1) && !attributeType)
      attributeType = readType(value);
    else return undefined;

    if (!match) index++;
    if (!attributeName && (key === "name" || (!match && index === 1))) return undefined;
    if (!attributeType && (key === "type" || key === "attr_type" || (!match && index === 2))) return undefined;
  }

  return attributeName && attributeType ? { attribute: { attributeName, attributeType }, value: "" } : undefined;
}

function splitArgs(input: string): string[] {
  const result: string[] = [];
  let current = "";
  let depth = 0;
  let quote = "";
  let triple = false;
  let escaped = false;

  for (let i = 0; i < input.length; i++) {
    const char = input[i];
    current += char;

    if (quote) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (triple && input.slice(i, i + 3) === quote.repeat(3)) {
        current += input.slice(i + 1, i + 3);
        i += 2;
        quote = "";
        triple = false;
      } else if (!triple && char === quote) quote = "";
      continue;
    }

    if (char === "'" || char === '"') {
      quote = char;
      triple = input.slice(i, i + 3) === char.repeat(3);
      if (triple) {
        current += input.slice(i + 1, i + 3);
        i += 2;
      }
    } else if ("([{".includes(char)) depth++;
    else if (")]}".includes(char)) depth--;
    else if (char === "," && depth === 0) {
      result.push(current.slice(0, -1).trim());
      current = "";
    }
  }

  const tail = current.trim();
  return tail ? [...result, tail] : result;
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

  const canonical = TYPES[token as ParserAttributeTypeToken];
  return SUPPORTED_UI_PARAMETER_ATTRIBUTE_TYPES.has(canonical) ? canonical : undefined;
}
