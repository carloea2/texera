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
const UDF_CLASSES = new Set(
  "ProcessTupleOperator ProcessBatchOperator ProcessTableOperator GenerateOperator".split(" ")
);

const ATTRIBUTE_TYPES: Readonly<Record<string, AttributeType>> = {
  STRING: "string",
  INTEGER: "integer",
  INT: "integer",
  LONG: "long",
  DOUBLE: "double",
  BOOLEAN: "boolean",
  BOOL: "boolean",
  TIMESTAMP: "timestamp",
};

type ParserSyntaxNode = ReturnType<typeof parser.parse>["topNode"];
type RawArgument = Readonly<{ key?: string; value: ParserSyntaxNode }>;

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
        if (!isSupportedUdfClass(name, node, code)) return;
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

function isSupportedUdfClass(name: string, node: ParserSyntaxNode, code: string): boolean {
  const className = node.getChild("VariableName");
  return name === "ClassDefinition" && !!className && UDF_CLASSES.has(code.slice(className.from, className.to));
}

function readCall(call: ParserSyntaxNode, code: string): UiUdfParameter | undefined {
  const args = call.getChild("ArgList");
  if (!args || code.slice(call.from, args.from).replace(/\s+/g, "") !== "self.UiParameter") return undefined;

  let attributeName: string | undefined;
  let attributeType: AttributeType | undefined;
  let positionalIndex = 0;

  for (const arg of readArguments(args, code)) {
    const key = arg.key;
    const value = code.slice(arg.value.from, arg.value.to);
    const isName = key === "name" || (!key && positionalIndex === 0);
    const isType = key === "type" || key === "attr_type" || (!key && positionalIndex === 1);

    if (isName && arg.value.name === "String" && !attributeName) attributeName = readString(value)?.trim();
    else if (isType && arg.value.name === "MemberExpression" && !attributeType) attributeType = readType(value);
    else return undefined;
    if (!arg.key) positionalIndex++;
  }

  return attributeName && attributeType ? { attribute: { attributeName, attributeType }, value: "" } : undefined;
}

function readArguments(args: ParserSyntaxNode, code: string): RawArgument[] {
  const result: RawArgument[] = [];
  const children = childNodes(args).filter(node => !["(", ")", ","].includes(node.name));

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

function childNodes(node: ParserSyntaxNode): ParserSyntaxNode[] {
  const children: ParserSyntaxNode[] = [];
  for (let child = node.firstChild; child; child = child.nextSibling) {
    children.push(child);
  }
  return children;
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
  if (!token || token === "BINARY" || token === "LARGE_BINARY") return undefined;
  return ATTRIBUTE_TYPES[token];
}
