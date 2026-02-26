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
import {
  AttributeType,
  JavaAttributeTypeName,
  PythonAttributeTypeName,
  SchemaAttribute,
  JAVA_ATTRIBUTE_TYPE_NAMES,
  PYTHON_ATTRIBUTE_TYPE_NAMES,
} from "../../types/workflow-compiling.interface";

export interface UiUdfParameter {
  attribute: SchemaAttribute;
  value: string;
}

type ParserAttributeTypeToken = JavaAttributeTypeName | PythonAttributeTypeName;

const ATTRIBUTE_TYPE_TOKEN_TO_CANONICAL: Readonly<Record<ParserAttributeTypeToken, AttributeType>> = {
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

@Injectable({ providedIn: "root" })
export class UiUdfParametersParserService {
  private static readonly SUPPORTED_CLASSES = [
    "ProcessTupleOperator",
    "ProcessBatchOperator",
    "ProcessTableOperator",
    "GenerateOperator",
  ];

  parse(code: string): UiUdfParameter[] {
    if (!code) {
      return [];
    }

    const classPattern = UiUdfParametersParserService.SUPPORTED_CLASSES.join("|");
    const classRegex = new RegExp(
      `class\\s+(${classPattern})\\s*\\([^)]*\\)\\s*:[\\s\\S]*?(?=\\nclass\\s+\\w+\\s*\\(|$)`,
      "g"
    );

    const parsed: UiUdfParameter[] = [];
    const existingNames = new Set<string>();

    let classMatch: RegExpExecArray | null;
    while ((classMatch = classRegex.exec(code)) !== null) {
      const classBlock = classMatch[0];

      for (const args of this.extractUiParameterArgumentLists(classBlock)) {
        const argumentTokens = this.tokenizeArguments(args);
        const extracted = this.extractParameter(argumentTokens);
        if (!extracted || existingNames.has(extracted.attribute.attributeName)) {
          continue;
        }

        existingNames.add(extracted.attribute.attributeName);
        parsed.push(extracted);
      }
    }

    return parsed;
  }

  /**
   * Extract argument strings from self.UiParameter(...)
   * More robust than regex when there are nested parentheses.
   */
  private extractUiParameterArgumentLists(code: string): string[] {
    const result: string[] = [];
    const needle = "self.UiParameter(";
    let index = 0;

    while (index < code.length) {
      const start = code.indexOf(needle, index);
      if (start === -1) {
        break;
      }

      const openParenIndex = start + needle.length - 1;
      const closeParenIndex = this.findMatchingParen(code, openParenIndex);
      if (closeParenIndex === -1) {
        break;
      }

      result.push(code.slice(openParenIndex + 1, closeParenIndex));
      index = closeParenIndex + 1;
    }

    return result;
  }

  /**
   * Find matching ')' for a '(' while ignoring quoted strings.
   */
  private findMatchingParen(text: string, openIndex: number): number {
    let depth = 0;
    let inSingle = false;
    let inDouble = false;
    let escaped = false;

    for (let i = openIndex; i < text.length; i++) {
      const ch = text[i];

      if (escaped) {
        escaped = false;
        continue;
      }

      if ((inSingle || inDouble) && ch === "\\") {
        escaped = true;
        continue;
      }

      if (!inDouble && ch === "'") {
        inSingle = !inSingle;
        continue;
      }

      if (!inSingle && ch === "\"") {
        inDouble = !inDouble;
        continue;
      }

      if (inSingle || inDouble) {
        continue;
      }

      if (ch === "(") {
        depth++;
      } else if (ch === ")") {
        depth--;
        if (depth === 0) {
          return i;
        }
      }
    }

    return -1;
  }

  /**
   * Split on top-level commas only (ignores commas inside strings / nested calls).
   */
  private tokenizeArguments(argumentList: string): string[] {
    const tokens: string[] = [];
    let current = "";
    let depth = 0;
    let inSingle = false;
    let inDouble = false;
    let escaped = false;

    for (let i = 0; i < argumentList.length; i++) {
      const ch = argumentList[i];

      if (escaped) {
        current += ch;
        escaped = false;
        continue;
      }

      if ((inSingle || inDouble) && ch === "\\") {
        current += ch;
        escaped = true;
        continue;
      }

      if (!inDouble && ch === "'") {
        inSingle = !inSingle;
        current += ch;
        continue;
      }

      if (!inSingle && ch === "\"") {
        inDouble = !inDouble;
        current += ch;
        continue;
      }

      if (!inSingle && !inDouble) {
        if (ch === "(") {
          depth++;
          current += ch;
          continue;
        }

        if (ch === ")") {
          depth = Math.max(0, depth - 1);
          current += ch;
          continue;
        }

        if (ch === "," && depth === 0) {
          const token = current.trim();
          if (token.length > 0) {
            tokens.push(token);
          }
          current = "";
          continue;
        }
      }

      current += ch;
    }

    const tail = current.trim();
    if (tail.length > 0) {
      tokens.push(tail);
    }

    return tokens;
  }

  private extractParameter(tokens: string[]): UiUdfParameter | undefined {
    let namedName: string | undefined;
    let namedType: AttributeType | undefined;
    let positionalName: string | undefined;
    let positionalType: AttributeType | undefined;

    for (const token of tokens) {
      const namedNameMatch = token.match(/^name\s*=\s*["']([^"']+)["']$/);
      if (namedNameMatch) {
        namedName = namedNameMatch[1].trim();
        continue;
      }

      const namedTypeMatch = token.match(/^type\s*=\s*AttributeType\.([A-Za-z_][A-Za-z0-9_]*)$/);
      if (namedTypeMatch) {
        namedType = this.normalizeAttributeType(namedTypeMatch[1]);
        continue;
      }

      const positionalTypeMatch = token.match(/^AttributeType\.([A-Za-z_][A-Za-z0-9_]*)$/);
      if (positionalTypeMatch && !positionalType) {
        positionalType = this.normalizeAttributeType(positionalTypeMatch[1]);
        continue;
      }

      const positionalNameMatch = token.match(/^["']([^"']+)["']$/);
      if (positionalNameMatch && !positionalName) {
        positionalName = positionalNameMatch[1].trim();
      }
    }

    const attributeName = namedName ?? positionalName;
    const attributeType = namedType ?? positionalType;

    if (!attributeName || !attributeType) {
      return undefined;
    }

    return {
      attribute: {
        attributeName,
        attributeType,
      },
      value: "",
    };
  }

  /**
   * Convert Java/Python enum tokens into canonical schema names.
   * Examples:
   *   STRING -> string
   *   INTEGER -> integer
   *   INT -> integer
   *   BOOL -> boolean
   */
  private normalizeAttributeType(token: string): AttributeType | undefined {
    const normalized = token.trim().toUpperCase();

    if (!JAVA_ATTRIBUTE_TYPE_NAME_SET.has(normalized) && !PYTHON_ATTRIBUTE_TYPE_NAME_SET.has(normalized)) {
      return undefined;
    }

    return ATTRIBUTE_TYPE_TOKEN_TO_CANONICAL[normalized as ParserAttributeTypeToken];
  }
}
