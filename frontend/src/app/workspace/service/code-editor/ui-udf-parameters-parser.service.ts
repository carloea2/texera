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

    const sanitizedCode = this.stripCommentsAndDocstrings(code);
    const classPattern = UiUdfParametersParserService.SUPPORTED_CLASSES.join("|");
    const classRegex = new RegExp(
      `class\\s+(${classPattern})\\s*\\([^)]*\\)\\s*:[\\s\\S]*?(?=\\nclass\\s+\\w+\\s*\\(|$)`,
      "g"
    );

    const parsed: UiUdfParameter[] = [];
    const existingNames = new Set<string>();

    let classMatch: RegExpExecArray | null;
    while ((classMatch = classRegex.exec(sanitizedCode)) !== null) {
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

  private stripCommentsAndDocstrings(code: string): string {
    let result = "";
    let inSingle = false;
    let inDouble = false;
    let inTripleSingle = false;
    let inTripleDouble = false;
    let escaped = false;

    for (let i = 0; i < code.length; i++) {
      const current = code[i];
      const nextThree = code.slice(i, i + 3);

      if (inTripleSingle) {
        if (nextThree === "'''") {
          result += "   ";
          i += 2;
          inTripleSingle = false;
        } else {
          result += current === "\n" ? "\n" : " ";
        }
        continue;
      }

      if (inTripleDouble) {
        if (nextThree === "\"\"\"") {
          result += "   ";
          i += 2;
          inTripleDouble = false;
        } else {
          result += current === "\n" ? "\n" : " ";
        }
        continue;
      }

      if (escaped) {
        result += current;
        escaped = false;
        continue;
      }

      if ((inSingle || inDouble) && current === "\\") {
        result += current;
        escaped = true;
        continue;
      }

      if (!inSingle && !inDouble && nextThree === "'''") {
        result += "   ";
        i += 2;
        inTripleSingle = true;
        continue;
      }

      if (!inSingle && !inDouble && nextThree === "\"\"\"") {
        result += "   ";
        i += 2;
        inTripleDouble = true;
        continue;
      }

      if (!inDouble && current === "'") {
        inSingle = !inSingle;
        result += current;
        continue;
      }

      if (!inSingle && current === "\"") {
        inDouble = !inDouble;
        result += current;
        continue;
      }

      if (!inSingle && !inDouble && current === "#") {
        while (i < code.length && code[i] !== "\n") {
          result += " ";
          i++;
        }
        if (i < code.length) {
          result += "\n";
        }
        continue;
      }

      result += current;
    }

    return result;
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
    let attributeName: string | undefined;
    let attributeType: AttributeType | undefined;
    let sawNamedArgument = false;
    let positionalIndex = 0;

    for (const token of tokens) {
      const namedNameMatch = token.match(/^name\s*=\s*["']([^"']+)["']$/);
      if (namedNameMatch) {
        sawNamedArgument = true;
        if (attributeName) {
          return undefined;
        }

        attributeName = namedNameMatch[1].trim();
        continue;
      }

      const namedTypeMatch = token.match(/^(type|attr_type)\s*=\s*AttributeType\.([A-Za-z_][A-Za-z0-9_]*)$/);
      if (namedTypeMatch) {
        sawNamedArgument = true;
        if (attributeType) {
          return undefined;
        }

        attributeType = this.normalizeAttributeType(namedTypeMatch[2]);
        if (!attributeType) {
          return undefined;
        }

        continue;
      }

      const positionalNameMatch = token.match(/^["']([^"']+)["']$/);
      if (positionalNameMatch) {
        if (sawNamedArgument || positionalIndex !== 0 || attributeName) {
          return undefined;
        }

        attributeName = positionalNameMatch[1].trim();
        positionalIndex++;
        continue;
      }

      const positionalTypeMatch = token.match(/^AttributeType\.([A-Za-z_][A-Za-z0-9_]*)$/);
      if (positionalTypeMatch) {
        if (sawNamedArgument || positionalIndex !== 1 || attributeType) {
          return undefined;
        }

        attributeType = this.normalizeAttributeType(positionalTypeMatch[1]);
        if (!attributeType) {
          return undefined;
        }

        positionalIndex++;
        continue;
      }

      return undefined;
    }

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

    const canonical = ATTRIBUTE_TYPE_TOKEN_TO_CANONICAL[normalized as ParserAttributeTypeToken];
    return SUPPORTED_UI_PARAMETER_ATTRIBUTE_TYPES.has(canonical) ? canonical : undefined;
  }
}
