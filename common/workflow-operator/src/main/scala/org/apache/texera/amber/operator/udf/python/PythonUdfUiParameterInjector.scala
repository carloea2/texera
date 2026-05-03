/*
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
package org.apache.texera.amber.operator.udf.python

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

import scala.util.matching.Regex

object PythonUdfUiParameterInjector {

  private val ReservedHookMethod = "_texera_injected_ui_parameters"
  private val UnsupportedUiParameterTypes = Set(AttributeType.BINARY, AttributeType.LARGE_BINARY)

  // Match user-facing UDF classes (the ones users write)
  private val SupportedUserClassRegex: Regex =
    """(?m)^([ \t]*)class\s+(ProcessTupleOperator|ProcessBatchOperator|ProcessTableOperator|GenerateOperator)\s*\([^)]*\)\s*:\s*(?:#.*)?$""".r

  private def validate(uiParameters: List[UiUDFParameter]): Unit = {
    uiParameters.foreach { parameter =>
      if (parameter.attribute == null) {
        throw new RuntimeException("UiParameter attribute is required.")
      }

      if (UnsupportedUiParameterTypes.contains(parameter.attribute.getType)) {
        throw new RuntimeException(
          s"UiParameter type '${parameter.attribute.getType.name()}' is not supported. " +
            "Use string, integer, long, double, boolean, or timestamp instead."
        )
      }
    }

    val grouped = uiParameters.groupBy(_.attribute.getName)
    grouped.foreach {
      case (key, values) =>
        val typeSet = values.map(_.attribute.getType).toSet
        if (typeSet.size > 1) {
          throw new RuntimeException(
            s"UiParameter key '$key' has multiple types: ${typeSet.map(_.name()).mkString(",")}."
          )
        }
    }
  }

  private def buildInjectedParametersMap(
      uiParameters: List[UiUDFParameter]
  ): PythonTemplateBuilder = {
    val entries = uiParameters.map { parameter =>
      pyb"${parameter.attribute.getName}: ${parameter.value}"
    }

    entries.reduceOption((acc, entry) => acc + pyb", " + entry).getOrElse(pyb"")
  }

  private def buildInjectedHookMethod(uiParameters: List[UiUDFParameter]): String = {
    val injectedParametersMap = buildInjectedParametersMap(uiParameters)

    // unindented method; we indent it when inserting into the class body
    (pyb"""|@overrides
           |def """ + pyb"$ReservedHookMethod" + pyb"""(self) -> Dict[str, Any]:
                                                      |    return {""" +
      injectedParametersMap +
      pyb"""}
           |""").encode
  }

  private def indentBlock(block: String, indent: String): String = {
    block
      .split("\n", -1)
      .map { line =>
        if (line.nonEmpty) indent + line else line
      }
      .mkString("\n")
  }

  private def lineEndIndex(text: String, from: Int): Int = {
    val idx = text.indexOf('\n', from)
    if (idx < 0) text.length else idx
  }

  private def detectClassBlockEnd(code: String, classHeaderStart: Int, classIndent: String): Int = {
    val classLineEnd = lineEndIndex(code, classHeaderStart)
    var pos = if (classLineEnd < code.length) classLineEnd + 1 else code.length

    while (pos < code.length) {
      val end = lineEndIndex(code, pos)
      val line = code.substring(pos, end)

      val trimmed = line.trim
      val isBlank = trimmed.isEmpty

      // a top-level (or same/lower-indented) non-blank line ends the class block
      val currentIndentLen = line.prefixLength(ch => ch == ' ' || ch == '\t')
      val classIndentLen = classIndent.length

      if (!isBlank && currentIndentLen <= classIndentLen) {
        return pos
      }

      pos = if (end < code.length) end + 1 else code.length
    }

    code.length
  }

  private def containsReservedHook(classBlock: String): Boolean = {
    val hookRegex = ("""(?m)^[ \t]+def\s+""" + Regex.quote(ReservedHookMethod) + """\s*\(""").r
    hookRegex.findFirstIn(classBlock).isDefined
  }

  private def injectHookIntoUserClass(encodedUserCode: String, hookMethod: String): String = {
    val m = SupportedUserClassRegex.findFirstMatchIn(encodedUserCode).getOrElse {
      return encodedUserCode
    }

    val classHeaderStart = m.start
    val classIndent = m.group(1)
    val classBlockEnd = detectClassBlockEnd(encodedUserCode, classHeaderStart, classIndent)

    val classBlock = encodedUserCode.substring(classHeaderStart, classBlockEnd)

    if (containsReservedHook(classBlock)) {
      throw new RuntimeException(
        s"Reserved method '$ReservedHookMethod' is already defined in the UDF class. Please rename your method."
      )
    }

    val bodyIndent = inferClassBodyIndent(classBlock, classIndent).getOrElse(classIndent + "    ")
    val indentedHook = indentBlock(
      (if (classBlock.endsWith("\n")) "" else "\n") + hookMethod.trim + "\n",
      bodyIndent
    )

    encodedUserCode.substring(0, classBlockEnd) +
      indentedHook +
      encodedUserCode.substring(classBlockEnd)
  }

  private def inferClassBodyIndent(classBlock: String, classIndent: String): Option[String] = {
    val lines = classBlock.split("\n", -1).toList.drop(1) // skip class header line

    lines.collectFirst {
      case line if line.trim.nonEmpty =>
        val leading = line.takeWhile(ch => ch == ' ' || ch == '\t')
        if (leading.length > classIndent.length) leading else classIndent + "    "
    }
  }

  def inject(code: String, uiParameters: List[UiUDFParameter]): String = {
    val params = Option(uiParameters).getOrElse(List.empty)
    validate(params)

    // Let pyb encode the user's source normally
    val encodedUserCode = pyb"$code".encode

    // If there are no UI params, return unchanged code (no hook injection needed)
    if (params.isEmpty) {
      return encodedUserCode
    }

    // Build encoded hook method (contains self.decode_python_template(...))
    val hookMethod = buildInjectedHookMethod(params)

    // Inject hook into the UDF class body; Python base class will auto-call it before open()
    injectHookIntoUserClass(encodedUserCode, hookMethod)
  }
}
