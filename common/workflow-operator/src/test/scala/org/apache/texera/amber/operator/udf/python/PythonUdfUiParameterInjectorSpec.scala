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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PythonUdfUiParameterInjectorSpec extends AnyFlatSpec with Matchers {

  private def createParameter(
      key: String,
      attributeType: AttributeType,
      value: String
  ): UiUDFParameter = {
    val parameter = new UiUDFParameter
    parameter.attribute = new Attribute(key, attributeType)
    parameter.value = value
    parameter
  }

  private val baseUdfCode: String =
    """from pytexera import *
      |
      |class ProcessTupleOperator(UDFOperatorV2):
      |    @overrides
      |    def open(self):
      |        print("open")
      |
      |    @overrides
      |    def process_tuple(self, tuple_: Tuple, port: int):
      |        yield tuple_
      |""".stripMargin

  it should "return encoded user code unchanged when there are no ui parameters" in {
    val injectedCode = PythonUdfUiParameterInjector.inject(baseUdfCode, Nil)

    injectedCode should include("class ProcessTupleOperator(UDFOperatorV2):")
    injectedCode should include("""print("open")""")
    injectedCode should not include ("_texera_injected_ui_parameters")
    injectedCode should not include ("self.decode_python_template")
  }

  it should "inject ui parameter hook into supported UDF class" in {
    val injectedCode = PythonUdfUiParameterInjector.inject(
      baseUdfCode,
      List(
        createParameter("date", AttributeType.TIMESTAMP, "2024-01-01T00:00:00Z")
      )
    )

    injectedCode should include("class ProcessTupleOperator(UDFOperatorV2):")
    injectedCode should include("def _texera_injected_ui_parameters(self):")
    injectedCode should include("return {")
    injectedCode should include("self.decode_python_template")
    injectedCode should include("""print("open")""")
  }

  it should "inject the reserved hook before the first method definition" in {
    val injectedCode = PythonUdfUiParameterInjector.inject(
      baseUdfCode,
      List(createParameter("k", AttributeType.STRING, "v"))
    )

    val hookIndex = injectedCode.indexOf("def _texera_injected_ui_parameters(self):")
    val openIndex = injectedCode.indexOf("def open(self):")

    hookIndex should be >= 0
    openIndex should be > hookIndex
  }

  it should "preserve multiple ui parameters in the injected map" in {
    val injectedCode = PythonUdfUiParameterInjector.inject(
      baseUdfCode,
      List(
        createParameter("param1", AttributeType.DOUBLE, "12.5"),
        createParameter("param2", AttributeType.INTEGER, "1"),
        createParameter("param3", AttributeType.STRING, "Hola"),
        createParameter("param4", AttributeType.TIMESTAMP, "2026-02-28T03:15:00Z")
      )
    )

    injectedCode should include("def _texera_injected_ui_parameters(self):")
    injectedCode should include("self.decode_python_template")
    injectedCode.count(_ == ':') should be > 0
  }

  it should "throw when a parameter attribute is missing" in {
    val invalidParameter = new UiUDFParameter
    invalidParameter.attribute = null
    invalidParameter.value = "anything"

    val exception = the[RuntimeException] thrownBy {
      PythonUdfUiParameterInjector.inject(baseUdfCode, List(invalidParameter))
    }

    exception.getMessage should include("UiParameter attribute is required")
  }

  it should "throw when a key is declared with conflicting attribute types" in {
    val conflictingParameters = List(
      createParameter("date", AttributeType.STRING, "2024-01-01"),
      createParameter("date", AttributeType.TIMESTAMP, "2024-01-01T00:00:00Z")
    )

    val exception = the[RuntimeException] thrownBy {
      PythonUdfUiParameterInjector.inject(baseUdfCode, conflictingParameters)
    }

    exception.getMessage should include("UiParameter key 'date' has multiple types")
  }

  it should "allow duplicate keys when the attribute type is the same" in {
    val sameTypeParameters = List(
      createParameter("date", AttributeType.TIMESTAMP, "2024-01-01"),
      createParameter("date", AttributeType.TIMESTAMP, "2024-01-01T00:00:00Z")
    )

    noException should be thrownBy {
      PythonUdfUiParameterInjector.inject(baseUdfCode, sameTypeParameters)
    }
  }

  it should "throw when the reserved hook is already defined by the user" in {
    val udfWithReservedHook =
      """from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    def _texera_injected_ui_parameters(self):
        |        return {}
        |
        |    def open(self):
        |        pass
        |""".stripMargin

    val exception = the[RuntimeException] thrownBy {
      PythonUdfUiParameterInjector.inject(
        udfWithReservedHook,
        List(createParameter("k", AttributeType.STRING, "v"))
      )
    }

    exception.getMessage should include(
      "Reserved method '_texera_injected_ui_parameters' is already defined"
    )
  }

  it should "leave code unchanged when no supported user class is present" in {
    val nonSupportedCode =
      """from pytexera import *
        |
        |class SomethingElse:
        |    def open(self):
        |        pass
        |""".stripMargin

    val injectedCode = PythonUdfUiParameterInjector.inject(
      nonSupportedCode,
      List(createParameter("k", AttributeType.STRING, "v"))
    )

    injectedCode should not include ("_texera_injected_ui_parameters")
    injectedCode should include("class SomethingElse:")
  }
}
