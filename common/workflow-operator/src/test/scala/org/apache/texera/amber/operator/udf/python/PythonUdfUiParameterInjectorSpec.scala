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

  private def createParameter(key: String, attributeType: AttributeType, value: String): UiUDFParameter = {
    val parameter = new UiUDFParameter
    parameter.attribute = new Attribute(key, attributeType)
    parameter.value = value
    parameter
  }

  it should "inject ui parameter prelude through PythonTemplateBuilder" in {
    val injectedCode = PythonUdfUiParameterInjector.inject(
      "print('done')",
      List(createParameter("date", AttributeType.TIMESTAMP, "2024-01-01T00:00:00Z"))
    )

    injectedCode should include("UDFOperatorV2.set_injected_ui_parameters({")
    injectedCode should include("self.decode_python_template")
    injectedCode should include("print('done')")
  }

  it should "throw when a key is declared with conflicting types" in {
    val conflictingParameters = List(
      createParameter("date", AttributeType.DATE, "2024-01-01"),
      createParameter("date", AttributeType.TIMESTAMP, "2024-01-01T00:00:00Z")
    )

    assertThrows[RuntimeException] {
      PythonUdfUiParameterInjector.inject("print('done')", conflictingParameters)
    }
  }
}
