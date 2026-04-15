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

package org.apache.texera.amber.operator.udf.python.source

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.operator.udf.python.{PythonUdfUiParameterInjector, UiUDFParameter}

class PythonUDFSourceOpDescV2 extends SourceOperatorDescriptor {

  @JsonProperty(
    required = true,
    defaultValue = "# UiParameter notes:\n" +
      "# - A UiParameter is a user-editable value exposed in the property panel and read from your Python code.\n" +
      "# - Define UiParameter values in open() and then use them later in your UDF methods.\n" +
      "# - Active UiParameter calls appear in the property panel; commented-out calls are ignored.\n" +
      "# - Supported UiParameter types are STRING, INT/LONG, DOUBLE, BOOL, and TIMESTAMP.\n" +
      "# \n" +
      "# from pytexera import *\n" +
      "# class GenerateOperator(UDFSourceOperator):\n" +
      "# \n" +
      "#     @overrides\n" +
      "#     def open(self):\n" +
      "#         self.value1 = self.UiParameter(\"string_param\", AttributeType.STRING).value\n" +
      "#         self.value2 = self.UiParameter(\"int_param\", AttributeType.INT).value\n" +
      "#         self.value3 = self.UiParameter(\"long_param\", AttributeType.LONG).value\n" +
      "#         self.value4 = self.UiParameter(\"double_param\", AttributeType.DOUBLE).value\n" +
      "#         self.value5 = self.UiParameter(\"bool_param\", AttributeType.BOOL).value\n" +
      "#         self.value6 = self.UiParameter(\"timestamp_param\", AttributeType.TIMESTAMP).value\n" +
      "# \n" +
      "#     @overrides\n" +
      "#     \n" +
      "#     def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:\n" +
      "#         yield\n"
  )
  @JsonSchemaTitle("Python script")
  @JsonPropertyDescription("Input your code here")
  var code: String = _

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to launch")
  var workers: Int = 1

  @JsonProperty()
  @JsonSchemaTitle("Columns")
  @JsonPropertyDescription("The columns of the source")
  var columns: List[Attribute] = List.empty

  @JsonProperty
  @JsonSchemaTitle("Parameters")
  @JsonPropertyDescription(
    "Parameters inferred from active self.UiParameter(...) calls in the Python script"
  )
  var uiParameters: List[UiUDFParameter] = List()

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    require(workers >= 1, "Need at least 1 worker.")
    val physicalOp = PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(PythonUdfUiParameterInjector.inject(code, uiParameters), "python")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
      .withLocationPreference(Option.empty)

    if (workers > 1) {
      physicalOp
        .withParallelizable(true)
        .withSuggestedWorkerNum(workers)
    } else {
      physicalOp.withParallelizable(false)
    }
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "1-out Python UDF",
      "User-defined function operator in Python script",
      OperatorGroupConstants.PYTHON_GROUP,
      List.empty, // No input ports for a source operator
      List(OutputPort()),
      supportReconfiguration = true
    )
  }

  override def sourceSchema(): Schema = {
    if (columns != null && columns.nonEmpty) {
      Schema().add(columns)
    } else {
      Schema()
    }
  }
}
