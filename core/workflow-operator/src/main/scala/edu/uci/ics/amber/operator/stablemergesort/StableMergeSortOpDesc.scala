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

package edu.uci.ics.amber.operator.stablemergesort

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.util

class StableMergeSortOpDesc extends LogicalOp {

  import StableMergeSortOpDesc._

  @JsonProperty(value = "keys", required = true)
  @JsonSchemaTitle("Sort Keys")
  @JsonPropertyDescription("List of attributes to sort by with ordering preferences.")
  var keys: util.List[StableSortKey] = new util.ArrayList[StableSortKey]()

  @JsonProperty("offset")
  @JsonSchemaTitle("Offset")
  @JsonPropertyDescription("Number of sorted tuples to skip before emitting results.")
  var offset: Int = 0

  @JsonProperty("limit")
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("Maximum number of tuples to emit after applying the offset. Leave empty to emit all tuples.")
  var limit: Integer = null

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.stablemergesort.StableMergeSortOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Stable Merge Sort",
      "Stable per-partition sort with multi-key ordering (stable merge sort).",
      OperatorGroupConstants.SORT_GROUP,
      List(InputPort()),
      List(OutputPort(blocking = true))
    )
}

object StableMergeSortOpDesc {

  class StableSortKey {

    @JsonProperty(value = "attribute", required = true)
    @JsonSchemaTitle("Attribute")
    @JsonPropertyDescription("Attribute to sort by.")
    @AutofillAttributeName
    var attribute: String = _

    @JsonProperty("order")
    @JsonSchemaTitle("Order")
    @JsonPropertyDescription("Sort order: asc for ascending or desc for descending.")
    @JsonSchemaInject(json = """{"enum": ["asc", "desc"]}""")
    var order: String = "asc"

    @JsonProperty("nulls")
    @JsonSchemaTitle("Nulls")
    @JsonPropertyDescription("Placement of null values: first or last.")
    @JsonSchemaInject(json = """{"enum": ["first", "last"]}""")
    var nulls: String = "last"


    @JsonProperty("caseInsensitive")
    @JsonSchemaTitle("Case Insensitive")
    @JsonPropertyDescription("Treat string comparisons as case-insensitive.")
    var caseInsensitive: Boolean = false
  }
}
