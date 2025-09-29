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
import java.util.Locale
import scala.jdk.CollectionConverters._

class StableMergeSortOpDesc extends LogicalOp {

  import StableMergeSortOpDesc._

  @JsonProperty(value = "keys", required = true)
  @JsonSchemaTitle("Sort Keys")
  @JsonPropertyDescription("List of attributes to sort by with ordering preferences.")
  var keys: util.List[StableSortKey] = new util.ArrayList[StableSortKey]()

  /**
   * Validate operator parameters during compilation (schema propagation phase in the framework).
   */
  private def validate(): Unit = {
    require(keys != null && !keys.isEmpty, "StableMergeSort requires at least one sort key.")
    keys.asScala.foreach { k =>
      val order = Option(k.order).map(_.toLowerCase(Locale.ROOT)).getOrElse("asc")
      require(order == "asc" || order == "desc", s"Unsupported sort order '$order'.")
      val nulls = Option(k.nulls).map(_.toLowerCase(Locale.ROOT)).getOrElse("last")
      require(nulls == "first" || nulls == "last", s"Unsupported nulls placement '$nulls'.")
    }
  }

  override def getPhysicalOp(
                              workflowId: WorkflowIdentity,
                              executionId: ExecutionIdentity
                            ): PhysicalOp = {
    validate() // compile-time validation
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
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Stable Merge Sort",
      "Stable per-partition sort with multi-key ordering (incremental run-stack merge).",
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
  }
}