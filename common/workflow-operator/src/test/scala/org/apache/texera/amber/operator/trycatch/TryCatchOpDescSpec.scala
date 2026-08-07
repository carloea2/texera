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

package org.apache.texera.amber.operator.trycatch

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TryCatchOpDescSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)

  "TryCatchOpDesc" should "declare one Data input and Try/Catch/Error Info outputs" in {
    val desc = new TryCatchOpDesc()
    val info = desc.operatorInfo
    info.inputPorts.map(_.displayName) shouldBe List("Data")
    info.outputPorts.map(_.displayName) shouldBe List("Try", "Catch", "Error Info")
    info.outputPorts.map(_.id) shouldBe List(PortIdentity(), PortIdentity(1), PortIdentity(2))
  }

  it should "expand into splitter and gate connected by an internal snapshot link" in {
    val desc = new TryCatchOpDesc()
    val plan = desc.getPhysicalPlan(WorkflowIdentity(1L), ExecutionIdentity(1L))
    plan.operators.map(_.id.layerName) shouldBe Set(
      TryCatchOpDesc.SPLITTER_LAYER,
      TryCatchOpDesc.GATE_LAYER
    )
    plan.links should have size 1
    val link = plan.links.head
    link.fromPortId shouldBe TryCatchOpDesc.SNAPSHOT_OUT
    link.toPortId shouldBe TryCatchOpDesc.SNAPSHOT_IN
    link.fromPortId.internal shouldBe true
    link.toPortId.internal shouldBe true
  }

  it should "propagate the input schema to Try/Catch and the fixed schema to Error Info" in {
    val desc = new TryCatchOpDesc()
    val outputSchemas = desc.getExternalOutputSchemas(Map(PortIdentity() -> schema))
    outputSchemas shouldBe Map(
      PortIdentity() -> schema,
      PortIdentity(1) -> schema,
      PortIdentity(2) -> TryCatchOpDesc.ERROR_INFO_SCHEMA
    )
  }

  "FinallyOpDesc" should "declare Try Result and Catch Result output ports" in {
    val desc = new FinallyOpDesc()
    val info = desc.operatorInfo
    info.outputPorts.map(_.displayName) shouldBe List("Try Result", "Catch Result")
    info.outputPorts.map(_.id) shouldBe
      List(FinallyOpDesc.TRY_RESULT, FinallyOpDesc.CATCH_RESULT)
  }

  it should "require matching schemas and emit the common schema on both result ports" in {
    val desc = new FinallyOpDesc()
    val out = desc.getExternalOutputSchemas(
      Map(PortIdentity() -> schema, PortIdentity(1) -> schema)
    )
    out shouldBe Map(
      FinallyOpDesc.TRY_RESULT -> schema,
      FinallyOpDesc.CATCH_RESULT -> schema
    )

    val otherSchema = Schema().add("other", AttributeType.STRING)
    assertThrows[Exception] {
      desc.getExternalOutputSchemas(
        Map(PortIdentity() -> schema, PortIdentity(1) -> otherSchema)
      )
    }
  }

  it should "adopt whatever schema the connected branches carry" in {
    // Finally does not impose a schema: it takes the one the user's two
    // branches actually produce (both must agree) and emits exactly that, so
    // the frame is transparent to whatever columns flow through it.
    val desc = new FinallyOpDesc()
    val wideSchema = Schema()
      .add("id", AttributeType.LONG)
      .add("name", AttributeType.STRING)
      .add("score", AttributeType.DOUBLE)
    desc.getExternalOutputSchemas(
      Map(PortIdentity() -> wideSchema, PortIdentity(1) -> wideSchema)
    ) shouldBe Map(
      FinallyOpDesc.TRY_RESULT -> wideSchema,
      FinallyOpDesc.CATCH_RESULT -> wideSchema
    )
  }

  it should "reject branches whose columns differ only in type or order" in {
    val desc = new FinallyOpDesc()
    val a = Schema().add("x", AttributeType.INTEGER).add("y", AttributeType.STRING)
    // same names, different type on x
    val differentType =
      Schema().add("x", AttributeType.LONG).add("y", AttributeType.STRING)
    // same attributes, different order
    val differentOrder =
      Schema().add("y", AttributeType.STRING).add("x", AttributeType.INTEGER)

    val typeError = intercept[Exception] {
      desc.getExternalOutputSchemas(Map(PortIdentity() -> a, PortIdentity(1) -> differentType))
    }
    typeError.getMessage should include("same schema")

    assertThrows[Exception] {
      desc.getExternalOutputSchemas(Map(PortIdentity() -> a, PortIdentity(1) -> differentOrder))
    }
  }

  "FinallyOpDesc" should "declare From Catch dependent on From Try" in {
    val desc = new FinallyOpDesc()
    val fromCatch = desc.operatorInfo.inputPorts.last
    fromCatch.dependencies shouldBe List(PortIdentity())
  }

  "TrySplitterOpExec" should "tee every tuple to the try and snapshot ports" in {
    val exec = new TrySplitterOpExec("")
    val tuple = org.apache.texera.amber.core.tuple.TupleLike(1).enforceSchema(schema)
    val out = exec.processTupleMultiPort(tuple, 0).toList
    out shouldBe List(
      (tuple, Some(PortIdentity())),
      (tuple, Some(TryCatchOpDesc.SNAPSHOT_OUT))
    )
  }
}
