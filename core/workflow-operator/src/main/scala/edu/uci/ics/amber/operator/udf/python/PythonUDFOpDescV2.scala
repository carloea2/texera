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

package edu.uci.ics.amber.operator.udf.python

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.{LogicalOp, PortDescription, StateTransferFunc}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.{Success, Try}
/** One entry of the annotation map. */
case class OutSpec(
                    retainInput:      Boolean               = false,
                    input:            Option[Int]           = None,          // 0-based input-port index
                    newColumns:       Map[String, String]   = Map.empty      // name → type
                  )

class PythonUDFOpDescV2 extends LogicalOp {
  @JsonProperty(
    required = true,
    defaultValue =
      "# Choose from the following templates:\n" +
        "# \n" +
        "# from pytexera import *\n" +
        "# \n" +
        "# class ProcessTupleOperator(UDFOperatorV2):\n" +
        "#     \n" +
        "#     @overrides\n" +
        "#     def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:\n" +
        "#         yield tuple_\n" +
        "# \n" +
        "# class ProcessBatchOperator(UDFBatchOperator):\n" +
        "#     BATCH_SIZE = 10 # must be a positive integer\n" +
        "# \n" +
        "#     @overrides\n" +
        "#     def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:\n" +
        "#         yield batch\n" +
        "# \n" +
        "# class ProcessTableOperator(UDFTableOperator):\n" +
        "# \n" +
        "#     @overrides\n" +
        "#     def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n" +
        "#         yield table\n"
  )
  @JsonSchemaTitle("Python script")
  @JsonPropertyDescription("Input your code here")
  var code: String = ""

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to lunch")
  var workers: Int = Int.box(1)


  @JsonProperty(required = true, defaultValue = "")
  @JsonSchemaTitle("Output schema annotation")
  @JsonPropertyDescription("Annotate output schema with this JSON string")
  var schemaAnnotation: String = ""

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    Preconditions.checkArgument(workers >= 1, "Need at least 1 worker.", Array())
    val opInfo = this.operatorInfo
    val partitionRequirement: List[Option[PartitionInfo]] = if (inputPorts != null) {
      inputPorts.map(p => Option(p.partitionRequirement))
    } else {
      opInfo.inputPorts.map(_ => None)
    }



    /** Parses `schemaAnnotation` into a Map[portId → OutSpec]. */
    lazy val annotation: Map[String, OutSpec] =
      if (schemaAnnotation.trim.isEmpty) Map.empty
      else {
        val tRef = new TypeReference[java.util.Map[String, OutSpec]](){}
        objectMapper
          .readValue(schemaAnnotation, tRef)
          .asScala
          .toMap         // convert to Scala Map
      }


    val propagateSchema: Map[PortIdentity, Schema] => Map[PortIdentity, Schema] =
      inputSchemas => {

        // Convenience: index input schemas by *position* (0,1,2,…)
        val inputByPos: Array[Schema] =
          operatorInfo.inputPorts
            .map(_.id)
            .zipWithIndex
            .map { case (pid, idx) => idx -> inputSchemas(pid) }
            .toArray
            .sortBy(_._1)
            .map(_._2)

        val outputs: Seq[(PortIdentity, Schema)] =
          operatorInfo.outputPorts.map { outPort =>

            val spec  = annotation.getOrElse(outPort.id.id.toString,
              throw new RuntimeException(
                s"No schema-annotation for output port '${outPort.id.id}'"))

            // ── 1. start with either an empty schema or a retained input schema ──
            val base: Schema =
              if (spec.retainInput) {
                val idx = spec.input.getOrElse(0)
                if (idx >= inputByPos.length)
                  throw new RuntimeException(s"Input index $idx out of range")
                inputByPos(idx)
              } else Schema()

            // ── 2. append new columns (duplicate-name check) ─────────────────────
            outPort.id -> base.add(Schema.fromRawSchema(spec.newColumns))
          }

        Map.from(outputs)

    }

    val physicalOp = if (workers > 1) {
      PhysicalOp
        .oneToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecWithCode(code, "python")
        )
        .withParallelizable(true)
        .withSuggestedWorkerNum(workers)
    } else {
      PhysicalOp
        .manyToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecWithCode(code, "python")
        )
        .withParallelizable(false)
    }

    physicalOp
      .withDerivePartition(_ => UnknownPartition())
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(partitionRequirement)
      .withIsOneToManyOp(true)
      .withPropagateSchema(SchemaPropagationFunc(propagateSchema))
  }

  override def operatorInfo: OperatorInfo = {
    val inputPortInfo = if (inputPorts != null) {
      inputPorts.zipWithIndex.map {
        case (portDesc: PortDescription, idx) =>
          InputPort(
            PortIdentity(idx),
            displayName = portDesc.displayName,
            allowMultiLinks = portDesc.allowMultiInputs,
            dependencies = portDesc.dependencies.map(idx => PortIdentity(idx))
          )
      }
    } else {
      List(InputPort(PortIdentity(), allowMultiLinks = true))
    }
    val outputPortInfo = if (outputPorts != null) {
      outputPorts.zipWithIndex.map {
        case (portDesc, idx) => OutputPort(PortIdentity(idx), displayName = portDesc.displayName)
      }
    } else {
      List(OutputPort())
    }

    OperatorInfo(
      "Python UDF",
      "User-defined function operator in Python script",
      OperatorGroupConstants.PYTHON_GROUP,
      inputPortInfo,
      outputPortInfo,
      dynamicInputPorts = true,
      dynamicOutputPorts = true,
      supportReconfiguration = true,
      allowPortCustomization = true
    )
  }
  override def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      oldLogicalOp: LogicalOp,
      newLogicalOp: LogicalOp
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    Success(newLogicalOp.getPhysicalOp(workflowId, executionId), None)
  }
}
