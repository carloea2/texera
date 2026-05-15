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

package org.apache.texera.amber.operator.udf.cpp

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp.oneToOnePhysicalOp
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeNameList
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import javax.validation.constraints.Positive

/**
  * Experimental/local-only C++ UDF MVP. Users provide a small C++ operator class over typed
  * input columns, and the operator emits typed TupleLike rows matching the declared output schema.
  */
class CompiledCppUDFOpDesc extends MapOpDesc {
  @JsonProperty(
    required = true,
    defaultValue =
      "// Choose from the following C++ UDF templates.\n" +
        "// Input columns are typed texera::Value objects and can be read by position,\n" +
        "// e.g. tuple.get(0).as_double(), or by selected column name,\n" +
        "// e.g. tuple.get(\"name\").as_string().\n" +
        "// Return one texera::TupleLike per output row. Each TupleLike must contain\n" +
        "// exactly the fields declared in Output column(s).\n" +
        "\n" +
        "class ProcessTupleOperator : public texera::UDFOperator {\n" +
        "public:\n" +
        "    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {\n" +
        "        double age = tuple.get(0).as_double();\n" +
        "        double income = tuple.size() > 1 ? tuple.get(1).as_double() : tuple.get(0).as_double();\n" +
        "        double score = income / (age + 1.0);\n" +
        "        return { texera::TupleLike{ texera::Value::double_value(score) } };\n" +
        "    }\n" +
        "};\n" +
        "using TexeraUDFOperator = ProcessTupleOperator;\n" +
        "\n" +
        "// class ProcessBatchOperator : public texera::UDFOperator {\n" +
        "// public:\n" +
        "//     texera::BatchLike process_batch(const texera::Batch& batch, int port) override {\n" +
        "//         texera::BatchLike output;\n" +
        "//         output.reserve(batch.size());\n" +
        "//         for (const auto& tuple : batch) {\n" +
        "//             double age = tuple.get(0).as_double();\n" +
        "//             double income = tuple.size() > 1 ? tuple.get(1).as_double() : tuple.get(0).as_double();\n" +
        "//             output.push_back(texera::TupleLike{ texera::Value::double_value(income / (age + 1.0)) });\n" +
        "//         }\n" +
        "//         return output;\n" +
        "//     }\n" +
        "// };\n" +
        "// using TexeraUDFOperator = ProcessBatchOperator;\n" +
        "\n" +
        "// class ProcessTableOperator : public texera::UDFOperator {\n" +
        "// public:\n" +
        "//     texera::TableLike process_table(const texera::Table& table, int port) override {\n" +
        "//         texera::TableLike output;\n" +
        "//         output.reserve(table.size());\n" +
        "//         for (const auto& tuple : table) {\n" +
        "//             double age = tuple.get(0).as_double();\n" +
        "//             double income = tuple.size() > 1 ? tuple.get(1).as_double() : tuple.get(0).as_double();\n" +
        "//             output.push_back(texera::TupleLike{ texera::Value::double_value(income / (age + 1.0)) });\n" +
        "//         }\n" +
        "//         return output;\n" +
        "//     }\n" +
        "// };\n" +
        "// using TexeraUDFOperator = ProcessTableOperator;\n"
  )
  @JsonSchemaTitle("C++ script")
  @JsonPropertyDescription("Input your C++ UDF operator code here")
  var code: String = CompiledCppUDFOpDesc.DefaultCppCode

  @JsonProperty(value = "inputColumns", required = false)
  @JsonSchemaTitle("Input columns")
  @JsonPropertyDescription(
    "Input columns exposed to tuple, batch, and table APIs in order. Leave empty to expose every supported input column."
  )
  @AutofillAttributeNameList
  var inputColumns: List[String] = List()

  @JsonProperty(required = true, defaultValue = "true")
  @JsonSchemaTitle("Retain input columns")
  @JsonPropertyDescription("Keep the original input columns? Requires one C++ output row per input row.")
  var retainInputColumns: Boolean = Boolean.box(true)

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel C++ workers to launch")
  @Positive(message = "Need at least 1 worker.")
  var workers: Int = Int.box(1)

  @JsonProperty
  @JsonSchemaTitle("Extra output column(s)")
  @JsonPropertyDescription(
    "Name and type of the newly added output columns that the C++ UDF will produce, if any"
  )
  var outputColumns: List[Attribute] =
    List(new Attribute("score", AttributeType.DOUBLE))

  @JsonProperty(value = "compilerFlags", required = false, defaultValue = "-O3")
  @JsonSchemaTitle("Compiler flags")
  @JsonPropertyDescription("Compiler flags passed to g++ or $CXX")
  var compilerFlags: String = CompiledCppUDFOpDesc.DefaultCompilerFlags

  @JsonProperty(value = "executionMode", required = false, defaultValue = "batch")
  @JsonSchemaTitle("Execution API")
  @JsonPropertyDescription("How Texera sends rows to the compiled UDF: tuple, batch, or table")
  @JsonSchemaInject(json = """{"enum": ["batch", "tuple", "table"]}""")
  var executionMode: String = CompiledCppUDFOpDesc.BatchMode

  @JsonProperty(value = "batchSize", required = false, defaultValue = "64")
  @JsonSchemaTitle("Batch size")
  @JsonPropertyDescription("Number of tuples to send to each compiled C++ subprocess in batch mode")
  @Positive(message = "Batch size must be positive")
  var batchSize: Int = CompiledCppUDFOpDesc.DefaultBatchSize

  @JsonProperty(value = "timeoutMs", required = false, defaultValue = "5000")
  @JsonSchemaTitle("Timeout (ms)")
  @JsonPropertyDescription("Compile and per-subprocess execution timeout in milliseconds")
  @Positive(message = "Timeout must be positive")
  var timeoutMs: Int = CompiledCppUDFOpDesc.DefaultTimeoutMs

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    validateBasicConfig()
    val physicalOp =
      if (workers > 1) {
        oneToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecWithClassName(
            "org.apache.texera.amber.operator.udf.cpp.CompiledCppUDFOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
          .withParallelizable(true)
          .withSuggestedWorkerNum(workers)
      } else {
        PhysicalOp.manyToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecWithClassName(
            "org.apache.texera.amber.operator.udf.cpp.CompiledCppUDFOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
          .withParallelizable(false)
      }

    physicalOp
      .withIsOneToManyOp(true)
      .withDerivePartition(_ => UnknownPartition())
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        val inputSchema = inputSchemas(operatorInfo.inputPorts.head.id)
        Map(operatorInfo.outputPorts.head.id -> validateAndDeriveOutputSchema(inputSchema))
      }))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Compiled C++ UDF",
      "Experimental/local only C++ UDF with typed tuple, batch, and table APIs",
      OperatorGroupConstants.CPP_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  private[cpp] def compileRequest: CompiledCppUDFCompileRequest =
    CompiledCppUDFCompileRequest(
      code = Option(this.code).getOrElse(""),
      inputColumns = Option(inputColumns).getOrElse(List.empty),
      retainInputColumns = retainInputColumns,
      outputColumns = Option(outputColumns)
        .getOrElse(List.empty)
        .map(attribute => s"${attribute.getName}:${attribute.getType}"),
      compilerFlags = normalizedCompilerFlags,
      timeoutMs = timeoutMs
    )

  private[cpp] def normalizedBatchSize: Int =
    if (batchSize > 0) batchSize else CompiledCppUDFOpDesc.DefaultBatchSize

  private[cpp] def validateAndDeriveOutputSchema(inputSchema: Schema): Schema = {
    validateBasicConfig()

    inputColumns.foreach { column =>
      if (!inputSchema.containsAttribute(column)) {
        throw new RuntimeException(s"Column '$column' does not exist in the input schema.")
      }
    }

    selectedInputAttributes(inputSchema).foreach { attribute =>
      if (attribute.getType == AttributeType.LARGE_BINARY) {
        throw new RuntimeException(
          s"Compiled C++ UDF does not support large_binary columns yet. Column '${attribute.getName}' has type ${attribute.getType}."
        )
      }
    }

    val extraOutputColumns = Option(outputColumns).getOrElse(List.empty)

    extraOutputColumns.foreach { column =>
      if (column.getType == AttributeType.LARGE_BINARY) {
        throw new RuntimeException(
          s"Compiled C++ UDF does not support large_binary output columns yet. Column '${column.getName}' has type ${column.getType}."
        )
      }
      if (retainInputColumns && inputSchema.containsAttribute(column.getName)) {
        throw new RuntimeException(
          s"Column name ${column.getName} already exists!"
        )
      }
    }

    val outputSchema = if (retainInputColumns) inputSchema else Schema()
    outputSchema.add(extraOutputColumns)
  }

  private[cpp] def validateBasicConfig(): Unit = {
    Option(outputColumns).getOrElse(List.empty).foreach { outputColumn =>
      require(
        outputColumn.getName != null && outputColumn.getName.trim.nonEmpty,
        "Output column names must not be empty"
      )
      require(outputColumn.getType != null, s"Output column ${outputColumn.getName} type is required")
    }
    require(code != null && code.trim.nonEmpty, "C++ code must not be empty")
    require(
      code.contains("TexeraUDFOperator"),
      "C++ code must define a TexeraUDFOperator alias"
    )
    require(
      CompiledCppUDFOpDesc.ExecutionModes.contains(normalizedExecutionMode),
      s"Execution API must be one of: ${CompiledCppUDFOpDesc.ExecutionModes.mkString(", ")}"
    )
    require(workers >= 1, "Need at least 1 worker.")
    require(batchSize > 0, "Batch size must be positive")
    require(timeoutMs > 0, "Timeout must be positive")
  }

  private[cpp] def normalizedExecutionMode: String =
    Option(executionMode)
      .map(_.trim.toLowerCase)
      .filter(_.nonEmpty)
      .getOrElse(CompiledCppUDFOpDesc.BatchMode)

  private[cpp] def selectedInputAttributes(inputSchema: Schema): List[Attribute] =
    Option(inputColumns)
      .getOrElse(List.empty)
      .filter(column => column != null && column.trim.nonEmpty) match {
      case Nil =>
        inputSchema.getAttributes
          .filter(attribute => attribute.getType != AttributeType.LARGE_BINARY)
          .toList
      case selectedColumns =>
        selectedColumns.map(column => inputSchema.getAttribute(column))
    }

  private def normalizedCompilerFlags: String =
    Option(compilerFlags)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(
        CompiledCppUDFOpDesc.DefaultCompilerFlags
      )
}

object CompiledCppUDFOpDesc {
  final val DefaultCppCode: String =
    "// Choose from the following C++ UDF templates.\n" +
      "// Input columns are typed texera::Value objects and can be read by position,\n" +
      "// e.g. tuple.get(0).as_double(), or by selected column name,\n" +
      "// e.g. tuple.get(\"name\").as_string().\n" +
      "// Return one texera::TupleLike per output row. Each TupleLike must contain\n" +
      "// exactly the fields declared in Output column(s).\n" +
      "\n" +
      "class ProcessTupleOperator : public texera::UDFOperator {\n" +
      "public:\n" +
      "    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {\n" +
      "        double age = tuple.get(0).as_double();\n" +
      "        double income = tuple.size() > 1 ? tuple.get(1).as_double() : tuple.get(0).as_double();\n" +
      "        double score = income / (age + 1.0);\n" +
      "        return { texera::TupleLike{ texera::Value::double_value(score) } };\n" +
      "    }\n" +
      "};\n" +
      "using TexeraUDFOperator = ProcessTupleOperator;\n" +
      "\n" +
      "// class ProcessBatchOperator : public texera::UDFOperator {\n" +
      "// public:\n" +
      "//     texera::BatchLike process_batch(const texera::Batch& batch, int port) override {\n" +
      "//         texera::BatchLike output;\n" +
      "//         output.reserve(batch.size());\n" +
      "//         for (const auto& tuple : batch) {\n" +
      "//             double age = tuple.get(0).as_double();\n" +
      "//             double income = tuple.size() > 1 ? tuple.get(1).as_double() : tuple.get(0).as_double();\n" +
      "//             output.push_back(texera::TupleLike{ texera::Value::double_value(income / (age + 1.0)) });\n" +
      "//         }\n" +
      "//         return output;\n" +
      "//     }\n" +
      "// };\n" +
      "// using TexeraUDFOperator = ProcessBatchOperator;\n" +
      "\n" +
      "// class ProcessTableOperator : public texera::UDFOperator {\n" +
      "// public:\n" +
      "//     texera::TableLike process_table(const texera::Table& table, int port) override {\n" +
      "//         texera::TableLike output;\n" +
      "//         output.reserve(table.size());\n" +
      "//         for (const auto& tuple : table) {\n" +
      "//             double age = tuple.get(0).as_double();\n" +
      "//             double income = tuple.size() > 1 ? tuple.get(1).as_double() : tuple.get(0).as_double();\n" +
      "//             output.push_back(texera::TupleLike{ texera::Value::double_value(income / (age + 1.0)) });\n" +
      "//         }\n" +
      "//         return output;\n" +
      "//     }\n" +
      "// };\n" +
      "// using TexeraUDFOperator = ProcessTableOperator;\n"
  val DefaultCompilerFlags = "-O3"
  val TupleMode = "tuple"
  val BatchMode = "batch"
  val TableMode = "table"
  val ExecutionModes: Set[String] = Set(TupleMode, BatchMode, TableMode)
  val DefaultBatchSize = 64
  val DefaultTimeoutMs = 5000
}
