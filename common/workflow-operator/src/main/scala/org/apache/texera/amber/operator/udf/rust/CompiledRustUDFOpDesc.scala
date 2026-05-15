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

package org.apache.texera.amber.operator.udf.rust

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
  * Experimental/local-only Rust UDF MVP. Users provide a small Rust operator over typed
  * input columns, and the operator emits typed TupleLike rows matching the declared output schema.
  */
class CompiledRustUDFOpDesc extends MapOpDesc {
  @JsonProperty(
    required = true,
    defaultValue =
      "// Choose from the following Rust UDF templates.\n" +
        "// Input columns are typed texera::Value objects and can be read by position,\n" +
        "// e.g. tuple.get(0)?.as_double()?, or by selected column name,\n" +
        "// e.g. tuple.get_by_name(\"name\")?.as_string()?.\n" +
        "// Return one texera::TupleLike per output row. Each TupleLike must contain\n" +
        "// exactly the fields declared in Output column(s).\n" +
        "\n" +
        "#[derive(Default)]\n" +
        "struct ProcessTupleOperator;\n" +
        "\n" +
        "impl texera::UDFOperator for ProcessTupleOperator {\n" +
        "    fn process_tuple(&mut self, tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {\n" +
        "        let age = tuple.get(0)?.as_double()?;\n" +
        "        let income = if tuple.size() > 1 { tuple.get(1)?.as_double()? } else { age };\n" +
        "        let score = income / (age + 1.0);\n" +
        "        Ok(vec![vec![texera::Value::double_value(score)]])\n" +
        "    }\n" +
        "}\n" +
        "type TexeraUDFOperator = ProcessTupleOperator;\n" +
        "\n" +
        "// #[derive(Default)]\n" +
        "// struct ProcessBatchOperator;\n" +
        "//\n" +
        "// impl texera::UDFOperator for ProcessBatchOperator {\n" +
        "//     fn process_batch(&mut self, batch: &texera::Batch, _port: i32) -> Result<texera::BatchLike, String> {\n" +
        "//         let mut output = Vec::with_capacity(batch.len());\n" +
        "//         for tuple in batch {\n" +
        "//             let age = tuple.get(0)?.as_double()?;\n" +
        "//             let income = if tuple.size() > 1 { tuple.get(1)?.as_double()? } else { age };\n" +
        "//             output.push(vec![texera::Value::double_value(income / (age + 1.0))]);\n" +
        "//         }\n" +
        "//         Ok(output)\n" +
        "//     }\n" +
        "// }\n" +
        "// type TexeraUDFOperator = ProcessBatchOperator;\n" +
        "\n" +
        "// #[derive(Default)]\n" +
        "// struct ProcessTableOperator;\n" +
        "//\n" +
        "// impl texera::UDFOperator for ProcessTableOperator {\n" +
        "//     fn process_table(&mut self, table: &texera::Table, _port: i32) -> Result<texera::TableLike, String> {\n" +
        "//         let mut output = Vec::with_capacity(table.len());\n" +
        "//         for tuple in table {\n" +
        "//             let age = tuple.get(0)?.as_double()?;\n" +
        "//             let income = if tuple.size() > 1 { tuple.get(1)?.as_double()? } else { age };\n" +
        "//             output.push(vec![texera::Value::double_value(income / (age + 1.0))]);\n" +
        "//         }\n" +
        "//         Ok(output)\n" +
        "//     }\n" +
        "// }\n" +
        "// type TexeraUDFOperator = ProcessTableOperator;\n"
  )
  @JsonSchemaTitle("Rust script")
  @JsonPropertyDescription("Input your Rust UDF operator code here")
  var code: String = CompiledRustUDFOpDesc.DefaultRustCode

  @JsonProperty(value = "inputColumns", required = false)
  @JsonSchemaTitle("Input columns")
  @JsonPropertyDescription(
    "Input columns exposed to tuple, batch, and table APIs in order. Leave empty to expose every supported input column."
  )
  @AutofillAttributeNameList
  var inputColumns: List[String] = List()

  @JsonProperty(required = true, defaultValue = "true")
  @JsonSchemaTitle("Retain input columns")
  @JsonPropertyDescription("Keep the original input columns? Requires one Rust output row per input row.")
  var retainInputColumns: Boolean = Boolean.box(true)

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel Rust workers to launch")
  @Positive(message = "Need at least 1 worker.")
  var workers: Int = Int.box(1)

  @JsonProperty
  @JsonSchemaTitle("Extra output column(s)")
  @JsonPropertyDescription(
    "Name and type of the newly added output columns that the Rust UDF will produce, if any"
  )
  var outputColumns: List[Attribute] =
    List(new Attribute("score", AttributeType.DOUBLE))

  @JsonProperty(value = "compilerFlags", required = false, defaultValue = "-O")
  @JsonSchemaTitle("Compiler flags")
  @JsonPropertyDescription("Compiler flags passed to rustc or $RUSTC")
  var compilerFlags: String = CompiledRustUDFOpDesc.DefaultCompilerFlags

  @JsonProperty(value = "executionMode", required = false, defaultValue = "batch")
  @JsonSchemaTitle("Execution API")
  @JsonPropertyDescription("How Texera sends rows to the compiled UDF: tuple, batch, or table")
  @JsonSchemaInject(json = """{"enum": ["batch", "tuple", "table"]}""")
  var executionMode: String = CompiledRustUDFOpDesc.BatchMode

  @JsonProperty(value = "batchSize", required = false, defaultValue = "64")
  @JsonSchemaTitle("Batch size")
  @JsonPropertyDescription("Number of tuples to send to each compiled Rust subprocess in batch mode")
  @Positive(message = "Batch size must be positive")
  var batchSize: Int = CompiledRustUDFOpDesc.DefaultBatchSize

  @JsonProperty(value = "timeoutMs", required = false, defaultValue = "5000")
  @JsonSchemaTitle("Timeout (ms)")
  @JsonPropertyDescription("Compile and per-subprocess execution timeout in milliseconds")
  @Positive(message = "Timeout must be positive")
  var timeoutMs: Int = CompiledRustUDFOpDesc.DefaultTimeoutMs

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
            "org.apache.texera.amber.operator.udf.rust.CompiledRustUDFOpExec",
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
            "org.apache.texera.amber.operator.udf.rust.CompiledRustUDFOpExec",
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
      "Compiled Rust UDF",
      "Experimental/local only Rust UDF with typed tuple, batch, and table APIs",
      OperatorGroupConstants.RUST_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  private[rust] def compileRequest: CompiledRustUDFCompileRequest =
    CompiledRustUDFCompileRequest(
      code = Option(this.code).getOrElse(""),
      inputColumns = Option(inputColumns).getOrElse(List.empty),
      retainInputColumns = retainInputColumns,
      outputColumns = Option(outputColumns)
        .getOrElse(List.empty)
        .map(attribute => s"${attribute.getName}:${attribute.getType}"),
      compilerFlags = normalizedCompilerFlags,
      timeoutMs = timeoutMs
    )

  private[rust] def normalizedBatchSize: Int =
    if (batchSize > 0) batchSize else CompiledRustUDFOpDesc.DefaultBatchSize

  private[rust] def validateAndDeriveOutputSchema(inputSchema: Schema): Schema = {
    validateBasicConfig()

    inputColumns.foreach { column =>
      if (!inputSchema.containsAttribute(column)) {
        throw new RuntimeException(s"Column '$column' does not exist in the input schema.")
      }
    }

    selectedInputAttributes(inputSchema).foreach { attribute =>
      if (attribute.getType == AttributeType.LARGE_BINARY) {
        throw new RuntimeException(
          s"Compiled Rust UDF does not support large_binary columns yet. Column '${attribute.getName}' has type ${attribute.getType}."
        )
      }
    }

    val extraOutputColumns = Option(outputColumns).getOrElse(List.empty)

    extraOutputColumns.foreach { column =>
      if (column.getType == AttributeType.LARGE_BINARY) {
        throw new RuntimeException(
          s"Compiled Rust UDF does not support large_binary output columns yet. Column '${column.getName}' has type ${column.getType}."
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

  private[rust] def validateBasicConfig(): Unit = {
    Option(outputColumns).getOrElse(List.empty).foreach { outputColumn =>
      require(
        outputColumn.getName != null && outputColumn.getName.trim.nonEmpty,
        "Output column names must not be empty"
      )
      require(outputColumn.getType != null, s"Output column ${outputColumn.getName} type is required")
    }
    require(code != null && code.trim.nonEmpty, "Rust code must not be empty")
    require(
      code.contains("TexeraUDFOperator"),
      "Rust code must define a TexeraUDFOperator type alias"
    )
    require(
      CompiledRustUDFOpDesc.ExecutionModes.contains(normalizedExecutionMode),
      s"Execution API must be one of: ${CompiledRustUDFOpDesc.ExecutionModes.mkString(", ")}"
    )
    require(workers >= 1, "Need at least 1 worker.")
    require(batchSize > 0, "Batch size must be positive")
    require(timeoutMs > 0, "Timeout must be positive")
  }

  private[rust] def normalizedExecutionMode: String =
    Option(executionMode)
      .map(_.trim.toLowerCase)
      .filter(_.nonEmpty)
      .getOrElse(CompiledRustUDFOpDesc.BatchMode)

  private[rust] def selectedInputAttributes(inputSchema: Schema): List[Attribute] =
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
        CompiledRustUDFOpDesc.DefaultCompilerFlags
      )
}

object CompiledRustUDFOpDesc {
  final val DefaultRustCode: String =
    "// Choose from the following Rust UDF templates.\n" +
      "// Input columns are typed texera::Value objects and can be read by position,\n" +
      "// e.g. tuple.get(0)?.as_double()?, or by selected column name,\n" +
      "// e.g. tuple.get_by_name(\"name\")?.as_string()?.\n" +
      "// Return one texera::TupleLike per output row. Each TupleLike must contain\n" +
      "// exactly the fields declared in Output column(s).\n" +
      "\n" +
      "#[derive(Default)]\n" +
      "struct ProcessTupleOperator;\n" +
      "\n" +
      "impl texera::UDFOperator for ProcessTupleOperator {\n" +
      "    fn process_tuple(&mut self, tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {\n" +
      "        let age = tuple.get(0)?.as_double()?;\n" +
      "        let income = if tuple.size() > 1 { tuple.get(1)?.as_double()? } else { age };\n" +
      "        let score = income / (age + 1.0);\n" +
      "        Ok(vec![vec![texera::Value::double_value(score)]])\n" +
      "    }\n" +
      "}\n" +
      "type TexeraUDFOperator = ProcessTupleOperator;\n" +
      "\n" +
      "// #[derive(Default)]\n" +
      "// struct ProcessBatchOperator;\n" +
      "//\n" +
      "// impl texera::UDFOperator for ProcessBatchOperator {\n" +
      "//     fn process_batch(&mut self, batch: &texera::Batch, _port: i32) -> Result<texera::BatchLike, String> {\n" +
      "//         let mut output = Vec::with_capacity(batch.len());\n" +
      "//         for tuple in batch {\n" +
      "//             let age = tuple.get(0)?.as_double()?;\n" +
      "//             let income = if tuple.size() > 1 { tuple.get(1)?.as_double()? } else { age };\n" +
      "//             output.push(vec![texera::Value::double_value(income / (age + 1.0))]);\n" +
      "//         }\n" +
      "//         Ok(output)\n" +
      "//     }\n" +
      "// }\n" +
      "// type TexeraUDFOperator = ProcessBatchOperator;\n" +
      "\n" +
      "// #[derive(Default)]\n" +
      "// struct ProcessTableOperator;\n" +
      "//\n" +
      "// impl texera::UDFOperator for ProcessTableOperator {\n" +
      "//     fn process_table(&mut self, table: &texera::Table, _port: i32) -> Result<texera::TableLike, String> {\n" +
      "//         let mut output = Vec::with_capacity(table.len());\n" +
      "//         for tuple in table {\n" +
      "//             let age = tuple.get(0)?.as_double()?;\n" +
      "//             let income = if tuple.size() > 1 { tuple.get(1)?.as_double()? } else { age };\n" +
      "//             output.push(vec![texera::Value::double_value(income / (age + 1.0))]);\n" +
      "//         }\n" +
      "//         Ok(output)\n" +
      "//     }\n" +
      "// }\n" +
      "// type TexeraUDFOperator = ProcessTableOperator;\n"
  val DefaultCompilerFlags = "-O"
  val TupleMode = "tuple"
  val BatchMode = "batch"
  val TableMode = "table"
  val ExecutionModes: Set[String] = Set(TupleMode, BatchMode, TableMode)
  val DefaultBatchSize = 64
  val DefaultTimeoutMs = 5000
}
