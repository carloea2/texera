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

import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.sql.Timestamp

class CompiledRustUDFOpDescSpec extends AnyFlatSpec with Matchers {
  private val validCode =
    """#[derive(Default)]
      |struct ProcessTupleOperator;
      |
      |impl texera::UDFOperator for ProcessTupleOperator {
      |    fn process_tuple(&mut self, tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {
      |        let age = tuple.get_by_name("age")?.as_double()?;
      |        let income = tuple.get_by_name("income")?.as_double()?;
      |        let score = income / (age + 1.0);
      |        Ok(vec![vec![texera::Value::double_value(score)]])
      |    }
      |}
      |type TexeraUDFOperator = ProcessTupleOperator;""".stripMargin

  private val batchCode =
    """#[derive(Default)]
      |struct ProcessBatchOperator;
      |
      |impl texera::UDFOperator for ProcessBatchOperator {
      |    fn process_batch(&mut self, batch: &texera::Batch, _port: i32) -> Result<texera::BatchLike, String> {
      |        let mut output = Vec::with_capacity(batch.len());
      |        for tuple in batch {
      |            output.push(vec![texera::Value::double_value(
      |                tuple.get_by_name("age")?.as_double()? + tuple.get_by_name("income")?.as_double()?
      |            )]);
      |        }
      |        Ok(output)
      |    }
      |}
      |type TexeraUDFOperator = ProcessBatchOperator;""".stripMargin

  private val tableCode =
    """#[derive(Default)]
      |struct ProcessTableOperator;
      |
      |impl texera::UDFOperator for ProcessTableOperator {
      |    fn process_table(&mut self, table: &texera::Table, _port: i32) -> Result<texera::TableLike, String> {
      |        let mut output = Vec::with_capacity(table.len());
      |        let mut row_number = 1.0;
      |        for tuple in table {
      |            output.push(vec![texera::Value::double_value(
      |                tuple.get_by_name("income")?.as_double()? + row_number
      |            )]);
      |            row_number += 1.0;
      |        }
      |        Ok(output)
      |    }
      |}
      |type TexeraUDFOperator = ProcessTableOperator;""".stripMargin

  private val mixedTypeCode =
    """#[derive(Default)]
      |struct MixedTypeOperator;
      |
      |impl texera::UDFOperator for MixedTypeOperator {
      |    fn process_tuple(&mut self, tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {
      |        let active = if tuple.get_by_name("active")?.as_bool()? { "active" } else { "inactive" };
      |        let summary = format!(
      |            "{}:{}:{}:{}",
      |            tuple.get_by_name("name")?.as_string()?,
      |            active,
      |            tuple.get_by_name("created")?.as_timestamp_millis()?,
      |            tuple.get_by_name("payload")?.as_binary()?.len()
      |        );
      |        Ok(vec![vec![texera::Value::string_value(summary)]])
      |    }
      |}
      |type TexeraUDFOperator = MixedTypeOperator;""".stripMargin

  private val binaryCode =
    """#[derive(Default)]
      |struct BinaryOperator;
      |
      |impl texera::UDFOperator for BinaryOperator {
      |    fn process_tuple(&mut self, tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {
      |        let mut payload = tuple.get_by_name("payload")?.as_binary()?.to_vec();
      |        payload.push(b'!');
      |        Ok(vec![vec![texera::Value::binary_value(payload)]])
      |    }
      |}
      |type TexeraUDFOperator = BinaryOperator;""".stripMargin

  private val retainOnlyCode =
    """#[derive(Default)]
      |struct RetainOnlyOperator;
      |
      |impl texera::UDFOperator for RetainOnlyOperator {
      |    fn process_tuple(&mut self, _tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {
      |        Ok(vec![vec![]])
      |    }
      |}
      |type TexeraUDFOperator = RetainOnlyOperator;""".stripMargin

  private val statefulCode =
    """#[derive(Default)]
      |struct StatefulOperator {
      |    seen: i32,
      |}
      |
      |impl texera::UDFOperator for StatefulOperator {
      |    fn process_tuple(&mut self, _tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {
      |        self.seen += 1;
      |        Ok(vec![vec![texera::Value::double_value(self.seen as f64)]])
      |    }
      |}
      |type TexeraUDFOperator = StatefulOperator;""".stripMargin

  private def compileRequest(
      source: String = validCode,
      columns: List[String] = List("age", "income")
  ): CompiledRustUDFCompileRequest =
    CompiledRustUDFCompileRequest(
      code = source,
      inputColumns = columns,
      retainInputColumns = true,
      outputColumns = List("score:double"),
      compilerFlags = "-O",
      timeoutMs = 5000
    )

  private def configuredDesc: CompiledRustUDFOpDesc = {
    val desc = new CompiledRustUDFOpDesc
    desc.code = validCode
    desc.inputColumns = List("age", "income")
    desc.retainInputColumns = true
    desc.outputColumns = List(new Attribute("score", AttributeType.DOUBLE))
    desc.executionMode = CompiledRustUDFOpDesc.TupleMode
    desc
  }

  private def numericSchema: Schema =
    Schema()
      .add(new Attribute("age", AttributeType.INTEGER))
      .add(new Attribute("income", AttributeType.DOUBLE))

  private def tuple(age: Int, income: Double): Tuple =
    Tuple
      .builder(numericSchema)
      .add("age", AttributeType.INTEGER, Integer.valueOf(age))
      .add("income", AttributeType.DOUBLE, Double.box(income))
      .build()

  private def mixedSchema: Schema =
    Schema()
      .add(new Attribute("name", AttributeType.STRING))
      .add(new Attribute("active", AttributeType.BOOLEAN))
      .add(new Attribute("created", AttributeType.TIMESTAMP))
      .add(new Attribute("payload", AttributeType.BINARY))

  private def mixedTuple: Tuple =
    Tuple
      .builder(mixedSchema)
      .add("name", AttributeType.STRING, "carlo")
      .add("active", AttributeType.BOOLEAN, Boolean.box(true))
      .add("created", AttributeType.TIMESTAMP, new Timestamp(12345L))
      .add("payload", AttributeType.BINARY, Array[Byte](1, 2, 3))
      .build()

  private def enforceOutput(tupleLike: TupleLike, outputSchema: Schema): Tuple =
    tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema)

  "CompiledRustUDFCompiler" should "compile a valid tuple API operator" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val executable = CompiledRustUDFCompiler.compile(compileRequest())

    Files.exists(executable) shouldBe true
  }

  it should "compile the default template with one selected input column" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val executable = CompiledRustUDFCompiler.compile(
      compileRequest(CompiledRustUDFOpDesc.DefaultRustCode, List("line"))
    )

    Files.exists(executable) shouldBe true
  }

  it should "surface a readable compile error for invalid Rust" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val invalidCode =
      """#[derive(Default)]
        |struct ProcessTupleOperator;
        |
        |impl texera::UDFOperator for ProcessTupleOperator {
        |    fn process_tuple(&mut self, _tuple: &texera::Tuple, _port: i32) -> Result<texera::TupleOutput, String> {
        |        let value = ;
        |        Ok(vec![vec![texera::Value::double_value(value)]])
        |    }
        |}
        |type TexeraUDFOperator = ProcessTupleOperator;""".stripMargin
    val error = intercept[RuntimeException] {
      CompiledRustUDFCompiler.compile(compileRequest(invalidCode))
    }

    error.getMessage should include("Rust compilation failed:")
    error.getMessage should include("type TexeraUDFOperator = ProcessTupleOperator;")
  }

  "CompiledRustUDFOpExec" should "append a typed output column while retaining input fields" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(configuredDesc))
    exec.open()
    val outputTuple = exec
      .processTuple(tuple(20, 50000.0), 0)
      .next()

    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    enforceOutput(outputTuple, outputSchema).getField[Double]("score") shouldBe (50000.0 / 21.0 +- 0.000001)
  }

  it should "allow retaining input columns without declaring extra output columns" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = retainOnlyCode
    desc.outputColumns = List.empty
    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val output = enforceOutput(exec.processTuple(tuple(20, 50000.0), 0).next(), numericSchema)

    output.getField[Integer]("age") shouldBe Integer.valueOf(20)
    output.getField[Double]("income") shouldBe 50000.0
  }

  it should "expose every upstream input column when input columns are left empty" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.inputColumns = List.empty
    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    val output = enforceOutput(exec.processTuple(tuple(20, 50000.0), 0).next(), outputSchema)

    output.getField[Double]("score") shouldBe (50000.0 / 21.0 +- 0.000001)
  }

  it should "reuse the compiled worker process across tuple calls" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = statefulCode
    desc.outputColumns = List(new Attribute("seen", AttributeType.DOUBLE))
    desc.executionMode = CompiledRustUDFOpDesc.TupleMode
    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    try {
      val outputSchema = numericSchema.add(new Attribute("seen", AttributeType.DOUBLE))
      val first = enforceOutput(exec.processTuple(tuple(20, 50000.0), 0).next(), outputSchema)
      val second = enforceOutput(exec.processTuple(tuple(40, 90000.0), 0).next(), outputSchema)

      first.getField[Double]("seen") shouldBe 1.0
      second.getField[Double]("seen") shouldBe 2.0
    } finally {
      exec.close()
    }
  }

  it should "support the Rust process_batch API" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = batchCode
    desc.executionMode = CompiledRustUDFOpDesc.BatchMode
    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    exec.processTuple(tuple(20, 50000.0), 0).toList shouldBe empty
    exec.processTuple(tuple(40, 90000.0), 0).toList shouldBe empty
    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    val output = exec.onFinish(0).map(enforceOutput(_, outputSchema)).toList

    output.map(_.getField[Double]("score")) shouldBe List(50020.0, 90040.0)
  }

  it should "support the Rust process_table API" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = tableCode
    desc.executionMode = CompiledRustUDFOpDesc.TableMode
    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    exec.processTuple(tuple(20, 50000.0), 0).toList shouldBe empty
    exec.processTuple(tuple(40, 90000.0), 0).toList shouldBe empty
    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    val output = exec.onFinish(0).map(enforceOutput(_, outputSchema)).toList

    output.map(_.getField[Double]("score")) shouldBe List(50001.0, 90002.0)
  }

  it should "support string boolean timestamp and binary input with string output" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val desc = new CompiledRustUDFOpDesc
    desc.code = mixedTypeCode
    desc.inputColumns = List("name", "active", "created", "payload")
    desc.retainInputColumns = false
    desc.outputColumns = List(new Attribute("summary", AttributeType.STRING))
    desc.executionMode = CompiledRustUDFOpDesc.TupleMode
    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val outputSchema = Schema().add(new Attribute("summary", AttributeType.STRING))
    val output = enforceOutput(exec.processTuple(mixedTuple, 0).next(), outputSchema)

    output.getField[String]("summary") shouldBe "carlo:active:12345:3"
  }

  it should "support binary output columns" in {
    assume(CompiledRustUDFCompiler.isCompilerAvailable)

    val desc = new CompiledRustUDFOpDesc
    desc.code = binaryCode
    desc.inputColumns = List("payload")
    desc.retainInputColumns = false
    desc.outputColumns = List(new Attribute("payload_out", AttributeType.BINARY))
    desc.executionMode = CompiledRustUDFOpDesc.TupleMode
    val exec = new CompiledRustUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val outputSchema = Schema().add(new Attribute("payload_out", AttributeType.BINARY))
    val output = enforceOutput(exec.processTuple(mixedTuple, 0).next(), outputSchema)

    output.getField[Array[Byte]]("payload_out") should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 33)
  }

  "CompiledRustUDFOpDesc" should "advertise the Rust UDF group" in {
    val info = configuredDesc.operatorInfo

    info.userFriendlyName shouldBe "Compiled Rust UDF"
    info.operatorGroupName shouldBe OperatorGroupConstants.RUST_GROUP
  }

  it should "initialize code with the default process template" in {
    val desc = new CompiledRustUDFOpDesc

    desc.code shouldBe CompiledRustUDFOpDesc.DefaultRustCode
    desc.code should include("process_tuple")
  }

  it should "publish the default process template in generated operator metadata" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[CompiledRustUDFOpDesc])
    val codeSchema = schema.get("properties").get("code")
    val inputColumnsSchema = schema.get("properties").get("inputColumns")
    val outputColumnsSchema = schema.get("properties").get("outputColumns")
    val requiredProperties = Option(schema.get("required")).map(_.toString).getOrElse("")

    codeSchema.get("default").asText() shouldBe CompiledRustUDFOpDesc.DefaultRustCode
    inputColumnsSchema.has("minItems") shouldBe false
    outputColumnsSchema.has("minItems") shouldBe false
    requiredProperties should not include "inputColumns"
    requiredProperties should not include "outputColumns"
  }

  it should "derive an output schema with retained input columns and typed output columns" in {
    val desc = configuredDesc
    desc.outputColumns = List(
      new Attribute("score", AttributeType.DOUBLE),
      new Attribute("label", AttributeType.STRING)
    )

    val outputSchemas = desc.getExternalOutputSchemas(
      Map(desc.operatorInfo.inputPorts.head.id -> numericSchema)
    )
    val outputSchema = outputSchemas(desc.operatorInfo.outputPorts.head.id)

    outputSchema.getAttributes.map(_.getName) shouldBe List("age", "income", "score", "label")
    outputSchema.getAttribute("score").getType shouldBe AttributeType.DOUBLE
    outputSchema.getAttribute("label").getType shouldBe AttributeType.STRING
  }

  it should "derive an output schema with only retained input columns when no extra output columns are declared" in {
    val desc = configuredDesc
    desc.outputColumns = List.empty

    val outputSchemas = desc.getExternalOutputSchemas(
      Map(desc.operatorInfo.inputPorts.head.id -> numericSchema)
    )
    val outputSchema = outputSchemas(desc.operatorInfo.outputPorts.head.id)

    outputSchema.getAttributes.map(_.getName) shouldBe List("age", "income")
  }

  it should "derive an output schema without retained input columns" in {
    val desc = configuredDesc
    desc.inputColumns = List("name", "active", "created", "payload")
    desc.retainInputColumns = false
    desc.outputColumns = List(new Attribute("summary", AttributeType.STRING))

    val outputSchemas = desc.getExternalOutputSchemas(
      Map(desc.operatorInfo.inputPorts.head.id -> mixedSchema)
    )
    val outputSchema = outputSchemas(desc.operatorInfo.outputPorts.head.id)

    outputSchema.getAttributes.map(_.getName) shouldBe List("summary")
    outputSchema.getAttribute("summary").getType shouldBe AttributeType.STRING
  }

  it should "accept non-numeric selected input columns" in {
    val desc = new CompiledRustUDFOpDesc
    desc.code = mixedTypeCode
    desc.inputColumns = List("name", "active", "created", "payload")
    desc.retainInputColumns = false
    desc.outputColumns = List(new Attribute("summary", AttributeType.STRING))

    val outputSchemas = desc.getExternalOutputSchemas(
      Map(desc.operatorInfo.inputPorts.head.id -> mixedSchema)
    )

    outputSchemas(desc.operatorInfo.outputPorts.head.id).getAttribute("summary").getType shouldBe AttributeType.STRING
  }

  it should "reject selected large_binary input columns" in {
    val schema = Schema().add(new Attribute("large", AttributeType.LARGE_BINARY))
    val desc = configuredDesc
    desc.inputColumns = List("large")

    val error = intercept[RuntimeException] {
      desc.getExternalOutputSchemas(Map(desc.operatorInfo.inputPorts.head.id -> schema))
    }

    error.getMessage should include("does not support large_binary columns yet")
  }

  it should "reject large_binary output columns" in {
    val desc = configuredDesc
    desc.outputColumns = List(new Attribute("large", AttributeType.LARGE_BINARY))

    val error = intercept[RuntimeException] {
      desc.getExternalOutputSchemas(Map(desc.operatorInfo.inputPorts.head.id -> numericSchema))
    }

    error.getMessage should include("does not support large_binary output columns yet")
  }
}
