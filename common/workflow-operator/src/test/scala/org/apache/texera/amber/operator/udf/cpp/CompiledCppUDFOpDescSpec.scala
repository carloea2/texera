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

import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.sql.Timestamp

class CompiledCppUDFOpDescSpec extends AnyFlatSpec with Matchers {
  private val validCode =
    """class ProcessTupleOperator : public texera::UDFOperator {
      |public:
      |    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {
      |        double age = tuple.get("age").as_double();
      |        double income = tuple.get("income").as_double();
      |        double score = income / (age + 1.0);
      |        return { texera::TupleLike{ texera::Value::double_value(score) } };
      |    }
      |};
      |using TexeraUDFOperator = ProcessTupleOperator;""".stripMargin

  private val batchCode =
    """class ProcessBatchOperator : public texera::UDFOperator {
      |public:
      |    texera::BatchLike process_batch(const texera::Batch& batch, int port) override {
      |        texera::BatchLike output;
      |        output.reserve(batch.size());
      |        for (const auto& tuple : batch) {
      |            output.push_back(texera::TupleLike{
      |                texera::Value::double_value(tuple.get("age").as_double() + tuple.get("income").as_double())
      |            });
      |        }
      |        return output;
      |    }
      |};
      |using TexeraUDFOperator = ProcessBatchOperator;""".stripMargin

  private val tableCode =
    """class ProcessTableOperator : public texera::UDFOperator {
      |public:
      |    texera::TableLike process_table(const texera::Table& table, int port) override {
      |        texera::TableLike output;
      |        output.reserve(table.size());
      |        long long row_number = 1;
      |        for (const auto& tuple : table) {
      |            output.push_back(texera::TupleLike{
      |                texera::Value::double_value(tuple.get("income").as_double() + row_number)
      |            });
      |            row_number += 1;
      |        }
      |        return output;
      |    }
      |};
      |using TexeraUDFOperator = ProcessTableOperator;""".stripMargin

  private val mixedTypeCode =
    """class MixedTypeOperator : public texera::UDFOperator {
      |public:
      |    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {
      |        std::string active = tuple.get("active").as_bool() ? "active" : "inactive";
      |        std::string summary =
      |            tuple.get("name").as_string() + ":" +
      |            active + ":" +
      |            std::to_string(tuple.get("created").as_timestamp_millis()) + ":" +
      |            std::to_string(tuple.get("payload").as_binary().size());
      |        return { texera::TupleLike{ texera::Value::string_value(summary) } };
      |    }
      |};
      |using TexeraUDFOperator = MixedTypeOperator;""".stripMargin

  private val binaryCode =
    """class BinaryOperator : public texera::UDFOperator {
      |public:
      |    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {
      |        std::string payload = tuple.get("payload").as_binary();
      |        payload.push_back('!');
      |        return { texera::TupleLike{ texera::Value::binary_value(payload) } };
      |    }
      |};
      |using TexeraUDFOperator = BinaryOperator;""".stripMargin

  private val retainOnlyCode =
    """class RetainOnlyOperator : public texera::UDFOperator {
      |public:
      |    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {
      |        return { texera::TupleLike{} };
      |    }
      |};
      |using TexeraUDFOperator = RetainOnlyOperator;""".stripMargin

  private val statefulCode =
    """class StatefulOperator : public texera::UDFOperator {
      |public:
      |    int seen = 0;
      |
      |    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {
      |        seen += 1;
      |        return { texera::TupleLike{ texera::Value::double_value(seen) } };
      |    }
      |};
      |using TexeraUDFOperator = StatefulOperator;""".stripMargin

  private def compileRequest(
      source: String = validCode,
      columns: List[String] = List("age", "income")
  ): CompiledCppUDFCompileRequest =
    CompiledCppUDFCompileRequest(
      code = source,
      inputColumns = columns,
      retainInputColumns = true,
      outputColumns = List("score:double"),
      compilerFlags = "-O3",
      timeoutMs = 5000
    )

  private def configuredDesc: CompiledCppUDFOpDesc = {
    val desc = new CompiledCppUDFOpDesc
    desc.code = validCode
    desc.inputColumns = List("age", "income")
    desc.retainInputColumns = true
    desc.outputColumns = List(new Attribute("score", AttributeType.DOUBLE))
    desc.executionMode = CompiledCppUDFOpDesc.TupleMode
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

  "CompiledCppUDFCompiler" should "compile a valid tuple API operator" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val executable = CompiledCppUDFCompiler.compile(compileRequest())

    Files.exists(executable) shouldBe true
  }

  it should "compile the default template with one selected input column" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val executable = CompiledCppUDFCompiler.compile(
      compileRequest(CompiledCppUDFOpDesc.DefaultCppCode, List("line"))
    )

    Files.exists(executable) shouldBe true
  }

  it should "surface a readable compile error for invalid C++" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val invalidCode =
      """class ProcessTupleOperator : public texera::UDFOperator {
        |public:
        |    texera::TupleOutput process_tuple(const texera::Tuple& tuple, int port) override {
        |    return age +
        |    }
        |};
        |using TexeraUDFOperator = ProcessTupleOperator;""".stripMargin
    val error = intercept[RuntimeException] {
      CompiledCppUDFCompiler.compile(compileRequest(invalidCode))
    }

    error.getMessage should include("C++ compilation failed:")
    error.getMessage should include("using TexeraUDFOperator = ProcessTupleOperator;")
  }

  "CompiledCppUDFOpExec" should "append a typed output column while retaining input fields" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(configuredDesc))
    exec.open()
    val outputTuple = exec
      .processTuple(tuple(20, 50000.0), 0)
      .next()

    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    enforceOutput(outputTuple, outputSchema).getField[Double]("score") shouldBe (50000.0 / 21.0 +- 0.000001)
  }

  it should "allow retaining input columns without declaring extra output columns" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = retainOnlyCode
    desc.outputColumns = List.empty
    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val output = enforceOutput(exec.processTuple(tuple(20, 50000.0), 0).next(), numericSchema)

    output.getField[Integer]("age") shouldBe Integer.valueOf(20)
    output.getField[Double]("income") shouldBe 50000.0
  }

  it should "expose every upstream input column when input columns are left empty" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.inputColumns = List.empty
    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    val output = enforceOutput(exec.processTuple(tuple(20, 50000.0), 0).next(), outputSchema)

    output.getField[Double]("score") shouldBe (50000.0 / 21.0 +- 0.000001)
  }

  it should "reuse the compiled worker process across tuple calls" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = statefulCode
    desc.outputColumns = List(new Attribute("seen", AttributeType.DOUBLE))
    desc.executionMode = CompiledCppUDFOpDesc.TupleMode
    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(desc))
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

  it should "support the C++ process_batch API" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = batchCode
    desc.executionMode = CompiledCppUDFOpDesc.BatchMode
    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    exec.processTuple(tuple(20, 50000.0), 0).toList shouldBe empty
    exec.processTuple(tuple(40, 90000.0), 0).toList shouldBe empty
    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    val output = exec.onFinish(0).map(enforceOutput(_, outputSchema)).toList

    output.map(_.getField[Double]("score")) shouldBe List(50020.0, 90040.0)
  }

  it should "support the C++ process_table API" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val desc = configuredDesc
    desc.code = tableCode
    desc.executionMode = CompiledCppUDFOpDesc.TableMode
    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    exec.processTuple(tuple(20, 50000.0), 0).toList shouldBe empty
    exec.processTuple(tuple(40, 90000.0), 0).toList shouldBe empty
    val outputSchema = numericSchema.add(new Attribute("score", AttributeType.DOUBLE))
    val output = exec.onFinish(0).map(enforceOutput(_, outputSchema)).toList

    output.map(_.getField[Double]("score")) shouldBe List(50001.0, 90002.0)
  }

  it should "support string boolean timestamp and binary input with string output" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val desc = new CompiledCppUDFOpDesc
    desc.code = mixedTypeCode
    desc.inputColumns = List("name", "active", "created", "payload")
    desc.retainInputColumns = false
    desc.outputColumns = List(new Attribute("summary", AttributeType.STRING))
    desc.executionMode = CompiledCppUDFOpDesc.TupleMode
    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val outputSchema = Schema().add(new Attribute("summary", AttributeType.STRING))
    val output = enforceOutput(exec.processTuple(mixedTuple, 0).next(), outputSchema)

    output.getField[String]("summary") shouldBe "carlo:active:12345:3"
  }

  it should "support binary output columns" in {
    assume(CompiledCppUDFCompiler.isCompilerAvailable)

    val desc = new CompiledCppUDFOpDesc
    desc.code = binaryCode
    desc.inputColumns = List("payload")
    desc.retainInputColumns = false
    desc.outputColumns = List(new Attribute("payload_out", AttributeType.BINARY))
    desc.executionMode = CompiledCppUDFOpDesc.TupleMode
    val exec = new CompiledCppUDFOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val outputSchema = Schema().add(new Attribute("payload_out", AttributeType.BINARY))
    val output = enforceOutput(exec.processTuple(mixedTuple, 0).next(), outputSchema)

    output.getField[Array[Byte]]("payload_out") should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 33)
  }

  "CompiledCppUDFOpDesc" should "advertise the C++ UDF group" in {
    val info = configuredDesc.operatorInfo

    info.userFriendlyName shouldBe "Compiled C++ UDF"
    info.operatorGroupName shouldBe OperatorGroupConstants.CPP_GROUP
  }

  it should "initialize code with the default process template" in {
    val desc = new CompiledCppUDFOpDesc

    desc.code shouldBe CompiledCppUDFOpDesc.DefaultCppCode
    desc.code should include("process_tuple")
  }

  it should "publish the default process template in generated operator metadata" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[CompiledCppUDFOpDesc])
    val codeSchema = schema.get("properties").get("code")
    val inputColumnsSchema = schema.get("properties").get("inputColumns")
    val outputColumnsSchema = schema.get("properties").get("outputColumns")
    val requiredProperties = Option(schema.get("required")).map(_.toString).getOrElse("")

    codeSchema.get("default").asText() shouldBe CompiledCppUDFOpDesc.DefaultCppCode
    inputColumnsSchema.has("minItems") shouldBe false
    outputColumnsSchema.has("minItems") shouldBe false
    requiredProperties should not include "inputColumns"
    requiredProperties should not include "outputColumns"
    schema.get("properties").has("cppCode") shouldBe false
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
    val desc = new CompiledCppUDFOpDesc
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
