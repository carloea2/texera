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

package org.apache.texera.amber.operator.statistics

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{LogicalOp, PythonOperatorDescriptor}
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Tag

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import scala.io.{Codec, Source}
import scala.util.Using

class OLSAnalysisOpDescSpec extends AnyFlatSpec with Matchers {
  private def config: ObjectNode =
    objectMapper
      .readTree(
        """{"operatorType":"OLSAnalysis","target":"y","predictors":["x"]}"""
      )
      .asInstanceOf[ObjectNode]

  private def descriptor(node: ObjectNode = config): PythonOperatorDescriptor =
    objectMapper.treeToValue(node, classOf[LogicalOp]).asInstanceOf[PythonOperatorDescriptor]

  private val input = Schema(
    List(
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("x", AttributeType.INTEGER),
      new Attribute("z", AttributeType.LONG),
      new Attribute("text", AttributeType.STRING),
      new Attribute("flag", AttributeType.BOOLEAN)
    )
  )
  private def schemas(
      d: PythonOperatorDescriptor,
      schema: Schema = input
  ): Map[PortIdentity, Schema] =
    Map(d.operatorInfo.inputPorts.head.id -> schema)

  "OLS Analysis" should "advertise a full-table single-input blocking operator" in {
    val d = descriptor()
    d.operatorInfo.userFriendlyName shouldBe "OLS Analysis"
    d.operatorInfo.inputPorts should have size 1
    d.operatorInfo.outputPorts should have size 1
    d.operatorInfo.outputPorts.head.blocking shouldBe true
    d.parallelizable() shouldBe false
  }

  it should "default to an intercept and complete-case omission" in {
    val json = objectMapper.valueToTree[ObjectNode](descriptor())
    json.get("includeIntercept").asBoolean() shouldBe true
    json.get("missingValues").asText() shouldBe "omit"
  }

  it should "round-trip ordered predictors and non-default choices" in {
    val node = config
    node.putArray("predictors").add("z").add("x")
    node.put("includeIntercept", false).put("missingValues", "error")
    val d = descriptor(node)
    val encoded = objectMapper.writeValueAsString(d)
    val restored = objectMapper.readValue(encoded, classOf[LogicalOp])
    objectMapper.writeValueAsString(restored) shouldBe encoded
    noException should be thrownBy d.getOutputSchemas(schemas(d))
  }

  private val doubleColumns = List(
    "estimate",
    "std_error",
    "t_statistic",
    "p_value",
    "ci_lower",
    "ci_upper",
    "residual_std_error",
    "r_squared",
    "adj_r_squared",
    "f_statistic",
    "f_p_value"
  )
  private val countColumns = List("n_input", "n_used", "n_omitted", "df_model", "df_residual")

  it should "declare typed coefficient and model statistics" in {
    val d = descriptor()
    val schema = d.getOutputSchemas(schemas(d))(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("term").getType shouldBe AttributeType.STRING
    schema.getAttribute("is_intercept").getType shouldBe AttributeType.BOOLEAN
    doubleColumns.foreach(n => schema.getAttribute(n).getType shouldBe AttributeType.DOUBLE)
    countColumns.foreach(n => schema.getAttribute(n).getType shouldBe AttributeType.LONG)
    schema.getAttributes should have size (2 + doubleColumns.size + countColumns.size)
  }

  private def rejects(name: String, change: ObjectNode => Unit, message: String): Unit = {
    it should s"reject $name before execution" in {
      val node = config
      change(node)
      val d = descriptor(node)
      intercept[IllegalArgumentException](d.getOutputSchemas(schemas(d))).getMessage should include(
        message
      )
      // Code generation must remain syntactically valid for half-filled/malformed
      // UI settings. Schema propagation rejects them before execution; generated
      // code must also refuse them if invoked without schema propagation.
      d.generatePythonCode() should include("raise ValueError(configuration_error)")
    }
  }
  rejects("an empty response", _.put("target", ""), "response")
  rejects("a null response", _.putNull("target"), "response")
  rejects("no predictors", _.putArray("predictors"), "predictor")
  rejects("null predictors", _.putNull("predictors"), "predictor")
  rejects("a null predictor", _.putArray("predictors").addNull(), "predictor")
  rejects("an empty predictor", _.putArray("predictors").add(""), "predictor")
  rejects("duplicate predictors", _.putArray("predictors").add("x").add("x"), "unique")
  rejects("the response as a predictor", _.putArray("predictors").add("y"), "response")
  rejects("an unknown missing policy", _.put("missingValues", "zero"), "missing")
  rejects("a null missing policy", _.putNull("missingValues"), "missing")
  rejects("a null intercept choice", _.putNull("includeIntercept"), "intercept")

  for (column <- List("absent", "text", "flag")) {
    it should s"reject a non-numeric or absent response/predictor '$column'" in {
      for (field <- List("target", "predictors")) {
        val node = config
        if (field == "target") node.put(field, column)
        else node.putArray(field).add(column)
        val d = descriptor(node)
        intercept[IllegalArgumentException](
          d.getOutputSchemas(schemas(d))
        ).getMessage should include(column)
      }
    }
  }

  it should "reject an absent input schema clearly" in {
    intercept[IllegalArgumentException](
      descriptor().getOutputSchemas(Map.empty)
    ).getMessage should include("input")
  }

  it should "expose numeric selectors, missing-value choices and UI defaults" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(descriptor().getClass)
    val properties = schema.get("properties")
    properties.get("includeIntercept").get("default").asBoolean() shouldBe true
    properties.get("missingValues").get("default").asText() shouldBe "omit"
    properties.get("missingValues").get("enum").toString shouldBe "[\"omit\",\"error\"]"
    properties.get("predictors").get("minItems").asInt() shouldBe 1
    properties.get("predictors").get("uniqueItems").asBoolean() shouldBe true
    schema.get("attributeTypeRules").get("target").get("enum").toString shouldBe
      "[\"integer\",\"long\",\"double\"]"
  }

  it should "execute generated code against real statsmodels and independent analytical checks" taggedAs Tag(
    classOf[IntegrationTest].getName
  ) in {
    checkRuntime(realRuntime = false)
  }

  it should "buffer all input and finalize typed results using real pytexera" taggedAs Tag(
    classOf[IntegrationTest].getName
  ) in {
    checkRuntime(realRuntime = true)
  }

  private def checkRuntime(realRuntime: Boolean): Unit = {
    val python =
      ConfigFactory.parseResources("udf.conf").resolve().getConfig("python").getString("path")
    val payload = objectMapper.createObjectNode()
    val cases = List("default", "no_intercept", "strict", "multiple", "unusual")
    cases.foreach { name =>
      val node = config
      name match {
        case "no_intercept" => node.put("includeIntercept", false)
        case "strict"       => node.put("missingValues", "error")
        case "multiple"     => node.putArray("predictors").add("x").add("z")
        case "unusual" =>
          node.put("target", "réponse\n'\\")
          node.putArray("predictors").add("(Intercept)").add("x\n'\"\\λ")
        case _ =>
      }
      payload.put(name, descriptor(node).generatePythonCode())
    }
    val invalid = config
    invalid.putArray("predictors").add("y")
    payload.put("invalid", descriptor(invalid).generatePythonCode())
    val d = descriptor()
    val columns = payload.putArray("columns")
    val types = payload.putObject("types")
    d.getOutputSchemas(schemas(d))(d.operatorInfo.outputPorts.head.id)
      .getAttributes
      .foreach { a =>
        columns.add(a.getName)
        types.put(a.getName, a.getType.name())
      }
    val driver = Using.resource(
      Source.fromResource("statistics/ols_runtime_checks.py")(Codec.UTF8)
    )(_.mkString)
    val moduleFile = Files.createTempFile("ols_modules_", ".json")
    val driverFile = Files.createTempFile("ols_checks_", ".py")
    val outputFile = Files.createTempFile("ols_checks_", ".log")
    try {
      Files.write(moduleFile, objectMapper.writeValueAsBytes(payload))
      Files.writeString(driverFile, driver, StandardCharsets.UTF_8)
      val builder =
        new ProcessBuilder(python, "-X", "utf8", driverFile.toString, moduleFile.toString)
          .redirectErrorStream(true)
          .redirectOutput(outputFile.toFile)
      if (realRuntime) {
        val root = Iterator
          .iterate(Paths.get("").toAbsolutePath)(_.getParent)
          .takeWhile(_ != null)
          .find(p => Files.isDirectory(p.resolve("amber/src/main/python")))
          .getOrElse(fail("Cannot locate the Amber Python source tree"))
        builder.environment().put("PYTHONPATH", root.resolve("amber/src/main/python").toString)
        builder.command().add("--real")
      }
      val process = builder.start()
      if (!process.waitFor(60, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        fail("OLS runtime checks timed out after 60 seconds")
      }
      val output = Files.readString(outputFile)
      withClue(output) { process.exitValue() shouldBe 0 }
      output should include("OK")
      if (realRuntime) output should include("REAL_TABLE_LIFECYCLE_OK")
    } finally {
      Files.deleteIfExists(moduleFile)
      Files.deleteIfExists(driverFile)
      Files.deleteIfExists(outputFile)
    }
  }
}
