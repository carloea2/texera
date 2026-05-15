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

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.SeqHasAsJava

class CompiledRustUDFOpExec(descString: String) extends OperatorExecutor {
  private val desc: CompiledRustUDFOpDesc =
    objectMapper.readValue(descString, classOf[CompiledRustUDFOpDesc])
  private var executablePath: Path = _
  private var executionMode: String = _
  private val pendingTuples = ArrayBuffer.empty[Tuple]

  override def open(): Unit = {
    desc.validateBasicConfig()
    executionMode = desc.normalizedExecutionMode
    executablePath = CompiledRustUDFCompiler.compile(desc.compileRequest)
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    pendingTuples += tuple
    if (shouldFlush) flushPendingTuples() else Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = flushPendingTuples()

  override def close(): Unit = pendingTuples.clear()

  private def shouldFlush: Boolean =
    executionMode match {
      case CompiledRustUDFOpDesc.TupleMode => pendingTuples.nonEmpty
      case CompiledRustUDFOpDesc.BatchMode =>
        pendingTuples.size >= desc.normalizedBatchSize
      case CompiledRustUDFOpDesc.TableMode => false
      case _                               => pendingTuples.nonEmpty
    }

  private def flushPendingTuples(): Iterator[TupleLike] = {
    if (pendingTuples.isEmpty) {
      return Iterator.empty
    }

    val tuples = pendingTuples.toVector
    pendingTuples.clear()

    val inputAttributes = selectedInputAttributes(tuples.head.getSchema)
    val rows = tuples.map(tuple =>
      inputAttributes.map(attribute =>
        encodeField(tuple.getField[Any](attribute.getName), attribute.getType)
      )
    )
    val outputRows = executeRows(inputAttributes, rows)

    if (desc.retainInputColumns && outputRows.size != tuples.size) {
      throw new RuntimeException(
        s"Rust UDF produced ${outputRows.size} output rows for ${tuples.size} input rows. Retain input columns requires exactly one output row per input row."
      )
    }

    outputRows.zipWithIndex.iterator.map {
      case (fields, index) if desc.retainInputColumns =>
        TupleLike(tuples(index).getFields ++ fields)
      case (fields, _) =>
        TupleLike(fields)
    }
  }

  private def executeRows(
      inputAttributes: List[Attribute],
      rows: Seq[List[String]]
  ): Seq[Array[Any]] = {
    val executable = ensureExecutable()
    val stdoutPath = Files.createTempFile("texera-rust-udf-runtime", ".out")
    val stderrPath = Files.createTempFile("texera-rust-udf-runtime", ".err")
    try {
      val process =
        try {
          new ProcessBuilder(Seq(executable.toString).asJava)
            .redirectOutput(stdoutPath.toFile)
            .redirectError(stderrPath.toFile)
            .start()
        } catch {
          case e: IOException =>
            throw runtimeException(
              s"Unable to start compiled Rust UDF executable '$executable': ${e.getMessage}",
              e
            )
        }

      val writer = new BufferedWriter(
        new OutputStreamWriter(process.getOutputStream, StandardCharsets.UTF_8)
      )
      try {
        writer.write(inputAttributes.size.toString)
        writer.newLine()
        writer.write(inputAttributes.map(attribute => base64(attribute.getName)).mkString("\t"))
        writer.newLine()
        writer.write(inputAttributes.map(attribute => typeTag(attribute.getType)).mkString("\t"))
        writer.newLine()
        rows.foreach { values =>
          writer.write(values.mkString("\t"))
          writer.newLine()
        }
      } finally {
        writer.close()
      }

      if (!process.waitFor(desc.timeoutMs.toLong, TimeUnit.MILLISECONDS)) {
        process.destroyForcibly()
        throw runtimeException(s"Timed out after ${desc.timeoutMs} ms.")
      }

      val stderr = Files.readString(stderrPath, StandardCharsets.UTF_8).trim
      if (process.exitValue() != 0) {
        val message =
          if (stderr.nonEmpty) stderr else s"Executable exited with status ${process.exitValue()}."
        throw runtimeException(message)
      }

      Files
        .readString(stdoutPath, StandardCharsets.UTF_8)
        .replace("\r\n", "\n")
        .replace('\r', '\n')
        .split("\n", -1)
        .toSeq match {
        case Seq("") => Seq.empty
        case lines if lines.lastOption.contains("") =>
          lines.dropRight(1).map(parseOutputRow).toList
        case lines => lines.map(parseOutputRow).toList
      }
    } finally {
      Files.deleteIfExists(stdoutPath)
      Files.deleteIfExists(stderrPath)
    }
  }

  private def parseOutputRow(line: String): Array[Any] = {
    val tokens = if (line.isEmpty) Array.empty[String] else line.split("\t", -1)
    if (tokens.length != desc.outputColumns.length) {
      throw runtimeException(
        s"Rust UDF produced ${tokens.length} fields, but output schema expects ${desc.outputColumns.length} fields."
      )
    }

    tokens.zip(desc.outputColumns).map {
      case (token, attribute) => parseField(token, attribute.getType)
    }
  }

  private def ensureExecutable(): Path = {
    if (executablePath == null) {
      open()
    }
    executablePath
  }

  private def selectedInputAttributes(schema: Schema): List[Attribute] =
    desc.selectedInputAttributes(schema).map { attribute =>
      ensureSupportedType(attribute.getName, attribute.getType, isOutput = false)
      attribute
    }

  private def encodeField(value: Any, attributeType: AttributeType): String = {
    ensureSupportedType("input", attributeType, isOutput = false)
    val tag = typeTag(attributeType)
    if (value == null) {
      return s"$tag:1:"
    }

    val payload = attributeType match {
      case AttributeType.STRING => base64(value.toString)
      case AttributeType.INTEGER =>
        value.asInstanceOf[Integer].intValue().toString
      case AttributeType.LONG =>
        value.asInstanceOf[java.lang.Long].longValue().toString
      case AttributeType.DOUBLE =>
        value.asInstanceOf[java.lang.Double].doubleValue().toString
      case AttributeType.BOOLEAN =>
        if (value.asInstanceOf[java.lang.Boolean].booleanValue()) "1" else "0"
      case AttributeType.TIMESTAMP =>
        value.asInstanceOf[Timestamp].getTime.toString
      case AttributeType.BINARY =>
        Base64.getEncoder.encodeToString(value.asInstanceOf[Array[Byte]])
      case AttributeType.ANY =>
        base64(String.valueOf(value))
      case AttributeType.LARGE_BINARY =>
        throw new RuntimeException("Compiled Rust UDF does not support large_binary values yet.")
    }
    s"$tag:0:$payload"
  }

  private def parseField(token: String, expectedType: AttributeType): Any = {
    ensureSupportedType("output", expectedType, isOutput = true)
    if (token.length < 4 || token.charAt(1) != ':' || token.charAt(3) != ':') {
      throw runtimeException(s"Invalid Rust UDF output field '$token'.")
    }

    val actualTag = token.charAt(0)
    val expectedTag = typeTag(expectedType)
    if (actualTag != expectedTag) {
      throw runtimeException(
        s"Rust UDF returned ${typeName(actualTag)} but output schema expects ${expectedType}."
      )
    }

    val isNull = token.charAt(2) == '1'
    if (isNull) {
      return null
    }

    val payload = token.substring(4)
    expectedType match {
      case AttributeType.STRING => unbase64(payload)
      case AttributeType.INTEGER =>
        Integer.valueOf(payload)
      case AttributeType.LONG =>
        java.lang.Long.valueOf(payload)
      case AttributeType.DOUBLE =>
        java.lang.Double.valueOf(payload)
      case AttributeType.BOOLEAN =>
        java.lang.Boolean.valueOf(payload == "1" || payload == "true")
      case AttributeType.TIMESTAMP =>
        new Timestamp(java.lang.Long.parseLong(payload))
      case AttributeType.BINARY =>
        Base64.getDecoder.decode(payload)
      case AttributeType.ANY =>
        unbase64(payload)
      case AttributeType.LARGE_BINARY =>
        throw new RuntimeException("Compiled Rust UDF does not support large_binary values yet.")
    }
  }

  private def ensureSupportedType(
      columnName: String,
      attributeType: AttributeType,
      isOutput: Boolean
  ): Unit =
    if (attributeType == AttributeType.LARGE_BINARY) {
      val direction = if (isOutput) "output" else "input"
      throw new RuntimeException(
        s"Compiled Rust UDF does not support large_binary $direction columns yet. Column '$columnName' has type $attributeType."
      )
    }

  private def typeTag(attributeType: AttributeType): Char =
    attributeType match {
      case AttributeType.STRING       => 'S'
      case AttributeType.INTEGER      => 'I'
      case AttributeType.LONG         => 'L'
      case AttributeType.DOUBLE       => 'D'
      case AttributeType.BOOLEAN      => 'O'
      case AttributeType.TIMESTAMP    => 'T'
      case AttributeType.BINARY       => 'B'
      case AttributeType.ANY          => 'A'
      case AttributeType.LARGE_BINARY => 'X'
    }

  private def typeName(tag: Char): String =
    tag match {
      case 'S' => AttributeType.STRING.toString
      case 'I' => AttributeType.INTEGER.toString
      case 'L' => AttributeType.LONG.toString
      case 'D' => AttributeType.DOUBLE.toString
      case 'O' => AttributeType.BOOLEAN.toString
      case 'T' => AttributeType.TIMESTAMP.toString
      case 'B' => AttributeType.BINARY.toString
      case 'A' => AttributeType.ANY.toString
      case _   => s"unknown($tag)"
    }

  private def base64(value: String): String =
    Base64.getEncoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def unbase64(value: String): String =
    new String(Base64.getDecoder.decode(value), StandardCharsets.UTF_8)

  private def runtimeException(message: String, cause: Throwable = null): RuntimeException =
    new RuntimeException(s"Rust UDF execution failed:\n$message", cause)
}
