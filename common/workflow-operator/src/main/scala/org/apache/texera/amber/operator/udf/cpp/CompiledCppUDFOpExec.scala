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

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.{
  BufferedReader,
  BufferedWriter,
  IOException,
  InputStreamReader,
  OutputStreamWriter
}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.util.Base64
import java.util.concurrent.{Callable, ExecutionException, Executors, TimeUnit, TimeoutException}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.SeqHasAsJava

class CompiledCppUDFOpExec(descString: String) extends OperatorExecutor {
  private val desc: CompiledCppUDFOpDesc =
    objectMapper.readValue(descString, classOf[CompiledCppUDFOpDesc])
  private var executablePath: Path = _
  private var executionMode: String = _
  private var process: Process = _
  private var processInput: BufferedWriter = _
  private var processOutput: BufferedReader = _
  private var stderrPath: Path = _
  private var schemaInitialized = false
  private val readExecutor = Executors.newSingleThreadExecutor { runnable =>
    val thread = new Thread(runnable, "compiled-cpp-udf-reader")
    thread.setDaemon(true)
    thread
  }
  private val pendingTuples = ArrayBuffer.empty[Tuple]

  override def open(): Unit = {
    desc.validateBasicConfig()
    executionMode = desc.normalizedExecutionMode
    executablePath = CompiledCppUDFCompiler.compile(desc.compileRequest)
    startWorkerProcess()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    pendingTuples += tuple
    if (shouldFlush) flushPendingTuples() else Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = flushPendingTuples()

  override def close(): Unit = {
    pendingTuples.clear()
    closeWorkerProcess()
    readExecutor.shutdownNow()
  }

  private def shouldFlush: Boolean =
    executionMode match {
      case CompiledCppUDFOpDesc.TupleMode => pendingTuples.nonEmpty
      case CompiledCppUDFOpDesc.BatchMode =>
        pendingTuples.size >= desc.normalizedBatchSize
      case CompiledCppUDFOpDesc.TableMode => false
      case _                              => pendingTuples.nonEmpty
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
        s"C++ UDF produced ${outputRows.size} output rows for ${tuples.size} input rows. Retain input columns requires exactly one output row per input row."
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
    ensureWorkerReady(inputAttributes)
    try {
      processInput.write(executionMode)
      processInput.newLine()
      processInput.write(rows.size.toString)
      processInput.newLine()
      rows.foreach { values =>
        processInput.write(values.mkString("\t"))
        processInput.newLine()
      }
      processInput.flush()

      val outputRowCount = parseOutputRowCount(readProtocolLine())
      (0 until outputRowCount).map { _ =>
        val line = readProtocolLine()
        if (line == null) {
          throw workerExitedException("Compiled C++ UDF worker exited while streaming output rows.")
        }
        parseOutputRow(line)
      }
    } catch {
      case e: IOException =>
        throw runtimeException(s"Unable to communicate with compiled C++ UDF worker: ${e.getMessage}", e)
    }
  }

  private def ensureWorkerReady(inputAttributes: List[Attribute]): Unit = {
    ensureExecutable()
    if (process == null) {
      startWorkerProcess()
    } else if (!process.isAlive) {
      throw workerExitedException("Compiled C++ UDF worker is not running.")
    }
    if (!schemaInitialized) {
      try {
        processInput.write(inputAttributes.size.toString)
        processInput.newLine()
        processInput.write(inputAttributes.map(attribute => base64(attribute.getName)).mkString("\t"))
        processInput.newLine()
        processInput.write(inputAttributes.map(attribute => typeTag(attribute.getType)).mkString("\t"))
        processInput.newLine()
        processInput.flush()
        schemaInitialized = true
      } catch {
        case e: IOException =>
          throw runtimeException(
            s"Unable to initialize compiled C++ UDF worker schema: ${e.getMessage}",
            e
          )
      }
    }
  }

  private def startWorkerProcess(): Unit = {
    val executable = ensureExecutable()
    closeWorkerProcess()
    stderrPath = Files.createTempFile("texera-cpp-udf-runtime", ".err")
    try {
      process = new ProcessBuilder(Seq(executable.toString).asJava)
        .redirectError(stderrPath.toFile)
        .start()
      processInput = new BufferedWriter(
        new OutputStreamWriter(process.getOutputStream, StandardCharsets.UTF_8)
      )
      processOutput = new BufferedReader(
        new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8)
      )
      schemaInitialized = false
    } catch {
      case e: IOException =>
        throw runtimeException(
          s"Unable to start compiled C++ UDF executable '$executable': ${e.getMessage}",
          e
        )
    }
  }

  private def closeWorkerProcess(): Unit = {
    Option(processInput).foreach(writer =>
      try writer.close()
      catch { case _: IOException => () }
    )
    Option(processOutput).foreach(reader =>
      try reader.close()
      catch { case _: IOException => () }
    )
    if (process != null && process.isAlive) {
      try {
        if (!process.waitFor(250, TimeUnit.MILLISECONDS)) {
          process.destroyForcibly()
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          process.destroyForcibly()
      }
    }
    Option(stderrPath).foreach(path => Files.deleteIfExists(path))
    process = null
    processInput = null
    processOutput = null
    stderrPath = null
    schemaInitialized = false
  }

  private def readProtocolLine(): String = {
    val future = readExecutor.submit(new Callable[String] {
      override def call(): String = processOutput.readLine()
    })
    try {
      val line = future.get(desc.timeoutMs.toLong, TimeUnit.MILLISECONDS)
      if (line == null) {
        throw workerExitedException("Compiled C++ UDF worker exited before producing output.")
      }
      line
    } catch {
      case _: TimeoutException =>
        future.cancel(true)
        destroyWorkerProcess()
        throw runtimeException(s"Timed out after ${desc.timeoutMs} ms.")
      case e: ExecutionException =>
        throw runtimeException(
          s"Unable to read compiled C++ UDF worker output: ${Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)}",
          e.getCause
        )
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw runtimeException("Interrupted while waiting for compiled C++ UDF worker output.", e)
    }
  }

  private def parseOutputRowCount(line: String): Int =
    try {
      Integer.parseInt(line)
    } catch {
      case e: NumberFormatException =>
        throw runtimeException(
          s"Invalid C++ UDF protocol response '$line'. Avoid writing to stdout outside process_tuple/process_batch/process_table; write logs to stderr instead.",
          e
        )
    }

  private def destroyWorkerProcess(): Unit =
    if (process != null && process.isAlive) {
      process.destroyForcibly()
    }

  private def workerExitedException(message: String): RuntimeException = {
    val stderr = readWorkerStderr
    val status =
      if (process != null && !process.isAlive) s" Exit status: ${process.exitValue()}." else ""
    runtimeException(
      Seq(message + status, stderr.trim)
        .filter(_.nonEmpty)
        .mkString("\n")
    )
  }

  private def readWorkerStderr: String =
    if (stderrPath != null && Files.exists(stderrPath)) {
      Files.readString(stderrPath, StandardCharsets.UTF_8)
    } else {
      ""
    }

  private def parseOutputRow(line: String): Array[Any] = {
    val tokens = if (line.isEmpty) Array.empty[String] else line.split("\t", -1)
    if (tokens.length != desc.outputColumns.length) {
      throw runtimeException(
        s"C++ UDF produced ${tokens.length} fields, but output schema expects ${desc.outputColumns.length} fields."
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
        throw new RuntimeException("Compiled C++ UDF does not support large_binary values yet.")
    }
    s"$tag:0:$payload"
  }

  private def parseField(token: String, expectedType: AttributeType): Any = {
    ensureSupportedType("output", expectedType, isOutput = true)
    if (token.length < 4 || token.charAt(1) != ':' || token.charAt(3) != ':') {
      throw runtimeException(s"Invalid C++ UDF output field '$token'.")
    }

    val actualTag = token.charAt(0)
    val expectedTag = typeTag(expectedType)
    if (actualTag != expectedTag) {
      throw runtimeException(
        s"C++ UDF returned ${typeName(actualTag)} but output schema expects ${expectedType}."
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
        throw new RuntimeException("Compiled C++ UDF does not support large_binary values yet.")
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
        s"Compiled C++ UDF does not support large_binary $direction columns yet. Column '$columnName' has type $attributeType."
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
    new RuntimeException(s"C++ UDF execution failed:\n$message", cause)
}
