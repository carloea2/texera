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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.SeqHasAsJava

case class CompiledRustUDFCompileRequest(
    code: String,
    inputColumns: List[String],
    retainInputColumns: Boolean,
    outputColumns: List[String],
    compilerFlags: String,
    timeoutMs: Int
)

object CompiledRustUDFCompiler {
  private val CacheRoot =
    Paths.get(System.getProperty("java.io.tmpdir"), "texera-compiled-rust-udf")
  private val WrapperProtocolVersion = "persistent-worker-v1"

  def compile(request: CompiledRustUDFCompileRequest): Path = {
    val compiler = sys.env.getOrElse("RUSTC", "rustc")
    val compilerVersionText = compilerVersion(compiler)
    val cacheKey = sha256(
      List(
        request.code,
        request.inputColumns.mkString("\u0000"),
        request.retainInputColumns.toString,
        request.outputColumns.mkString("\u0000"),
        request.compilerFlags,
        compilerVersionText,
        WrapperProtocolVersion
      ).mkString("\u0001")
    )
    val cacheDir = CacheRoot.resolve(cacheKey)
    Files.createDirectories(cacheDir)

    val rustPath = cacheDir.resolve("compiled_udf.rs")
    val executablePath = cacheDir.resolve(executableFileName)
    if (Files.exists(executablePath)) {
      return executablePath
    }

    Files.writeString(
      rustPath,
      generatedSource(request.code),
      StandardCharsets.UTF_8
    )

    val stdoutPath = cacheDir.resolve("compile.stdout")
    val stderrPath = cacheDir.resolve("compile.stderr")
    val command =
      Seq(compiler) ++ splitCompilerFlags(request.compilerFlags) ++ Seq(
        "--edition=2021",
        rustPath.toString,
        "-o",
        executablePath.toString
      )

    val processBuilder = new ProcessBuilder(command.asJava)
      .redirectOutput(stdoutPath.toFile)
      .redirectError(stderrPath.toFile)
    val process =
      try {
        processBuilder.start()
      } catch {
        case e: IOException =>
          throw compilationException(
            s"Unable to start compiler '$compiler': ${e.getMessage}",
            e
          )
      }

    if (!process.waitFor(request.timeoutMs.toLong, TimeUnit.MILLISECONDS)) {
      process.destroyForcibly()
      throw compilationException(s"Timed out after ${request.timeoutMs} ms.", null)
    }

    val stderr = readIfExists(stderrPath)
    val stdout = readIfExists(stdoutPath)
    if (process.exitValue() != 0) {
      val compilerOutput = Seq(stderr.trim, stdout.trim).filter(_.nonEmpty).mkString("\n")
      throw compilationException(compilerOutput, null)
    }

    executablePath
  }

  private[rust] def isCompilerAvailable: Boolean = {
    val compiler = sys.env.getOrElse("RUSTC", "rustc")
    try {
      val process = new ProcessBuilder(compiler, "--version")
        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
        .redirectError(ProcessBuilder.Redirect.DISCARD)
        .start()
      process.waitFor(2, TimeUnit.SECONDS) && process.exitValue() == 0
    } catch {
      case _: IOException => false
    }
  }

  private def compilerVersion(compiler: String): String = {
    val stdoutPath = Files.createTempFile("texera-rust-compiler-version", ".out")
    val stderrPath = Files.createTempFile("texera-rust-compiler-version", ".err")
    try {
      val process = new ProcessBuilder(compiler, "--version")
        .redirectOutput(stdoutPath.toFile)
        .redirectError(stderrPath.toFile)
        .start()
      if (process.waitFor(2, TimeUnit.SECONDS) && process.exitValue() == 0) {
        readIfExists(stdoutPath).linesIterator.nextOption().getOrElse("unknown")
      } else {
        process.destroyForcibly()
        "unknown"
      }
    } catch {
      case _: IOException => "unknown"
    } finally {
      Files.deleteIfExists(stdoutPath)
      Files.deleteIfExists(stderrPath)
    }
  }

  private def generatedSource(code: String): String =
    s"""use std::io::{self, BufRead, Write};
       |use std::process;
       |
       |pub mod texera {
       |    #[derive(Clone, Debug, PartialEq)]
       |    pub enum Type {
       |        String,
       |        Integer,
       |        Long,
       |        Double,
       |        Boolean,
       |        Timestamp,
       |        Binary,
       |        Any,
       |    }
       |
       |    pub fn type_name(type_: &Type) -> &'static str {
       |        match type_ {
       |            Type::String => "string",
       |            Type::Integer => "integer",
       |            Type::Long => "long",
       |            Type::Double => "double",
       |            Type::Boolean => "boolean",
       |            Type::Timestamp => "timestamp",
       |            Type::Binary => "binary",
       |            Type::Any => "any",
       |        }
       |    }
       |
       |    pub fn type_tag(type_: &Type) -> char {
       |        match type_ {
       |            Type::String => 'S',
       |            Type::Integer => 'I',
       |            Type::Long => 'L',
       |            Type::Double => 'D',
       |            Type::Boolean => 'O',
       |            Type::Timestamp => 'T',
       |            Type::Binary => 'B',
       |            Type::Any => 'A',
       |        }
       |    }
       |
       |    pub fn type_from_tag(tag: char) -> Result<Type, String> {
       |        match tag {
       |            'S' => Ok(Type::String),
       |            'I' => Ok(Type::Integer),
       |            'L' => Ok(Type::Long),
       |            'D' => Ok(Type::Double),
       |            'O' => Ok(Type::Boolean),
       |            'T' => Ok(Type::Timestamp),
       |            'B' => Ok(Type::Binary),
       |            'A' => Ok(Type::Any),
       |            _ => Err("unknown Texera value type tag".to_string()),
       |        }
       |    }
       |
       |    #[derive(Clone, Debug)]
       |    pub enum Value {
       |        Null(Type),
       |        String(String),
       |        Integer(i32),
       |        Long(i64),
       |        Double(f64),
       |        Boolean(bool),
       |        TimestampMillis(i64),
       |        Binary(Vec<u8>),
       |        Any(String),
       |    }
       |
       |    impl Value {
       |        pub fn null_value(type_: Type) -> Self {
       |            Self::Null(type_)
       |        }
       |
       |        pub fn string_value(value: impl Into<String>) -> Self {
       |            Self::String(value.into())
       |        }
       |
       |        pub fn integer_value(value: i32) -> Self {
       |            Self::Integer(value)
       |        }
       |
       |        pub fn long_value(value: i64) -> Self {
       |            Self::Long(value)
       |        }
       |
       |        pub fn double_value(value: f64) -> Self {
       |            Self::Double(value)
       |        }
       |
       |        pub fn boolean_value(value: bool) -> Self {
       |            Self::Boolean(value)
       |        }
       |
       |        pub fn timestamp_millis(value: i64) -> Self {
       |            Self::TimestampMillis(value)
       |        }
       |
       |        pub fn binary_value(value: impl Into<Vec<u8>>) -> Self {
       |            Self::Binary(value.into())
       |        }
       |
       |        pub fn any_value(value: impl Into<String>) -> Self {
       |            Self::Any(value.into())
       |        }
       |
       |        pub fn type_(&self) -> Type {
       |            match self {
       |                Self::Null(type_) => type_.clone(),
       |                Self::String(_) => Type::String,
       |                Self::Integer(_) => Type::Integer,
       |                Self::Long(_) => Type::Long,
       |                Self::Double(_) => Type::Double,
       |                Self::Boolean(_) => Type::Boolean,
       |                Self::TimestampMillis(_) => Type::Timestamp,
       |                Self::Binary(_) => Type::Binary,
       |                Self::Any(_) => Type::Any,
       |            }
       |        }
       |
       |        pub fn is_null(&self) -> bool {
       |            matches!(self, Self::Null(_))
       |        }
       |
       |        pub fn as_string(&self) -> Result<&str, String> {
       |            match self {
       |                Self::String(value) | Self::Any(value) => Ok(value.as_str()),
       |                Self::Null(_) => Err("value is null".to_string()),
       |                _ => Err("value is not a string".to_string()),
       |            }
       |        }
       |
       |        pub fn as_binary(&self) -> Result<&[u8], String> {
       |            match self {
       |                Self::Binary(value) => Ok(value.as_slice()),
       |                Self::Null(_) => Err("value is null".to_string()),
       |                _ => Err("value is not binary".to_string()),
       |            }
       |        }
       |
       |        pub fn as_int(&self) -> Result<i32, String> {
       |            let value = self.as_long()?;
       |            if value < i32::MIN as i64 || value > i32::MAX as i64 {
       |                Err("value is outside integer range".to_string())
       |            } else {
       |                Ok(value as i32)
       |            }
       |        }
       |
       |        pub fn as_long(&self) -> Result<i64, String> {
       |            match self {
       |                Self::Integer(value) => Ok(*value as i64),
       |                Self::Long(value) | Self::TimestampMillis(value) => Ok(*value),
       |                Self::Double(value) => Ok(*value as i64),
       |                Self::Boolean(value) => Ok(if *value { 1 } else { 0 }),
       |                Self::Null(_) => Err("value is null".to_string()),
       |                _ => Err("value is not numeric".to_string()),
       |            }
       |        }
       |
       |        pub fn as_double(&self) -> Result<f64, String> {
       |            match self {
       |                Self::Integer(value) => Ok(*value as f64),
       |                Self::Long(value) => Ok(*value as f64),
       |                Self::Double(value) => Ok(*value),
       |                Self::Boolean(value) => Ok(if *value { 1.0 } else { 0.0 }),
       |                Self::Null(_) => Err("value is null".to_string()),
       |                _ => Err("value is not numeric".to_string()),
       |            }
       |        }
       |
       |        pub fn as_bool(&self) -> Result<bool, String> {
       |            match self {
       |                Self::Boolean(value) => Ok(*value),
       |                Self::Integer(value) => Ok(*value != 0),
       |                Self::Long(value) | Self::TimestampMillis(value) => Ok(*value != 0),
       |                Self::Double(value) => Ok(*value != 0.0),
       |                Self::Null(_) => Err("value is null".to_string()),
       |                _ => Err("value is not boolean-compatible".to_string()),
       |            }
       |        }
       |
       |        pub fn as_timestamp_millis(&self) -> Result<i64, String> {
       |            match self {
       |                Self::TimestampMillis(value) | Self::Long(value) => Ok(*value),
       |                Self::Integer(value) => Ok(*value as i64),
       |                Self::Null(_) => Err("value is null".to_string()),
       |                _ => Err("value is not a timestamp".to_string()),
       |            }
       |        }
       |    }
       |
       |    #[derive(Clone, Debug)]
       |    pub struct Tuple {
       |        column_names: Vec<String>,
       |        column_types: Vec<Type>,
       |        values: Vec<Value>,
       |    }
       |
       |    impl Tuple {
       |        pub fn new(column_names: &[String], column_types: &[Type], values: Vec<Value>) -> Self {
       |            Self {
       |                column_names: column_names.to_vec(),
       |                column_types: column_types.to_vec(),
       |                values,
       |            }
       |        }
       |
       |        pub fn get(&self, index: usize) -> Result<&Value, String> {
       |            self.values
       |                .get(index)
       |                .ok_or_else(|| "tuple column index is out of range".to_string())
       |        }
       |
       |        pub fn get_by_name(&self, name: &str) -> Result<&Value, String> {
       |            let index = self
       |                .column_names
       |                .iter()
       |                .position(|column| column == name)
       |                .ok_or_else(|| format!("tuple column '{}' does not exist", name))?;
       |            self.get(index)
       |        }
       |
       |        pub fn size(&self) -> usize {
       |            self.values.len()
       |        }
       |
       |        pub fn len(&self) -> usize {
       |            self.values.len()
       |        }
       |
       |        pub fn column_type(&self, index: usize) -> Result<&Type, String> {
       |            self.column_types
       |                .get(index)
       |                .ok_or_else(|| "tuple column index is out of range".to_string())
       |        }
       |    }
       |
       |    pub type TupleLike = Vec<Value>;
       |    pub type TupleOutput = Vec<TupleLike>;
       |    pub type Batch = Vec<Tuple>;
       |    pub type BatchLike = Vec<TupleLike>;
       |    pub type Table = Vec<Tuple>;
       |    pub type TableLike = Vec<TupleLike>;
       |
       |    pub trait UDFOperator {
       |        fn process_tuple(&mut self, tuple: &Tuple, port: i32) -> Result<TupleOutput, String> {
       |            let _ = tuple;
       |            let _ = port;
       |            Err("process_tuple is not implemented".to_string())
       |        }
       |
       |        fn process_batch(&mut self, batch: &Batch, port: i32) -> Result<BatchLike, String> {
       |            let mut output = Vec::new();
       |            for tuple in batch {
       |                output.extend(self.process_tuple(tuple, port)?);
       |            }
       |            Ok(output)
       |        }
       |
       |        fn process_table(&mut self, table: &Table, port: i32) -> Result<TableLike, String> {
       |            let mut output = Vec::new();
       |            for chunk in table.chunks(1024) {
       |                let batch: Batch = chunk.to_vec();
       |                output.extend(self.process_batch(&batch, port)?);
       |            }
       |            Ok(output)
       |        }
       |    }
       |
       |    pub fn split_tab(line: &str) -> Vec<String> {
       |        if line.is_empty() {
       |            Vec::new()
       |        } else {
       |            line.split('\\t').map(|value| value.to_string()).collect()
       |        }
       |    }
       |
       |    pub fn base64_encode(input: &[u8]) -> String {
       |        const ALPHABET: &[u8; 64] =
       |            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
       |        let mut output = String::new();
       |        let mut value: u32 = 0;
       |        let mut bits: i32 = -6;
       |        for byte in input {
       |            value = (value << 8) + (*byte as u32);
       |            bits += 8;
       |            while bits >= 0 {
       |                output.push(ALPHABET[((value >> (bits as u32)) & 0x3F) as usize] as char);
       |                bits -= 6;
       |            }
       |        }
       |        if bits > -6 {
       |            output.push(ALPHABET[(((value << 8) >> ((bits + 8) as u32)) & 0x3F) as usize] as char);
       |        }
       |        while output.len() % 4 != 0 {
       |            output.push('=');
       |        }
       |        output
       |    }
       |
       |    pub fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
       |        const ALPHABET: &[u8; 64] =
       |            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
       |        let mut table = [-1_i16; 256];
       |        for (index, byte) in ALPHABET.iter().enumerate() {
       |            table[*byte as usize] = index as i16;
       |        }
       |
       |        let mut output = Vec::new();
       |        let mut value: i32 = 0;
       |        let mut bits: i32 = -8;
       |        for byte in input.bytes() {
       |            if byte == b'=' {
       |                break;
       |            }
       |            let decoded = table[byte as usize];
       |            if decoded < 0 {
       |                return Err("invalid base64 input".to_string());
       |            }
       |            value = (value << 6) + decoded as i32;
       |            bits += 6;
       |            if bits >= 0 {
       |                output.push(((value >> (bits as u32)) & 0xFF) as u8);
       |                bits -= 8;
       |            }
       |        }
       |        Ok(output)
       |    }
       |
       |    pub fn parse_wire_value(token: &str) -> Result<Value, String> {
       |        let bytes = token.as_bytes();
       |        if bytes.len() < 4 || bytes[1] != b':' || bytes[3] != b':' {
       |            return Err("invalid Texera wire value".to_string());
       |        }
       |        let type_ = type_from_tag(bytes[0] as char)?;
       |        let is_null = bytes[2] == b'1';
       |        let payload = &token[4..];
       |        if is_null {
       |            return Ok(Value::null_value(type_));
       |        }
       |
       |        match type_ {
       |            Type::String => String::from_utf8(base64_decode(payload)?)
       |                .map(|value| Value::string_value(value))
       |                .map_err(|error| error.to_string()),
       |            Type::Integer => payload
       |                .parse::<i32>()
       |                .map(Value::integer_value)
       |                .map_err(|error| error.to_string()),
       |            Type::Long => payload
       |                .parse::<i64>()
       |                .map(Value::long_value)
       |                .map_err(|error| error.to_string()),
       |            Type::Double => payload
       |                .parse::<f64>()
       |                .map(Value::double_value)
       |                .map_err(|error| error.to_string()),
       |            Type::Boolean => Ok(Value::boolean_value(payload == "1" || payload == "true")),
       |            Type::Timestamp => payload
       |                .parse::<i64>()
       |                .map(Value::timestamp_millis)
       |                .map_err(|error| error.to_string()),
       |            Type::Binary => Ok(Value::binary_value(base64_decode(payload)?)),
       |            Type::Any => String::from_utf8(base64_decode(payload)?)
       |                .map(|value| Value::any_value(value))
       |                .map_err(|error| error.to_string()),
       |        }
       |    }
       |
       |    pub fn serialize_wire_value(value: &Value) -> String {
       |        let type_ = value.type_();
       |        let mut output = String::new();
       |        output.push(type_tag(&type_));
       |        output.push_str(if value.is_null() { ":1:" } else { ":0:" });
       |        if value.is_null() {
       |            return output;
       |        }
       |
       |        match value {
       |            Value::String(value) | Value::Any(value) => output.push_str(&base64_encode(value.as_bytes())),
       |            Value::Integer(value) => output.push_str(&value.to_string()),
       |            Value::Long(value) | Value::TimestampMillis(value) => output.push_str(&value.to_string()),
       |            Value::Double(value) => output.push_str(&format!("{:.17}", value)),
       |            Value::Boolean(value) => output.push_str(if *value { "1" } else { "0" }),
       |            Value::Binary(value) => output.push_str(&base64_encode(value)),
       |            Value::Null(_) => {}
       |        }
       |        output
       |    }
       |}
       |
       |$code
       |
       |fn read_required_line<I>(lines: &mut I, message: &str) -> Result<String, String>
       |where
       |    I: Iterator<Item = std::io::Result<String>>,
       |{
       |    match lines.next() {
       |        Some(line) => line.map_err(|error| error.to_string()),
       |        None => Err(message.to_string()),
       |    }
       |}
       |
       |fn run() -> Result<(), String> {
       |    let stdin = io::stdin();
       |    let mut lines = stdin.lock().lines();
       |
       |    let column_count_line = match lines.next() {
       |        Some(line) => line.map_err(|error| error.to_string())?,
       |        None => return Ok(()),
       |    };
       |    let column_count = column_count_line
       |        .parse::<usize>()
       |        .map_err(|error| error.to_string())?;
       |
       |    let names_line = read_required_line(&mut lines, "Missing Rust UDF input schema header")?;
       |    let types_line = read_required_line(&mut lines, "Missing Rust UDF input schema header")?;
       |
       |    let encoded_names = texera::split_tab(&names_line);
       |    let encoded_types = texera::split_tab(&types_line);
       |    if encoded_names.len() != column_count || encoded_types.len() != column_count {
       |        return Err("Rust UDF schema header column count mismatch".to_string());
       |    }
       |
       |    let mut column_names = Vec::with_capacity(column_count);
       |    let mut column_types = Vec::with_capacity(column_count);
       |    for index in 0..column_count {
       |        let name_bytes = texera::base64_decode(&encoded_names[index])?;
       |        column_names.push(String::from_utf8(name_bytes).map_err(|error| error.to_string())?);
       |        let mut chars = encoded_types[index].chars();
       |        let type_tag = chars
       |            .next()
       |            .ok_or_else(|| "Invalid Rust UDF input type tag".to_string())?;
       |        if chars.next().is_some() {
       |            return Err("Invalid Rust UDF input type tag".to_string());
       |        }
       |        column_types.push(texera::type_from_tag(type_tag)?);
       |    }
       |
       |    let mut op = TexeraUDFOperator::default();
       |    let stdout = io::stdout();
       |    let mut protocol_out = stdout.lock();
       |
       |    while let Some(mode_line) = lines.next() {
       |        let mode = mode_line.map_err(|error| error.to_string())?;
       |        let row_count_line = read_required_line(&mut lines, "Missing Rust UDF frame row count")?;
       |        let row_count = row_count_line
       |            .parse::<usize>()
       |            .map_err(|error| error.to_string())?;
       |
       |        let mut table = Vec::with_capacity(row_count);
       |        for _ in 0..row_count {
       |            let line = read_required_line(&mut lines, "Missing Rust UDF input row")?;
       |            let fields = texera::split_tab(&line);
       |            if fields.len() != column_count {
       |                return Err(format!(
       |                    "Expected {} input values, got {}",
       |                    column_count,
       |                    fields.len()
       |                ));
       |            }
       |
       |            let mut values = Vec::with_capacity(column_count);
       |            for field in fields {
       |                values.push(texera::parse_wire_value(&field)?);
       |            }
       |            table.push(texera::Tuple::new(&column_names, &column_types, values));
       |        }
       |
       |        let output = match mode.as_str() {
       |            "tuple" => {
       |                let mut output = Vec::new();
       |                for tuple in &table {
       |                    output.extend(texera::UDFOperator::process_tuple(&mut op, tuple, 0)?);
       |                }
       |                output
       |            }
       |            "batch" => texera::UDFOperator::process_batch(&mut op, &table, 0)?,
       |            "table" => texera::UDFOperator::process_table(&mut op, &table, 0)?,
       |            _ => return Err(format!("unknown Rust UDF execution mode: {}", mode)),
       |        };
       |
       |        writeln!(&mut protocol_out, "{}", output.len()).map_err(|error| error.to_string())?;
       |        for tuple_like in output {
       |            let encoded = tuple_like
       |                .iter()
       |                .map(texera::serialize_wire_value)
       |                .collect::<Vec<_>>();
       |            writeln!(&mut protocol_out, "{}", encoded.join("\\t"))
       |                .map_err(|error| error.to_string())?;
       |        }
       |        protocol_out.flush().map_err(|error| error.to_string())?;
       |    }
       |    Ok(())
       |}
       |
       |fn main() {
       |    if let Err(error) = run() {
       |        eprintln!("Rust UDF threw an error: {}", error);
       |        process::exit(6);
       |    }
       |}
       |""".stripMargin

  private def splitCompilerFlags(flags: String): Seq[String] =
    Option(flags).map(_.trim).filter(_.nonEmpty).map(_.split("\\s+").toSeq).getOrElse(Seq.empty)

  private def executableFileName: String =
    if (System.getProperty("os.name").toLowerCase.contains("win")) "compiled_udf.exe"
    else "compiled_udf"

  private def readIfExists(path: Path): String =
    if (Files.exists(path)) Files.readString(path, StandardCharsets.UTF_8) else ""

  private def compilationException(
      message: String,
      cause: Throwable
  ): RuntimeException =
    new RuntimeException(
      s"Rust compilation failed:\n$message\n\nCheck that your code defines a Rust operator and type alias, for example:\ntype TexeraUDFOperator = ProcessTupleOperator;",
      cause
    )

  private def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(byte => "%02x".format(byte & 0xff)).mkString
  }
}
