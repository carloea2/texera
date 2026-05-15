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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.SeqHasAsJava

case class CompiledCppUDFCompileRequest(
    code: String,
    inputColumns: List[String],
    retainInputColumns: Boolean,
    outputColumns: List[String],
    compilerFlags: String,
    timeoutMs: Int
)

object CompiledCppUDFCompiler {
  private val CacheRoot =
    Paths.get(System.getProperty("java.io.tmpdir"), "texera-compiled-cpp-udf")

  def compile(request: CompiledCppUDFCompileRequest): Path = {
    val compiler = sys.env.getOrElse("CXX", "g++")
    val compilerVersionText = compilerVersion(compiler)
    val cacheKey = sha256(
      List(
        request.code,
        request.inputColumns.mkString("\u0000"),
        request.retainInputColumns.toString,
        request.outputColumns.mkString("\u0000"),
        request.compilerFlags,
        compilerVersionText
      ).mkString("\u0001")
    )
    val cacheDir = CacheRoot.resolve(cacheKey)
    Files.createDirectories(cacheDir)

    val cppPath = cacheDir.resolve("compiled_udf.cpp")
    val executablePath = cacheDir.resolve(executableFileName)
    if (Files.exists(executablePath)) {
      return executablePath
    }

    Files.writeString(
      cppPath,
      generatedSource(request.code),
      StandardCharsets.UTF_8
    )

    val stdoutPath = cacheDir.resolve("compile.stdout")
    val stderrPath = cacheDir.resolve("compile.stderr")
    val command =
      Seq(compiler) ++ splitCompilerFlags(request.compilerFlags) ++ Seq(
        "-std=c++17",
        cppPath.toString,
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

  private[cpp] def isCompilerAvailable: Boolean = {
    val compiler = sys.env.getOrElse("CXX", "g++")
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
    val stdoutPath = Files.createTempFile("texera-cpp-compiler-version", ".out")
    val stderrPath = Files.createTempFile("texera-cpp-compiler-version", ".err")
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
    s"""#include <algorithm>
       |#include <cstddef>
       |#include <cstdint>
       |#include <exception>
       |#include <iomanip>
       |#include <iostream>
       |#include <limits>
       |#include <stdexcept>
       |#include <sstream>
       |#include <string>
       |#include <utility>
       |#include <vector>
       |
       |namespace texera {
       |
       |enum class Type {
       |  String,
       |  Integer,
       |  Long,
       |  Double,
       |  Boolean,
       |  Timestamp,
       |  Binary,
       |  Any
       |};
       |
       |inline const char* type_name(Type type) {
       |  switch (type) {
       |    case Type::String: return "string";
       |    case Type::Integer: return "integer";
       |    case Type::Long: return "long";
       |    case Type::Double: return "double";
       |    case Type::Boolean: return "boolean";
       |    case Type::Timestamp: return "timestamp";
       |    case Type::Binary: return "binary";
       |    case Type::Any: return "any";
       |  }
       |  return "unknown";
       |}
       |
       |inline char type_tag(Type type) {
       |  switch (type) {
       |    case Type::String: return 'S';
       |    case Type::Integer: return 'I';
       |    case Type::Long: return 'L';
       |    case Type::Double: return 'D';
       |    case Type::Boolean: return 'O';
       |    case Type::Timestamp: return 'T';
       |    case Type::Binary: return 'B';
       |    case Type::Any: return 'A';
       |  }
       |  return 'A';
       |}
       |
       |inline Type type_from_tag(char tag) {
       |  switch (tag) {
       |    case 'S': return Type::String;
       |    case 'I': return Type::Integer;
       |    case 'L': return Type::Long;
       |    case 'D': return Type::Double;
       |    case 'O': return Type::Boolean;
       |    case 'T': return Type::Timestamp;
       |    case 'B': return Type::Binary;
       |    case 'A': return Type::Any;
       |    default: throw std::invalid_argument("unknown Texera value type tag");
       |  }
       |}
       |
       |class Value {
       | public:
       |  static Value null_value(Type type = Type::Any) {
       |    Value value(type);
       |    value.is_null_ = true;
       |    return value;
       |  }
       |
       |  static Value string_value(std::string value) {
       |    Value result(Type::String);
       |    result.text_value_ = std::move(value);
       |    return result;
       |  }
       |
       |  static Value integer_value(int value) {
       |    Value result(Type::Integer);
       |    result.long_value_ = value;
       |    return result;
       |  }
       |
       |  static Value long_value(long long value) {
       |    Value result(Type::Long);
       |    result.long_value_ = value;
       |    return result;
       |  }
       |
       |  static Value double_value(double value) {
       |    Value result(Type::Double);
       |    result.double_value_ = value;
       |    return result;
       |  }
       |
       |  static Value boolean_value(bool value) {
       |    Value result(Type::Boolean);
       |    result.bool_value_ = value;
       |    return result;
       |  }
       |
       |  static Value timestamp_millis(long long value) {
       |    Value result(Type::Timestamp);
       |    result.long_value_ = value;
       |    return result;
       |  }
       |
       |  static Value binary_value(std::string bytes) {
       |    Value result(Type::Binary);
       |    result.text_value_ = std::move(bytes);
       |    return result;
       |  }
       |
       |  static Value any_value(std::string value) {
       |    Value result(Type::Any);
       |    result.text_value_ = std::move(value);
       |    return result;
       |  }
       |
       |  Type type() const { return type_; }
       |  bool is_null() const { return is_null_; }
       |
       |  const std::string& as_string() const {
       |    require_not_null();
       |    if (type_ != Type::String && type_ != Type::Any) {
       |      throw std::runtime_error("value is not a string");
       |    }
       |    return text_value_;
       |  }
       |
       |  const std::string& as_binary() const {
       |    require_not_null();
       |    if (type_ != Type::Binary) {
       |      throw std::runtime_error("value is not binary");
       |    }
       |    return text_value_;
       |  }
       |
       |  int as_int() const {
       |    long long value = as_long();
       |    if (value < std::numeric_limits<int>::min() || value > std::numeric_limits<int>::max()) {
       |      throw std::runtime_error("value is outside integer range");
       |    }
       |    return static_cast<int>(value);
       |  }
       |
       |  long long as_long() const {
       |    require_not_null();
       |    switch (type_) {
       |      case Type::Integer:
       |      case Type::Long:
       |      case Type::Timestamp:
       |        return long_value_;
       |      case Type::Double:
       |        return static_cast<long long>(double_value_);
       |      case Type::Boolean:
       |        return bool_value_ ? 1LL : 0LL;
       |      default:
       |        throw std::runtime_error("value is not numeric");
       |    }
       |  }
       |
       |  double as_double() const {
       |    require_not_null();
       |    switch (type_) {
       |      case Type::Integer:
       |      case Type::Long:
       |        return static_cast<double>(long_value_);
       |      case Type::Double:
       |        return double_value_;
       |      case Type::Boolean:
       |        return bool_value_ ? 1.0 : 0.0;
       |      default:
       |        throw std::runtime_error("value is not numeric");
       |    }
       |  }
       |
       |  bool as_bool() const {
       |    require_not_null();
       |    switch (type_) {
       |      case Type::Boolean:
       |        return bool_value_;
       |      case Type::Integer:
       |      case Type::Long:
       |      case Type::Timestamp:
       |        return long_value_ != 0;
       |      case Type::Double:
       |        return double_value_ != 0.0;
       |      default:
       |        throw std::runtime_error("value is not boolean-compatible");
       |    }
       |  }
       |
       |  long long as_timestamp_millis() const {
       |    require_not_null();
       |    if (type_ == Type::Timestamp || type_ == Type::Long || type_ == Type::Integer) {
       |      return long_value_;
       |    }
       |    throw std::runtime_error("value is not a timestamp");
       |  }
       |
       | private:
       |  explicit Value(Type type)
       |      : type_(type), is_null_(false), long_value_(0), double_value_(0.0), bool_value_(false) {}
       |
       |  void require_not_null() const {
       |    if (is_null_) {
       |      throw std::runtime_error("value is null");
       |    }
       |  }
       |
       |  Type type_;
       |  bool is_null_;
       |  std::string text_value_;
       |  long long long_value_;
       |  double double_value_;
       |  bool bool_value_;
       |
       |  friend Value parse_wire_value(const std::string& token);
       |  friend std::string serialize_wire_value(const Value& value);
       |};
       |
       |struct Tuple {
       |  const std::vector<std::string>* column_names;
       |  const std::vector<Type>* column_types;
       |  std::vector<Value> values;
       |
       |  Tuple(const std::vector<std::string>& names,
       |        const std::vector<Type>& types,
       |        std::vector<Value> row_values)
       |      : column_names(&names), column_types(&types), values(std::move(row_values)) {}
       |
       |  const Value& get(std::size_t index) const {
       |    if (index >= values.size()) {
       |      throw std::out_of_range("tuple column index is out of range");
       |    }
       |    return values[index];
       |  }
       |
       |  const Value& get(const std::string& name) const {
       |    for (std::size_t i = 0; i < column_names->size(); ++i) {
       |      if ((*column_names)[i] == name) {
       |        return get(i);
       |      }
       |    }
       |    throw std::invalid_argument("tuple column '" + name + "' does not exist");
       |  }
       |
       |  std::size_t size() const { return values.size(); }
       |};
       |
       |using TupleLike = std::vector<Value>;
       |using TupleOutput = std::vector<TupleLike>;
       |using Batch = std::vector<Tuple>;
       |using BatchLike = std::vector<TupleLike>;
       |using Table = std::vector<Tuple>;
       |using TableLike = std::vector<TupleLike>;
       |
       |class UDFOperator {
       | public:
       |  virtual ~UDFOperator() = default;
       |
       |  virtual TupleOutput process_tuple(const Tuple& tuple, int port) {
       |    (void) tuple;
       |    (void) port;
       |    throw std::runtime_error("process_tuple is not implemented");
       |  }
       |
       |  virtual BatchLike process_batch(const Batch& batch, int port) {
       |    BatchLike output;
       |    for (const auto& tuple : batch) {
       |      TupleOutput tuple_output = process_tuple(tuple, port);
       |      output.insert(output.end(), tuple_output.begin(), tuple_output.end());
       |    }
       |    return output;
       |  }
       |
       |  virtual TableLike process_table(const Table& table, int port) {
       |    constexpr std::size_t batch_size = 1024;
       |    TableLike output;
       |    for (std::size_t start = 0; start < table.size(); start += batch_size) {
       |      const std::size_t end = std::min(start + batch_size, table.size());
       |      Batch batch(table.begin() + static_cast<std::ptrdiff_t>(start),
       |                  table.begin() + static_cast<std::ptrdiff_t>(end));
       |      BatchLike batch_output = process_batch(batch, port);
       |      output.insert(output.end(), batch_output.begin(), batch_output.end());
       |    }
       |    return output;
       |  }
       |};
       |
       |inline std::vector<std::string> split_tab(const std::string& line) {
       |  std::vector<std::string> parts;
       |  std::stringstream row(line);
       |  std::string cell;
       |  while (std::getline(row, cell, '\\t')) {
       |    parts.push_back(cell);
       |  }
       |  if (!line.empty() && line.back() == '\\t') {
       |    parts.emplace_back();
       |  }
       |  return parts;
       |}
       |
       |inline std::string base64_encode(const std::string& input) {
       |  static const char alphabet[] =
       |      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
       |  std::string output;
       |  int value = 0;
       |  int bits = -6;
       |  for (unsigned char c : input) {
       |    value = (value << 8) + c;
       |    bits += 8;
       |    while (bits >= 0) {
       |      output.push_back(alphabet[(value >> bits) & 0x3F]);
       |      bits -= 6;
       |    }
       |  }
       |  if (bits > -6) {
       |    output.push_back(alphabet[((value << 8) >> (bits + 8)) & 0x3F]);
       |  }
       |  while (output.size() % 4) {
       |    output.push_back('=');
       |  }
       |  return output;
       |}
       |
       |inline std::string base64_decode(const std::string& input) {
       |  static const std::string alphabet =
       |      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
       |  std::vector<int> table(256, -1);
       |  for (int i = 0; i < 64; ++i) {
       |    table[static_cast<unsigned char>(alphabet[i])] = i;
       |  }
       |  std::string output;
       |  int value = 0;
       |  int bits = -8;
       |  for (unsigned char c : input) {
       |    if (c == '=') {
       |      break;
       |    }
       |    if (table[c] == -1) {
       |      throw std::invalid_argument("invalid base64 input");
       |    }
       |    value = (value << 6) + table[c];
       |    bits += 6;
       |    if (bits >= 0) {
       |      output.push_back(static_cast<char>((value >> bits) & 0xFF));
       |      bits -= 8;
       |    }
       |  }
       |  return output;
       |}
       |
       |inline Value parse_wire_value(const std::string& token) {
       |  if (token.size() < 4 || token[1] != ':' || token[3] != ':') {
       |    throw std::invalid_argument("invalid Texera wire value");
       |  }
       |  Type type = type_from_tag(token[0]);
       |  bool is_null = token[2] == '1';
       |  const std::string payload = token.substr(4);
       |  if (is_null) {
       |    return Value::null_value(type);
       |  }
       |  switch (type) {
       |    case Type::String:
       |      return Value::string_value(base64_decode(payload));
       |    case Type::Integer:
       |      return Value::integer_value(std::stoi(payload));
       |    case Type::Long:
       |      return Value::long_value(std::stoll(payload));
       |    case Type::Double:
       |      return Value::double_value(std::stod(payload));
       |    case Type::Boolean:
       |      return Value::boolean_value(payload == "1" || payload == "true");
       |    case Type::Timestamp:
       |      return Value::timestamp_millis(std::stoll(payload));
       |    case Type::Binary:
       |      return Value::binary_value(base64_decode(payload));
       |    case Type::Any:
       |      return Value::any_value(base64_decode(payload));
       |  }
       |  throw std::invalid_argument("unsupported Texera wire value");
       |}
       |
       |inline std::string serialize_wire_value(const Value& value) {
       |  std::string output;
       |  output.push_back(type_tag(value.type_));
       |  output += value.is_null_ ? ":1:" : ":0:";
       |  if (value.is_null_) {
       |    return output;
       |  }
       |  switch (value.type_) {
       |    case Type::String:
       |    case Type::Binary:
       |    case Type::Any:
       |      return output + base64_encode(value.text_value_);
       |    case Type::Integer:
       |    case Type::Long:
       |    case Type::Timestamp:
       |      return output + std::to_string(value.long_value_);
       |    case Type::Double: {
       |      std::ostringstream stream;
       |      stream << std::setprecision(17) << value.double_value_;
       |      return output + stream.str();
       |    }
       |    case Type::Boolean:
       |      return output + (value.bool_value_ ? "1" : "0");
       |  }
       |  return output;
       |}
       |
       |}  // namespace texera
       |
       |$code
       |
       |int main() {
       |  try {
       |    std::string column_count_line;
       |    if (!std::getline(std::cin, column_count_line)) {
       |      return 0;
       |    }
       |    const std::size_t column_count =
       |        static_cast<std::size_t>(std::stoul(column_count_line));
       |
       |    std::string names_line;
       |    std::string types_line;
       |    if (!std::getline(std::cin, names_line) || !std::getline(std::cin, types_line)) {
       |      std::cerr << "Missing C++ UDF input schema header" << std::endl;
       |      return 2;
       |    }
       |
       |    std::vector<std::string> encoded_names = texera::split_tab(names_line);
       |    std::vector<std::string> encoded_types = texera::split_tab(types_line);
       |    if (encoded_names.size() != column_count || encoded_types.size() != column_count) {
       |      std::cerr << "C++ UDF schema header column count mismatch" << std::endl;
       |      return 3;
       |    }
       |
       |    std::vector<std::string> column_names;
       |    std::vector<texera::Type> column_types;
       |    column_names.reserve(column_count);
       |    column_types.reserve(column_count);
       |    for (std::size_t i = 0; i < column_count; ++i) {
       |      column_names.push_back(texera::base64_decode(encoded_names[i]));
       |      if (encoded_types[i].size() != 1) {
       |        std::cerr << "Invalid C++ UDF input type tag" << std::endl;
       |        return 4;
       |      }
       |      column_types.push_back(texera::type_from_tag(encoded_types[i][0]));
       |    }
       |
       |    texera::Table table;
       |    std::string line;
       |    while (std::getline(std::cin, line)) {
       |      std::vector<std::string> fields = texera::split_tab(line);
       |      if (fields.size() != column_count) {
       |        std::cerr << "Expected " << column_count << " input values, got " << fields.size()
       |                  << std::endl;
       |        return 5;
       |      }
       |      std::vector<texera::Value> values;
       |      values.reserve(column_count);
       |      for (const std::string& field : fields) {
       |        values.push_back(texera::parse_wire_value(field));
       |      }
       |      table.emplace_back(column_names, column_types, std::move(values));
       |    }
       |
       |    TexeraUDFOperator op;
       |    texera::TableLike output = op.process_table(table, 0);
       |    for (const auto& tuple_like : output) {
       |      for (std::size_t i = 0; i < tuple_like.size(); ++i) {
       |        if (i > 0) {
       |          std::cout << '\\t';
       |        }
       |        std::cout << texera::serialize_wire_value(tuple_like[i]);
       |      }
       |      std::cout << std::endl;
       |    }
       |  } catch (const std::exception& e) {
       |    std::cerr << "C++ UDF threw an exception: " << e.what() << std::endl;
       |    return 6;
       |  }
       |  return 0;
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
      s"C++ compilation failed:\n$message\n\nCheck that your code defines a C++ operator and alias, for example:\nusing TexeraUDFOperator = ProcessTupleOperator;",
      cause
    )

  private def sha256(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))
    digest.map(byte => "%02x".format(byte & 0xff)).mkString
  }
}
