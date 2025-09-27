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

package edu.uci.ics.amber.operator.stablemergesort

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.operator.stablemergesort.StableMergeSortOpDesc.StableSortKey
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import scala.jdk.CollectionConverters._

class StableMergeSortOpExecSpec extends AnyFlatSpec {

  private def schemaOf(attributes: (String, AttributeType)*): Schema = {
    attributes.foldLeft(Schema()) { case (acc, (name, tpe)) => acc.add(new Attribute(name, tpe)) }
  }

  private def tupleOf(schema: Schema, values: (String, Any)*): Tuple = {
    val valueMap = values.toMap
    val builder = Tuple.builder(schema)
    schema.getAttributeNames.foreach { name =>
      builder.add(schema.getAttribute(name), valueMap(name))
    }
    builder.build()
  }

  private def key(
      attribute: String,
      order: String = "asc",
      nulls: String = "last",
      caseInsensitive: Boolean = false
  ): StableSortKey = {
    val k = new StableSortKey()
    k.attribute = attribute
    k.order = order
    k.nulls = nulls
    k.caseInsensitive = caseInsensitive
    k
  }

  private def execute(
      schema: Schema,
      tuples: Seq[Tuple]
  )(configure: StableMergeSortOpDesc => Unit): List[Tuple] = {
    val desc = new StableMergeSortOpDesc()
    configure(desc)
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    tuples.foreach(tuple => exec.processTuple(tuple, 0))
    val result = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
    exec.close()
    result
  }

  "StableMergeSortOpExec" should "sort integers ascending and preserve duplicate order" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> 3, "label" -> "a"),
      tupleOf(schema, "value" -> 1, "label" -> "first-1"),
      tupleOf(schema, "value" -> 2, "label" -> "b"),
      tupleOf(schema, "value" -> 1, "label" -> "first-2"),
      tupleOf(schema, "value" -> 3, "label" -> "c")
    )

    val result = execute(schema, tuples) { desc =>
      desc.keys = List(key("value")).asJava
    }

    assert(result.map(_.getField[Int]("value")) == List(1, 1, 2, 3, 3))
    val labelsForOnes = result.filter(_.getField[Int]("value") == 1).map(_.getField[String]("label"))
    assert(labelsForOnes == List("first-1", "first-2"))
  }

  it should "sort integers descending while preserving stability" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> 2, "label" -> "first"),
      tupleOf(schema, "value" -> 2, "label" -> "second"),
      tupleOf(schema, "value" -> 1, "label" -> "third"),
      tupleOf(schema, "value" -> 3, "label" -> "fourth")
    )

    val result = execute(schema, tuples) { desc =>
      desc.keys = List(key("value", order = "desc")).asJava
    }

    assert(result.map(_.getField[Int]("value")) == List(3, 2, 2, 1))
    val labelsForTwos = result.filter(_.getField[Int]("value") == 2).map(_.getField[String]("label"))
    assert(labelsForTwos == List("first", "second"))
  }

  it should "handle case-sensitive and case-insensitive string ordering" in {
    val schema = schemaOf("name" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "name" -> "apple"),
      tupleOf(schema, "name" -> "Banana"),
      tupleOf(schema, "name" -> "banana"),
      tupleOf(schema, "name" -> "APPLE")
    )

    val sensitive = execute(schema, tuples) { desc =>
      desc.keys = List(key("name", order = "asc", caseInsensitive = false)).asJava
    }
    assert(sensitive.map(_.getField[String]("name")) == List("APPLE", "Banana", "apple", "banana"))

    val insensitive = execute(schema, tuples) { desc =>
      desc.keys = List(key("name", caseInsensitive = true)).asJava
    }
    assert(insensitive.map(_.getField[String]("name")) == List("apple", "APPLE", "Banana", "banana"))
  }

  it should "respect null ordering preferences" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> null, "label" -> "null-1"),
      tupleOf(schema, "value" -> 5, "label" -> "five"),
      tupleOf(schema, "value" -> null, "label" -> "null-2"),
      tupleOf(schema, "value" -> 3, "label" -> "three")
    )

    val nullsFirst = execute(schema, tuples) { desc =>
      desc.keys = List(key("value", nulls = "first")).asJava
    }
    assert(nullsFirst.map(_.getField[String]("label")) == List("null-1", "null-2", "three", "five"))

    val nullsLast = execute(schema, tuples) { desc =>
      desc.keys = List(key("value", nulls = "last")).asJava
    }
    assert(nullsLast.map(_.getField[String]("label")) == List("three", "five", "null-1", "null-2"))
  }

  it should "support multi-key sorting with mixed attribute types" in {
    val schema = schemaOf(
      "dept" -> AttributeType.STRING,
      "score" -> AttributeType.DOUBLE,
      "name" -> AttributeType.STRING,
      "hired" -> AttributeType.TIMESTAMP
    )

    val baseTime = Timestamp.valueOf("2020-01-01 00:00:00")
    val tuples = List(
      tupleOf(
        schema,
        "dept" -> "Sales",
        "score" -> 9.5,
        "name" -> "Alice",
        "hired" -> baseTime
      ),
      tupleOf(
        schema,
        "dept" -> "Sales",
        "score" -> 9.5,
        "name" -> "Bob",
        "hired" -> new Timestamp(baseTime.getTime + 1000)
      ),
      tupleOf(
        schema,
        "dept" -> "Sales",
        "score" -> 8.0,
        "name" -> "Carol",
        "hired" -> new Timestamp(baseTime.getTime + 2000)
      ),
      tupleOf(
        schema,
        "dept" -> "Engineering",
        "score" -> 9.5,
        "name" -> "Dave",
        "hired" -> new Timestamp(baseTime.getTime + 3000)
      ),
      tupleOf(
        schema,
        "dept" -> null,
        "score" -> 9.5,
        "name" -> "Eve",
        "hired" -> new Timestamp(baseTime.getTime + 4000)
      )
    )

    val result = execute(schema, tuples) { desc =>
      desc.keys = List(
        key("dept", nulls = "last"),
        key("score", order = "desc"),
        key("name", caseInsensitive = true)
      ).asJava
    }

    assert(
      result.map(_.getField[String]("name")) ==
        List("Dave", "Alice", "Bob", "Carol", "Eve")
    )
  }

  it should "apply offset and limit after sorting" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = (1 to 10).map { i =>
      tupleOf(schema, "value" -> i, "label" -> s"row-$i")
    }

    val result = execute(schema, tuples) { desc =>
      desc.keys = List(key("value")).asJava
      desc.offset = 3
      desc.limit = Int.box(4)
    }

    assert(result.map(_.getField[Int]("value")) == List(4, 5, 6, 7))
  }

  it should "fail when a sort key attribute is missing" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val tuple = tupleOf(schema, "value" -> 1)

    val desc = new StableMergeSortOpDesc()
    desc.keys = List(key("missing")).asJava
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    val exception = intercept[IllegalArgumentException] {
      exec.processTuple(tuple, 0)
    }
    assert(exception.getMessage.contains("does not exist"))
    exec.close()
  }

  it should "fail when an unsupported attribute type is used" in {
    val schema = schemaOf("data" -> AttributeType.BINARY)
    val tuple = tupleOf(schema, "data" -> Array[Byte](1, 2, 3))

    val desc = new StableMergeSortOpDesc()
    desc.keys = List(key("data")).asJava
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    val exception = intercept[IllegalArgumentException] {
      exec.processTuple(tuple, 0)
    }
    assert(exception.getMessage.contains("unsupported type"))
    exec.close()
  }

  it should "sort large inputs efficiently" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = (50000 to 1 by -1).map { i =>
      tupleOf(schema, "value" -> i, "label" -> s"row-$i")
    }

    val result = execute(schema, tuples) { desc =>
      desc.keys = List(key("value")).asJava
    }

    assert(result.head.getField[Int]("value") == 1)
    assert(result(1).getField[Int]("value") == 2)
    assert(result.takeRight(2).map(_.getField[Int]("value")) == List(49999, 50000))
  }

  it should "preserve original order among tuples with equal keys" in {
    val schema = schemaOf("key" -> AttributeType.INTEGER, "index" -> AttributeType.INTEGER)
    val tuples = (0 until 100).map { i =>
      tupleOf(schema, "key" -> (i % 5), "index" -> i)
    }

    val result = execute(schema, tuples) { desc =>
      desc.keys = List(key("key")).asJava
    }

    val grouped = result.groupBy(_.getField[Int]("key")).values
    grouped.foreach { group =>
      val indices = group.map(_.getField[Int]("index"))
      assert(indices == indices.sorted)
    }
  }

  it should "buffer tuples until onFinish is called" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val tuple = tupleOf(schema, "value" -> 2)

    val desc = new StableMergeSortOpDesc()
    desc.keys = List(key("value")).asJava
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    val immediate = exec.processTuple(tuple, 0)
    assert(immediate.isEmpty)
    val result = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
    assert(result.map(_.getField[Int]("value")) == List(2))
    exec.close()
  }
}
