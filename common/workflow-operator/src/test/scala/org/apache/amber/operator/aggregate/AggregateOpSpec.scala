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

package org.apache.amber.operator.aggregate

import org.apache.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.amber.util.JSONUtils.objectMapper
import org.scalatest.funsuite.AnyFunSuite

class AggregateOpSpec extends AnyFunSuite {

  /** Helpers */

  private def makeAggregationOp(
      fn: AggregationFunction,
      attributeName: String,
      resultName: String
  ): AggregationOperation = {
    val op = new AggregationOperation()
    op.aggFunction = fn
    op.attribute = attributeName
    op.resultAttribute = resultName
    op
  }

  private def makeSchema(fields: (String, AttributeType)*): Schema =
    Schema(fields.map { case (n, t) => new Attribute(n, t) }.toList)

  private def makeTuple(schema: Schema, values: Any*): Tuple =
    Tuple(schema, values.toArray)

  test("getAggregationAttribute keeps original type for SUM") {
    val op = makeAggregationOp(AggregationFunction.SUM, "amount", "total_amount")
    val attr = op.getAggregationAttribute(AttributeType.DOUBLE)

    assert(attr.getName == "total_amount")
    assert(attr.getType == AttributeType.DOUBLE)
  }

  test("getAggregationAttribute maps COUNT result to INTEGER regardless of input type") {
    val op = makeAggregationOp(AggregationFunction.COUNT, "quantity", "row_count")
    val attr = op.getAggregationAttribute(AttributeType.LONG)

    assert(attr.getName == "row_count")
    assert(attr.getType == AttributeType.INTEGER)
  }

  test("getAggregationAttribute maps CONCAT result type to STRING") {
    val op = makeAggregationOp(AggregationFunction.CONCAT, "tag", "all_tags")
    val attr = op.getAggregationAttribute(AttributeType.INTEGER)

    assert(attr.getName == "all_tags")
    assert(attr.getType == AttributeType.STRING)
  }

  // ---------------------------------------------------------------------------
  // Basic DistributedAggregation behaviour via AggregationOperation.getAggFunc
  // ---------------------------------------------------------------------------

  test("SUM aggregation over INTEGER column adds values correctly") {
    val schema = makeSchema("amount" -> AttributeType.INTEGER)
    val t1 = makeTuple(schema, 5)
    val t2 = makeTuple(schema, 7)
    val t3 = makeTuple(schema, 3)

    val op = makeAggregationOp(AggregationFunction.SUM, "amount", "total_amount")
    val agg = op.getAggFunc(AttributeType.INTEGER)

    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)
    partial = agg.iterate(partial, t3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == 15)
  }

  test("SUM aggregation over DOUBLE column keeps fractional part") {
    val schema = makeSchema("score" -> AttributeType.DOUBLE)
    val t1 = makeTuple(schema, 1.25)
    val t2 = makeTuple(schema, 2.75)

    val op = makeAggregationOp(AggregationFunction.SUM, "score", "total_score")
    val agg = op.getAggFunc(AttributeType.DOUBLE)

    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)

    val result = agg.finalAgg(partial).asInstanceOf[java.lang.Double].doubleValue()
    assert(math.abs(result - 4.0) < 1e-6)
  }

  test("COUNT aggregation with attribute == null counts all rows") {
    val schema = makeSchema("points" -> AttributeType.INTEGER)
    val t1 = makeTuple(schema, 10)
    val t2 = makeTuple(schema, null)
    val t3 = makeTuple(schema, 20)

    val op = makeAggregationOp(AggregationFunction.COUNT, null, "row_count")
    val agg = op.getAggFunc(AttributeType.INTEGER)

    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)
    partial = agg.iterate(partial, t3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == 3)
  }

  test("COUNT aggregation with attribute set only counts non-null values") {
    val schema = makeSchema("points" -> AttributeType.INTEGER)
    val t1 = makeTuple(schema, 10)
    val t2 = makeTuple(schema, null)
    val t3 = makeTuple(schema, 5)

    val op = makeAggregationOp(AggregationFunction.COUNT, "points", "non_null_points")
    val agg = op.getAggFunc(AttributeType.INTEGER)

    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)
    partial = agg.iterate(partial, t3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == 2)
  }

  test("CONCAT aggregation concatenates string representations with commas") {
    val schema = makeSchema("tag" -> AttributeType.STRING)
    val t1 = makeTuple(schema, "red")
    val t2 = makeTuple(schema, null)
    val t3 = makeTuple(schema, "blue")

    val op = makeAggregationOp(AggregationFunction.CONCAT, "tag", "all_tags")
    val agg = op.getAggFunc(AttributeType.STRING)

    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)
    partial = agg.iterate(partial, t3)

    // concatAgg semantics:
    // "" + "red" -> "red"
    // "red" + "," + "" -> "red,"
    // "red," + "," + "blue" -> "red,,blue"
    val result = agg.finalAgg(partial).asInstanceOf[String]
    assert(result == "red,,blue")
  }

  test("MIN aggregation finds smallest INTEGER and returns null when given no values") {
    val schema = makeSchema("temperature" -> AttributeType.INTEGER)
    val t1 = makeTuple(schema, 10)
    val t2 = makeTuple(schema, -2)
    val t3 = makeTuple(schema, 5)

    val op = makeAggregationOp(AggregationFunction.MIN, "temperature", "min_temp")
    val agg = op.getAggFunc(AttributeType.INTEGER)

    // Empty case: never iterate, just finalize init
    val emptyPartial = agg.init()
    val emptyResult = agg.finalAgg(emptyPartial)
    assert(emptyResult == null)

    // Non-empty case
    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)
    partial = agg.iterate(partial, t3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == -2)
  }

  test("MAX aggregation finds largest LONG value") {
    val schema = makeSchema("latency" -> AttributeType.LONG)
    val t1 = makeTuple(schema, 100L)
    val t2 = makeTuple(schema, 50L)
    val t3 = makeTuple(schema, 250L)

    val op = makeAggregationOp(AggregationFunction.MAX, "latency", "max_latency")
    val agg = op.getAggFunc(AttributeType.LONG)

    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)
    partial = agg.iterate(partial, t3)

    val result = agg.finalAgg(partial).asInstanceOf[java.lang.Long].longValue()
    assert(result == 250L)
  }

  test("AVERAGE aggregation ignores nulls and returns null when all values are null") {
    val schema = makeSchema("price" -> AttributeType.DOUBLE)
    val t1 = makeTuple(schema, 10.0)
    val t2 = makeTuple(schema, null)
    val t3 = makeTuple(schema, 20.0)

    val op = makeAggregationOp(AggregationFunction.AVERAGE, "price", "avg_price")
    val agg = op.getAggFunc(AttributeType.DOUBLE)

    // Mixed null and non-null
    var partial = agg.init()
    partial = agg.iterate(partial, t1)
    partial = agg.iterate(partial, t2)
    partial = agg.iterate(partial, t3)

    val avg = agg.finalAgg(partial).asInstanceOf[java.lang.Double].doubleValue()
    assert(math.abs(avg - 15.0) < 1e-6)

    // All nulls
    val allNull = makeTuple(schema, null)
    var partialAllNull = agg.init()
    partialAllNull = agg.iterate(partialAllNull, allNull)
    val allNullResult = agg.finalAgg(partialAllNull)
    assert(allNullResult == null)
  }

  // ---------------------------------------------------------------------------
  // getFinal behaviour
  // ---------------------------------------------------------------------------

  test("getFinal rewrites COUNT into SUM over the intermediate result attribute") {
    val op = makeAggregationOp(AggregationFunction.COUNT, "price", "price_count")
    val finalOp = op.getFinal

    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "price_count")
    assert(finalOp.resultAttribute == "price_count")
  }

  test("getFinal keeps non-COUNT aggregation function and rewires attribute to resultAttribute") {
    val op = makeAggregationOp(AggregationFunction.SUM, "amount", "total_amount")
    val finalOp = op.getFinal

    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "total_amount")
    assert(finalOp.resultAttribute == "total_amount")
  }

  // ---------------------------------------------------------------------------
  // AggregateOpExec: integration-style tests with groupBy
  // ---------------------------------------------------------------------------

  test("AggregateOpExec groups by a single key and computes SUM per group") {
    // schema: city (group key), sales
    val schema = makeSchema(
      "city" -> AttributeType.STRING,
      "sales" -> AttributeType.INTEGER
    )

    val t1 = makeTuple(schema, "NY", 10)
    val t2 = makeTuple(schema, "SF", 20)
    val t3 = makeTuple(schema, "NY", 5)

    val desc = new AggregateOpDesc()
    val sumAgg = makeAggregationOp(AggregationFunction.SUM, "sales", "total_sales")
    desc.aggregations = List(sumAgg)
    desc.groupByKeys = List("city")

    val descJson = objectMapper.writeValueAsString(desc)

    val exec = new AggregateOpExec(descJson)
    exec.open()
    exec.processTuple(t1, 0)
    exec.processTuple(t2, 0)
    exec.processTuple(t3, 0)

    val results = exec.onFinish(0).toList

    // Expect two output rows: (NY, 15) and (SF, 20)
    val resultMap = results.map { tupleLike =>
      val fields = tupleLike.getFields
      val city = fields(0).asInstanceOf[String]
      val total = fields(1).asInstanceOf[Number].intValue()
      city -> total
    }.toMap

    assert(resultMap.size == 2)
    assert(resultMap("NY") == 15)
    assert(resultMap("SF") == 20)
  }

  test("AggregateOpExec performs global SUM and COUNT when there are no groupBy keys") {
    // schema: region (ignored for aggregation), revenue
    val schema = makeSchema(
      "region" -> AttributeType.STRING,
      "revenue" -> AttributeType.INTEGER
    )

    val t1 = makeTuple(schema, "west", 100)
    val t2 = makeTuple(schema, "east", 200)
    val t3 = makeTuple(schema, "west", 50)

    val desc = new AggregateOpDesc()
    val sumAgg = makeAggregationOp(AggregationFunction.SUM, "revenue", "total_revenue")
    val countAgg = makeAggregationOp(AggregationFunction.COUNT, "revenue", "row_count")
    desc.aggregations = List(sumAgg, countAgg)
    desc.groupByKeys = List() // global aggregation

    val descJson = objectMapper.writeValueAsString(desc)

    val exec = new AggregateOpExec(descJson)
    exec.open()
    exec.processTuple(t1, 0)
    exec.processTuple(t2, 0)
    exec.processTuple(t3, 0)

    val results = exec.onFinish(0).toList
    assert(results.size == 1)

    val fields = results.head.getFields
    // No group keys, so fields(0) is SUM(revenue), fields(1) is COUNT(revenue)
    val totalRevenue = fields(0).asInstanceOf[Number].intValue()
    val rowCount = fields(1).asInstanceOf[Number].intValue()

    assert(totalRevenue == 350)
    assert(rowCount == 3)
  }
}
