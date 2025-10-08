package edu.uci.ics.amber.operator.stablemergesort

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import edu.uci.ics.amber.operator.sort.{SortCriteriaUnit, SortPreference}

import scala.collection.convert.ImplicitConversions.`collection asJava`

class StableMergeSortOpExecSpec extends AnyFlatSpec {

  // ---------------------------
  // Helpers
  // ---------------------------

  private def schemaOf(attributes: (String, AttributeType)*): Schema = {
    attributes.foldLeft(Schema()) { case (acc, (name, tpe)) => acc.add(new Attribute(name, tpe)) }
  }

  private def tupleOf(schema: Schema, values: (String, Any)*): Tuple = {
    val valueMap = values.toMap
    val builder = Tuple.builder(schema)
    schema.getAttributeNames.forEach { name =>
      builder.add(schema.getAttribute(name), valueMap(name))
    }
    builder.build()
  }

  private def key(attribute: String, pref: SortPreference = SortPreference.ASC): SortCriteriaUnit = {
    val k = new SortCriteriaUnit()
    k.attributeName = attribute
    k.sortPreference = pref
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
    tuples.foreach(t => exec.processTuple(t, 0))
    val result = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
    exec.close()
    result
  }

  // ---- Reflection helpers (test-only) to verify incrementality ----
  private def getRunSizes(exec: StableMergeSortOpExec): List[Int] = {
    val f = classOf[StableMergeSortOpExec].getDeclaredField("runs")
    f.setAccessible(true)
    val runsBuf = f.get(exec).asInstanceOf[ArrayBuffer[ArrayBuffer[Tuple]]]
    runsBuf.filter(_ != null).map(_.size).toList
  }

  private def binaryDecomposition(n: Int): List[Int] = {
    var k = n
    val out = scala.collection.mutable.ListBuffer[Int]()
    while (k > 0) {
      val p = Integer.lowestOneBit(k)
      out += p
      k -= p
    }
    out.toList.sorted
  }

  // ---------------------------
  // Tests
  // ---------------------------

  // 1
  "StableMergeSortOpExec" should "sort integers ascending and preserve duplicate order" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> 3, "label" -> "a"),
      tupleOf(schema, "value" -> 1, "label" -> "first-1"),
      tupleOf(schema, "value" -> 2, "label" -> "b"),
      tupleOf(schema, "value" -> 1, "label" -> "first-2"),
      tupleOf(schema, "value" -> 3, "label" -> "c")
    )
    val result = execute(schema, tuples) { _.keys = List(key("value")).asJava }
    assert(result.map(_.getField[Int]("value")) == List(1, 1, 2, 3, 3))
    val labelsForOnes = result.filter(_.getField[Int]("value") == 1).map(_.getField[String]("label"))
    assert(labelsForOnes == List("first-1", "first-2"))
  }

  // 2
  it should "sort integers descending while preserving stability" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> 2, "label" -> "first"),
      tupleOf(schema, "value" -> 2, "label" -> "second"),
      tupleOf(schema, "value" -> 1, "label" -> "third"),
      tupleOf(schema, "value" -> 3, "label" -> "fourth")
    )
    val result = execute(schema, tuples) { _.keys = List(key("value", SortPreference.DESC)).asJava }
    assert(result.map(_.getField[Int]("value")) == List(3, 2, 2, 1))
    val labelsForTwos = result.filter(_.getField[Int]("value") == 2).map(_.getField[String]("label"))
    assert(labelsForTwos == List("first", "second"))
  }

  // 3
  it should "handle string ordering (case-sensitive)" in {
    val schema = schemaOf("name" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "name" -> "apple"),
      tupleOf(schema, "name" -> "Banana"),
      tupleOf(schema, "name" -> "banana"),
      tupleOf(schema, "name" -> "APPLE")
    )
    val sorted = execute(schema, tuples) { _.keys = List(key("name", SortPreference.ASC)).asJava }
    assert(sorted.map(_.getField[String]("name")) == List("APPLE", "Banana", "apple", "banana"))
  }

  // 4
  it should "place nulls last regardless of ascending or descending" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> null, "label" -> "null-1"),
      tupleOf(schema, "value" -> 5, "label" -> "five"),
      tupleOf(schema, "value" -> null, "label" -> "null-2"),
      tupleOf(schema, "value" -> 3, "label" -> "three")
    )
    val asc = execute(schema, tuples) { _.keys = List(key("value", SortPreference.ASC)).asJava }
    assert(asc.map(_.getField[String]("label")) == List("three", "five", "null-1", "null-2"))

    val desc = execute(schema, tuples) { _.keys = List(key("value", SortPreference.DESC)).asJava }
    assert(desc.map(_.getField[String]("label")) == List("five", "three", "null-1", "null-2"))
  }

  // 5
  it should "support multi-key sorting with mixed attribute types" in {
    val schema = schemaOf(
      "dept" -> AttributeType.STRING,
      "score" -> AttributeType.DOUBLE,
      "name" -> AttributeType.STRING,
      "hired" -> AttributeType.TIMESTAMP
    )
    val base = Timestamp.valueOf("2020-01-01 00:00:00")
    val tuples = List(
      tupleOf(schema, "dept" -> "Sales", "score" -> 9.5, "name" -> "Alice", "hired" -> base),
      tupleOf(schema, "dept" -> "Sales", "score" -> 9.5, "name" -> "Bob",   "hired" -> new Timestamp(base.getTime + 1000)),
      tupleOf(schema, "dept" -> "Sales", "score" -> 8.0, "name" -> "Carol", "hired" -> new Timestamp(base.getTime + 2000)),
      tupleOf(schema, "dept" -> "Engineering", "score" -> 9.5, "name" -> "Dave",  "hired" -> new Timestamp(base.getTime + 3000)),
      tupleOf(schema, "dept" -> null, "score" -> 9.5, "name" -> "Eve",   "hired" -> new Timestamp(base.getTime + 4000))
    )
    val result = execute(schema, tuples) { desc =>
      desc.keys = List(
        key("dept", SortPreference.ASC),
        key("score", SortPreference.DESC),
        key("name", SortPreference.ASC)
      ).asJava
    }
    assert(result.map(_.getField[String]("name")) == List("Dave", "Alice", "Bob", "Carol", "Eve"))
  }

  // 6
  it should "sort large inputs efficiently" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = (50000 to 1 by -1).map(i => tupleOf(schema, "value" -> i, "label" -> s"row-$i"))
    val result = execute(schema, tuples) { _.keys = List(key("value")).asJava }
    assert(result.head.getField[Int]("value") == 1)
    assert(result(1).getField[Int]("value") == 2)
    assert(result.takeRight(2).map(_.getField[Int]("value")) == List(49999, 50000))
  }

  // 7
  it should "preserve original order among tuples with equal keys" in {
    val schema = schemaOf("key" -> AttributeType.INTEGER, "index" -> AttributeType.INTEGER)
    val tuples = (0 until 100).map(i => tupleOf(schema, "key" -> (i % 5), "index" -> i))
    val result = execute(schema, tuples) { _.keys = List(key("key")).asJava }
    val grouped = result.groupBy(_.getField[Int]("key")).values
    grouped.foreach { group =>
      val indices = group.map(_.getField[Int]("index"))
      assert(indices == indices.sorted)
    }
  }

  // 8
  it should "buffer tuples until onFinish is called" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val tuple = tupleOf(schema, "value" -> 2)
    val desc = new StableMergeSortOpDesc(); desc.keys = List(key("value")).asJava
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    val immediate = exec.processTuple(tuple, 0)
    assert(immediate.isEmpty)
    val result = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
    assert(result.map(_.getField[Int]("value")) == List(2))
    exec.close()
  }

  // 9
  it should "return empty for empty input" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val result = execute(schema, Seq.empty) { _.keys = List(key("value")).asJava }
    assert(result.isEmpty)
  }

  // 10
  it should "handle single element input" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val result = execute(schema, Seq(tupleOf(schema, "value" -> 42))) { _.keys = List(key("value")).asJava }
    assert(result.map(_.getField[Int]("value")) == List(42))
  }

  // 11
  it should "sort negatives and zeros correctly" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val tuples = List(0, -1, -10, 5, -3, 2).map(v => tupleOf(schema, "value" -> v))
    val result = execute(schema, tuples) { _.keys = List(key("value")).asJava }
    assert(result.map(_.getField[Int]("value")) == List(-10, -3, -1, 0, 2, 5))
  }

  // 12
  it should "sort LONG values ascending" in {
    val schema = schemaOf("id" -> AttributeType.LONG)
    val tuples = List(5L, 1L, 3L, 9L, 0L).map(v => tupleOf(schema, "id" -> v))
    val result = execute(schema, tuples) { _.keys = List(key("id")).asJava }
    assert(result.map(_.getField[Long]("id")) == List(0L, 1L, 3L, 5L, 9L))
  }

  // 13
  it should "sort DOUBLE values including -0.0, 0.0, infinities and NaN" in {
    val schema = schemaOf("x" -> AttributeType.DOUBLE)
    val tuples = List(Double.NaN, Double.PositiveInfinity, 1.5, -0.0, 0.0, -3.2, Double.NegativeInfinity)
      .map(v => tupleOf(schema, "x" -> v))
    val result = execute(schema, tuples) { _.keys = List(key("x")).asJava }
    val xs = result.map(_.getField[Double]("x"))
    assert(xs.head == Double.NegativeInfinity)
    assert(xs(1) == -3.2)
    assert(java.lang.Double.compare(xs(2), -0.0) == 0)
    assert(java.lang.Double.compare(xs(3), 0.0) == 0)
    assert(xs(4) == 1.5)
    assert(xs(5) == Double.PositiveInfinity)
    assert(java.lang.Double.isNaN(xs(6)))
  }

  // 14
  it should "sort BOOLEAN ascending (false < true) and descending" in {
    val schema = schemaOf("b" -> AttributeType.BOOLEAN)
    val tuples = List(true, false, true, false).map(v => tupleOf(schema, "b" -> v))
    val asc = execute(schema, tuples) { _.keys = List(key("b", SortPreference.ASC)).asJava }
    assert(asc.map(_.getField[Boolean]("b")) == List(false, false, true, true))
    val desc = execute(schema, tuples) { _.keys = List(key("b", SortPreference.DESC)).asJava }
    assert(desc.map(_.getField[Boolean]("b")) == List(true, true, false, false))
  }

  // 15
  it should "sort TIMESTAMP descending" in {
    val schema = schemaOf("t" -> AttributeType.TIMESTAMP)
    val base = Timestamp.valueOf("2023-01-01 00:00:00")
    val tuples = List(
      new Timestamp(base.getTime + 3000),
      base,
      new Timestamp(base.getTime + 1000),
      new Timestamp(base.getTime + 2000)
    ).map(ts => tupleOf(schema, "t" -> ts))
    val result = execute(schema, tuples) { _.keys = List(key("t", SortPreference.DESC)).asJava }
    val times = result.map(_.getField[Timestamp]("t").getTime)
    assert(times == times.sorted(Ordering.Long.reverse))
  }

  // 16
  it should "handle multi-key with descending primary and ascending secondary" in {
    val schema = schemaOf("major" -> AttributeType.INTEGER, "minor" -> AttributeType.INTEGER, "idx" -> AttributeType.INTEGER)
    val tuples = List(
      (1, 9, 0), (1, 1, 1), (2, 5, 2), (2, 3, 3), (1, 1, 4), (3, 0, 5), (3, 2, 6)
    ).map { case (ma, mi, i) => tupleOf(schema, "major" -> ma, "minor" -> mi, "idx" -> i) }
    val result = execute(schema, tuples) { desc =>
      desc.keys = List(key("major", SortPreference.DESC), key("minor", SortPreference.ASC)).asJava
    }
    val pairs = result.map(t => (t.getField[Int]("major"), t.getField[Int]("minor")))
    assert(pairs == List((3,0),(3,2),(2,3),(2,5),(1,1),(1,1),(1,9)))
    val idxFor11 = result.filter(t => t.getField[Int]("major")==1 && t.getField[Int]("minor")==1).map(_.getField[Int]("idx"))
    assert(idxFor11 == List(1,4))
  }

  // 17
  it should "place nulls last across multiple keys (primary then secondary desc)" in {
    val schema = schemaOf("a" -> AttributeType.STRING, "b" -> AttributeType.INTEGER)
    val tuples = List(
      ("x", 2), (null, 1), ("x", 1), (null, 5), ("a", 9), ("a", 2)
    ).map { case (s, i) => tupleOf(schema, "a" -> s, "b" -> i) }
    val result = execute(schema, tuples) { desc =>
      desc.keys = List(key("a", SortPreference.ASC), key("b", SortPreference.DESC)).asJava
    }
    val out = result.map(t => (t.getField[String]("a"), t.getField[Int]("b")))
    assert(out == List(("a",9),("a",2),("x",2),("x",1),(null,5),(null,1)))
  }

  // 18
  it should "be idempotent on already-sorted input and preserve full stability" in {
    val schema = schemaOf("v" -> AttributeType.INTEGER, "i" -> AttributeType.INTEGER)
    val tuples = (0 until 10).flatMap(v => List((v, v*2), (v, v*2+1)))
      .map { case (v,i) => tupleOf(schema, "v" -> v, "i" -> i) }
    val result = execute(schema, tuples) { _.keys = List(key("v")).asJava }
    assert(result.map(_.getField[Int]("v")) == (0 until 10).flatMap(v => List(v,v)).toList)
    val grouped = result.groupBy(_.getField[Int]("v")).values
    grouped.foreach { g =>
      val is = g.map(_.getField[Int]("i"))
      assert(is == is.sorted)
    }
  }

  // 19
  it should "handle incremental carry merges across many levels (power-of-two length)" in {
    val n = 1024
    val schema = schemaOf("v" -> AttributeType.INTEGER)
    val tuples = (n-1 to 0 by -1).map(i => tupleOf(schema, "v" -> i))
    val result = execute(schema, tuples) { _.keys = List(key("v")).asJava }
    assert(result.map(_.getField[Int]("v")) == (0 until n).toList)
  }

  // 20
  it should "correctly merge runs built at different times (rotated halves regression)" in {
    val schema = schemaOf("v" -> AttributeType.INTEGER)
    val tuples = (65 to 95).map(i => tupleOf(schema, "v" -> i)) ++ (0 to 60).map(i => tupleOf(schema, "v" -> i))
    val result = execute(schema, tuples) { _.keys = List(key("v")).asJava }
    assert(result.map(_.getField[Int]("v")) == ((0 to 60) ++ (65 to 95)).toList)
  }

  // 21
  it should "treat numeric strings as strings (lexicographic ordering)" in {
    val schema = schemaOf("s" -> AttributeType.STRING)
    val tuples = List("2","10","1","11","20").map(s => tupleOf(schema, "s" -> s))
    val result = execute(schema, tuples) { _.keys = List(key("s")).asJava }
    assert(result.map(_.getField[String]("s")) == List("1","10","11","2","20"))
  }

  // 22
  it should "be stable when all keys are equal across multiple attributes" in {
    val schema = schemaOf("a" -> AttributeType.STRING, "b" -> AttributeType.INTEGER, "idx" -> AttributeType.INTEGER)
    val tuples = List(0,1,2,3,4).map(i => tupleOf(schema, "a" -> "X", "b" -> 7, "idx" -> i))
    val result = execute(schema, tuples) { _.keys = List(key("a"), key("b")).asJava }
    assert(result.map(_.getField[Int]("idx")) == List(0,1,2,3,4))
  }

  // 23
  it should "merge incrementally: run sizes match binary decomposition after each push" in {
    val schema = schemaOf("v" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = List(key("v")).asJava
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val N = 64
    for (i <- (N-1) to 0 by -1) {
      exec.processTuple(tupleOf(schema, "v" -> i), 0)
      val sizes = getRunSizes(exec).sorted
      assert(sizes == binaryDecomposition(N - i))
    }

    exec.close()
  }

  // 24
  it should "maintain run-stack invariant (no adjacent equal sizes) after each insertion" in {
    val schema = schemaOf("v" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = List(key("v")).asJava
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val N = 200
    val stream = (0 until N by 2) ++ (1 until N by 2)
    stream.foreach { i =>
      exec.processTuple(tupleOf(schema, "v" -> (N - 1 - i)), 0)
      val sizes = getRunSizes(exec)
      sizes.sliding(2).foreach { pair =>
        if (pair.length == 2) assert(pair.head != pair.last)
      }
    }

    exec.close()
  }

  // 25
  it should "form expected run sizes at milestones (1,2,3,4,7,8,15,16)" in {
    val schema = schemaOf("v" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = List(key("v")).asJava
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val seq = (100 to 1 by -1).map(i => tupleOf(schema, "v" -> i))
    val milestones = Set(1,2,3,4,7,8,15,16)
    var pushed = 0
    seq.foreach { t =>
      exec.processTuple(t, 0); pushed += 1
      if (milestones.contains(pushed)) {
        val sizes = getRunSizes(exec).sorted
        assert(sizes == binaryDecomposition(pushed))
      }
    }

    exec.close()
  }
  // 26
  it should "order NaN highest on secondary DESC but still place nulls last" in {
    val schema = schemaOf(
      "g" -> AttributeType.STRING,
      "score" -> AttributeType.DOUBLE,
      "label" -> AttributeType.STRING
    )
    val tuples = List(
      tupleOf(schema, "g" -> "A", "score" -> java.lang.Double.NaN,           "label" -> "nan"),
      tupleOf(schema, "g" -> "A", "score" -> Double.PositiveInfinity,         "label" -> "pinf"),
      tupleOf(schema, "g" -> "A", "score" -> 1.0,                             "label" -> "one"),
      tupleOf(schema, "g" -> "A", "score" -> 0.0,                             "label" -> "zero"),
      tupleOf(schema, "g" -> "A", "score" -> -1.0,                            "label" -> "neg"),
      tupleOf(schema, "g" -> "A", "score" -> Double.NegativeInfinity,         "label" -> "ninf"),
      tupleOf(schema, "g" -> "A", "score" -> null,                            "label" -> "null-1"),
      tupleOf(schema, "g" -> "A", "score" -> null,                            "label" -> "null-2")
    )
    val result = execute(schema, tuples) { desc =>
      desc.keys = List(
        key("g", SortPreference.ASC),
        key("score", SortPreference.DESC)
      ).asJava
    }
    assert(result.map(_.getField[String]("label")) ==
      List("nan","pinf","one","zero","neg","ninf","null-1","null-2"))
  }

  // 27
  it should "act as a stable pass-through when keys are empty" in {
    val schema = schemaOf("v" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(3, 1, 4, 1, 5, 9).zipWithIndex
      .map { case (v, i) => tupleOf(schema, "v" -> v, "label" -> s"row-$i") }
    val result = execute(schema, tuples) { desc =>
      desc.keys = List.empty[SortCriteriaUnit].asJava
    }
    assert(result.map(_.getField[String]("label")) ==
      List("row-0","row-1","row-2","row-3","row-4","row-5"))
  }

  // 28
  it should "place null primary values last under DESC and then order them by secondary ASC" in {
    val schema = schemaOf(
      "value" -> AttributeType.INTEGER,
      "idx"   -> AttributeType.INTEGER,
      "label" -> AttributeType.STRING
    )
    val tuples = List(
      tupleOf(schema, "value" -> 5,    "idx" -> 2, "label" -> "a"),
      tupleOf(schema, "value" -> 3,    "idx" -> 1, "label" -> "b"),
      tupleOf(schema, "value" -> null, "idx" -> 4, "label" -> "n1"),
      tupleOf(schema, "value" -> 4,    "idx" -> 3, "label" -> "c"),
      tupleOf(schema, "value" -> null, "idx" -> 2, "label" -> "n2")
    )
    val result = execute(schema, tuples) { desc =>
      desc.keys = List(
        key("value", SortPreference.DESC),
        key("idx",   SortPreference.ASC)
      ).asJava
    }
    assert(result.map(_.getField[String]("label")) ==
      List("a","c","b","n2","n1"))
  }

  // 29
  it should "place nulls last on secondary ASC when primary keys tie" in {
    val schema = schemaOf(
      "p"     -> AttributeType.STRING,
      "s"     -> AttributeType.INTEGER,
      "label" -> AttributeType.STRING
    )
    val tuples = List(
      tupleOf(schema, "p" -> "X", "s" -> 2,    "label" -> "two"),
      tupleOf(schema, "p" -> "X", "s" -> null, "label" -> "null-1"),
      tupleOf(schema, "p" -> "X", "s" -> 1,    "label" -> "one"),
      tupleOf(schema, "p" -> "X", "s" -> null, "label" -> "null-2")
    )
    val result = execute(schema, tuples) { desc =>
      desc.keys = List(
        key("p", SortPreference.ASC),
        key("s", SortPreference.ASC)
      ).asJava
    }
    assert(result.map(_.getField[String]("label")) ==
      List("one","two","null-1","null-2"))
  }

}
