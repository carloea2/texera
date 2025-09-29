
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

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import edu.uci.ics.amber.operator.stablemergesort.StableMergeSortOpDesc.StableSortKey
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.sql.Timestamp
import java.util.Locale
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class StableMergeSortOpExec(descString: String) extends OperatorExecutor {

  private val desc: StableMergeSortOpDesc =
    objectMapper.readValue(descString, classOf[StableMergeSortOpDesc])

  private val configuredKeys: List[StableSortKey] = desc.keys.asScala.toList

  private case class ResolvedKey(
                                  index: Int,
                                  attributeType: AttributeType,
                                  descending: Boolean,
                                  nullsFirst: Boolean
                                )

  private var inputSchema: Schema = _
  private var resolved: Array[ResolvedKey] = _

  // Incremental run stack: runs(level) holds a sorted run of size 2^level, or null if empty
  private var runs: ArrayBuffer[ArrayBuffer[Tuple]] = _

  override def open(): Unit = {
    runs = ArrayBuffer.empty[ArrayBuffer[Tuple]]
  }

  override def close(): Unit = {
    if (runs != null) runs.clear()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (inputSchema == null) {
      inputSchema = tuple.getSchema
      resolved = resolveKeys(inputSchema)
    }
    // each incoming tuple is a size-1 sorted run
    val run = ArrayBuffer[Tuple](tuple)
    pushRun(run)
    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    if (runs.isEmpty) return Iterator.empty
    // fold-merge from left (earliest chunks) to right (latest), preserving chronology
    var acc = runs(0)
    var i = 1
    while (i < runs.length) {
      acc = mergeRuns(acc, runs(i))
      i += 1
    }
    acc.iterator
  }

  private def resolveKeys(schema: Schema): Array[ResolvedKey] = {
    configuredKeys.map { k =>
      val idx = schema.getIndex(k.attribute)
      val at  = schema.getAttribute(k.attribute).getType
      val ord = Option(k.order).map(_.toLowerCase(Locale.ROOT)).getOrElse("asc")
      val nul = Option(k.nulls).map(_.toLowerCase(Locale.ROOT)).getOrElse("last")
      ResolvedKey(
        index = idx,
        attributeType = at,
        descending = (ord == "desc"),
        nullsFirst = (nul == "first")
      )
    }.toArray
  }


  private def pushRun(initial: ArrayBuffer[Tuple]): Unit = {
    // treat 'runs' as a chronological stack: append new run to end
    runs.append(initial)
    // while the top two runs have equal size, merge them into one (older run first)
    while (runs.length >= 2 && runs(runs.length - 1).size == runs(runs.length - 2).size) {
      val right = runs.remove(runs.length - 1) // newest
      val left  = runs.remove(runs.length - 1) // older
      val merged = mergeRuns(left, right)      // stable: left items come before right when equal
      runs.append(merged)
    }
  }

  private def mergeRuns(left: ArrayBuffer[Tuple], right: ArrayBuffer[Tuple]): ArrayBuffer[Tuple] = {
    val out = new ArrayBuffer[Tuple](left.size + right.size)
    var i = 0
    var j = 0
    while (i < left.size && j < right.size) {
      if (compareTuples(left(i), right(j)) <= 0) {
        out += left(i); i += 1
      } else {
        out += right(j); j += 1
      }
    }
    while (i < left.size) { out += left(i); i += 1 }
    while (j < right.size) { out += right(j); j += 1 }
    out
  }

  private def compareTuples(a: Tuple, b: Tuple): Int = {
    var k = 0
    while (k < resolved.length) {
      val r = resolved(k)
      val c = compareField(a.getField[Any](r.index), b.getField[Any](r.index), r)
      if (c != 0) return if (r.descending) -c else c
      k += 1
    }
    0
  }

  private def compareField(va: Any, vb: Any, key: ResolvedKey): Int = {
    if (va == null && vb == null) return 0
    if (va == null) return if (key.nullsFirst) -1 else 1
    if (vb == null) return if (key.nullsFirst) 1 else -1

    key.attributeType match {
      case AttributeType.INTEGER   => Integer.compare(va.asInstanceOf[Int], vb.asInstanceOf[Int])
      case AttributeType.LONG      => java.lang.Long.compare(va.asInstanceOf[Long], vb.asInstanceOf[Long])
      case AttributeType.DOUBLE    => java.lang.Double.compare(va.asInstanceOf[Double], vb.asInstanceOf[Double])
      case AttributeType.BOOLEAN   => java.lang.Boolean.compare(va.asInstanceOf[Boolean], vb.asInstanceOf[Boolean])
      case AttributeType.TIMESTAMP => va.asInstanceOf[Timestamp].compareTo(vb.asInstanceOf[Timestamp])
      case AttributeType.STRING    => va.asInstanceOf[String].compareTo(vb.asInstanceOf[String])
      case other                   =>
        // Should be validated during compilation; unreachable at runtime.
        throw new IllegalStateException(s"Unsupported attribute type $other in StableMergeSort.")
    }
  }
}