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

package edu.uci.ics.amber.operator.sort

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.collection.mutable.ArrayBuffer

class StableMergeSortOpExec(descString: String) extends OperatorExecutor {

  private val desc: StableMergeSortOpDesc =
    objectMapper.readValue(descString, classOf[StableMergeSortOpDesc])

  private var inputSchema: Schema = _

  private case class ResolvedKey(
      index: Int,
      attributeType: AttributeType,
      descending: Boolean
  )

  private var resolved: Array[ResolvedKey] = _

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
    val run = ArrayBuffer[Tuple](tuple) // size-1 sorted run
    pushRun(run)
    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    if (runs.isEmpty) return Iterator.empty

    // Collapse all remaining runs left-to-right, preserving stability
    var acc = runs(0)
    var i = 1
    while (i < runs.length) {
      acc = mergeRuns(acc, runs(i))
      i += 1
    }

    runs.clear()
    runs.append(acc)

    acc.iterator
  }

  private def resolveKeys(schema: Schema): Array[ResolvedKey] = {
    desc.keys.map { k: SortCriteriaUnit =>
      val name = k.attributeName
      val idx = schema.getIndex(name)
      val tpe = schema.getAttribute(name).getType
      val descOrder = k.sortPreference == SortPreference.DESC
      ResolvedKey(idx, tpe, descOrder)
    }.toArray
  }

  private def pushRun(initial: ArrayBuffer[Tuple]): Unit = {
    runs.append(initial)
    // merge top two runs if they have equal sizes; preserve left-before-right for stability
    while (runs.length >= 2 && runs(runs.length - 1).size == runs(runs.length - 2).size) {
      val right = runs.remove(runs.length - 1) // newer
      val left = runs.remove(runs.length - 1) // older
      val merged = mergeRuns(left, right)
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
      val va = a.getField[Any](r.index)
      val vb = b.getField[Any](r.index)

      // Null policy: ALWAYS last, regardless of ASC/DESC
      if (va == null || vb == null) {
        if (va == null && vb == null) {
          k += 1
        } else {
          return if (va == null) 1 else -1
        }
      } else {
        val base = compareNonNull(va, vb, r.attributeType)
        if (base != 0) return if (r.descending) -base else base
        k += 1
      }
    }
    0
  }

  private def compareNonNull(va: Any, vb: Any, tpe: AttributeType): Int = {
    tpe match {
      case AttributeType.INTEGER =>
        java.lang.Integer.compare(
          va.asInstanceOf[Number].intValue(),
          vb.asInstanceOf[Number].intValue()
        )
      case AttributeType.LONG =>
        java.lang.Long.compare(
          va.asInstanceOf[Number].longValue(),
          vb.asInstanceOf[Number].longValue()
        )
      case AttributeType.DOUBLE =>
        java.lang.Double.compare(
          va.asInstanceOf[Number].doubleValue(),
          vb.asInstanceOf[Number].doubleValue()
        )
      case AttributeType.BOOLEAN =>
        java.lang.Boolean.compare(
          va.asInstanceOf[Boolean],
          vb.asInstanceOf[Boolean]
        )
      case AttributeType.TIMESTAMP =>
        va.asInstanceOf[java.sql.Timestamp]
          .compareTo(
            vb.asInstanceOf[java.sql.Timestamp]
          )
      case AttributeType.STRING =>
        va.asInstanceOf[String].compareTo(vb.asInstanceOf[String])
      case other =>
        throw new IllegalStateException(s"Unsupported attribute type $other in StableMergeSort")
    }
  }
}
