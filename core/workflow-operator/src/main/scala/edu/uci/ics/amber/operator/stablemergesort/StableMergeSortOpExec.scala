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

  private val configuredKeys: List[StableSortKey] =
    Option(desc.keys).map(_.asScala.toList).getOrElse(Nil)

  if (configuredKeys.isEmpty) {
    throw new IllegalArgumentException("StableMergeSort requires at least one sort key.")
  }

  if (desc.offset < 0) {
    throw new IllegalArgumentException("Offset must be non-negative.")
  }

  private val limitOpt: Option[Int] = Option(desc.limit).map(_.intValue())

  limitOpt.foreach { limitValue =>
    if (limitValue < 0) {
      throw new IllegalArgumentException("Limit must be non-negative when specified.")
    }
  }

  private case class ResolvedKey(
      attribute: String,
      index: Int,
      attributeType: AttributeType,
      descending: Boolean,
      nullsFirst: Boolean,
      caseInsensitive: Boolean
  )

  private var bufferedTuples: ArrayBuffer[Tuple] = _
  private var inputSchema: Schema = _
  private var resolvedKeys: Array[ResolvedKey] = _

  override def open(): Unit = {
    bufferedTuples = ArrayBuffer.empty[Tuple]
  }

  override def close(): Unit = {
    if (bufferedTuples != null) {
      bufferedTuples.clear()
    }
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (inputSchema == null) {
      inputSchema = tuple.getSchema
      resolvedKeys = resolveKeys(inputSchema)
    }
    bufferedTuples.append(tuple)
    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    if (bufferedTuples.isEmpty) {
      Iterator.empty
    } else {
      if (resolvedKeys == null) {
        resolvedKeys = resolveKeys(bufferedTuples.head.getSchema)
      }
      stableMergeSort(bufferedTuples)
      val startIndex = math.min(desc.offset, bufferedTuples.size)
      val endExclusive = limitOpt match {
        case Some(limitValue) => math.min(startIndex + limitValue, bufferedTuples.size)
        case None             => bufferedTuples.size
      }
      if (startIndex >= endExclusive) {
        Iterator.empty
      } else {
        bufferedTuples.slice(startIndex, endExclusive).iterator
      }
    }
  }

  private def resolveKeys(schema: Schema): Array[ResolvedKey] = {
    configuredKeys.map { key =>
      val attributeName = Option(key.attribute)
        .getOrElse(throw new IllegalArgumentException("Sort key attribute must be provided."))

      if (!schema.containsAttribute(attributeName)) {
        throw new IllegalArgumentException(
          s"Attribute '$attributeName' specified in the sort key does not exist in the input schema."
        )
      }

      val attributeType = schema.getAttribute(attributeName).getType
      if (!isSupportedType(attributeType)) {
        throw new IllegalArgumentException(
          s"Attribute '$attributeName' has unsupported type $attributeType for StableMergeSort."
        )
      }

      val orderToken = Option(key.order).map(_.toLowerCase(Locale.ROOT)).getOrElse("asc")
      val descending = orderToken match {
        case "asc"  => false
        case "desc" => true
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported sort order '$other' for attribute '$attributeName'."
          )
      }

      val nullsToken = Option(key.nulls).map(_.toLowerCase(Locale.ROOT)).getOrElse("last")
      val nullsFirst = nullsToken match {
        case "first" => true
        case "last"  => false
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported nulls placement '$other' for attribute '$attributeName'."
          )
      }

      if (key.caseInsensitive && attributeType != AttributeType.STRING) {
        throw new IllegalArgumentException(
          s"Case insensitive comparison is only supported for STRING attributes. Attribute '$attributeName' is of type $attributeType."
        )
      }

      ResolvedKey(
        attributeName,
        schema.getIndex(attributeName),
        attributeType,
        descending,
        nullsFirst,
        key.caseInsensitive
      )
    }.toArray
  }

  private def isSupportedType(attributeType: AttributeType): Boolean =
    attributeType match {
      case AttributeType.STRING | AttributeType.INTEGER | AttributeType.LONG |
          AttributeType.DOUBLE | AttributeType.BOOLEAN | AttributeType.TIMESTAMP => true
      case _ => false
    }

  private def stableMergeSort(buffer: ArrayBuffer[Tuple]): Unit = {
    if (buffer.lengthCompare(1) <= 0) {
      return
    }
    val tempArray = new Array[Tuple](buffer.length)
    mergeSortRecursive(buffer, tempArray, 0, buffer.length)
  }

  private def mergeSortRecursive(
      buffer: ArrayBuffer[Tuple],
      temp: Array[Tuple],
      left: Int,
      right: Int
  ): Unit = {
    if (right - left <= 1) {
      return
    }
    val mid = (left + right) >>> 1
    mergeSortRecursive(buffer, temp, left, mid)
    mergeSortRecursive(buffer, temp, mid, right)
    merge(buffer, temp, left, mid, right)
  }

  private def merge(
      buffer: ArrayBuffer[Tuple],
      temp: Array[Tuple],
      left: Int,
      mid: Int,
      right: Int
  ): Unit = {
    var i = left
    var j = mid
    var k = left
    while (i < mid && j < right) {
      if (compareTuples(buffer(i), buffer(j)) <= 0) {
        temp(k) = buffer(i)
        i += 1
      } else {
        temp(k) = buffer(j)
        j += 1
      }
      k += 1
    }
    while (i < mid) {
      temp(k) = buffer(i)
      i += 1
      k += 1
    }
    while (j < right) {
      temp(k) = buffer(j)
      j += 1
      k += 1
    }
    var idx = left
    while (idx < right) {
      buffer(idx) = temp(idx)
      idx += 1
    }
  }

  private def compareTuples(left: Tuple, right: Tuple): Int = {
    var idx = 0
    while (idx < resolvedKeys.length) {
      val key = resolvedKeys(idx)
      val comparison = compareField(left, right, key)
      if (comparison != 0) {
        return if (key.descending) -comparison else comparison
      }
      idx += 1
    }
    0
  }

  private def compareField(left: Tuple, right: Tuple, key: ResolvedKey): Int = {
    val leftValue = left.getField[Any](key.index)
    val rightValue = right.getField[Any](key.index)

    if (leftValue == null && rightValue == null) {
      0
    } else if (leftValue == null) {
      if (key.nullsFirst) -1 else 1
    } else if (rightValue == null) {
      if (key.nullsFirst) 1 else -1
    } else {
      key.attributeType match {
        case AttributeType.INTEGER =>
          Integer.compare(leftValue.asInstanceOf[Int], rightValue.asInstanceOf[Int])
        case AttributeType.LONG =>
          java.lang.Long.compare(leftValue.asInstanceOf[Long], rightValue.asInstanceOf[Long])
        case AttributeType.DOUBLE =>
          java.lang.Double.compare(leftValue.asInstanceOf[Double], rightValue.asInstanceOf[Double])
        case AttributeType.BOOLEAN =>
          java.lang.Boolean.compare(leftValue.asInstanceOf[Boolean], rightValue.asInstanceOf[Boolean])
        case AttributeType.TIMESTAMP =>
          leftValue.asInstanceOf[Timestamp].compareTo(rightValue.asInstanceOf[Timestamp])
        case AttributeType.STRING =>
          val leftString = leftValue.asInstanceOf[String]
          val rightString = rightValue.asInstanceOf[String]
          if (key.caseInsensitive) {
            val lowerComparison =
              leftString.toLowerCase(Locale.ROOT).compareTo(rightString.toLowerCase(Locale.ROOT))
            if (lowerComparison != 0) lowerComparison else 0
          } else {
            leftString.compareTo(rightString)
          }
        case other =>
          throw new IllegalArgumentException(s"Unsupported attribute type $other encountered during comparison.")
      }
    }
  }
}
