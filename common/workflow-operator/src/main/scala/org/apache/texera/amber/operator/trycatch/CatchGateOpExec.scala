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

package org.apache.texera.amber.operator.trycatch

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable

/**
  * An If generalized to N conditions (the frame's outcome gate).
  *
  * Ports: internal port 0 = the materialized input snapshot (its InputPort
  * declares dependencies on all signal ports, so two-phase execution resolves
  * the try side fully before the first snapshot tuple); internal ports 1..N =
  * signal ports, one per try-cone leaf, carrying only States and completion
  * (their links use SignalPartitioning — tuples dropped at the sender).
  *
  * Decision: an own-cone `__error__` State seen on a signal port => the attempt
  * failed => forward the snapshot into the catch subgraph. Clean completion, or
  * a foreign error (failure upstream of the frame — not ours to handle) =>
  * drop the snapshot; the catch subgraph runs empty and finalizes cleanly.
  *
  * State handling: signal-port States are absorbed (a caught error must not
  * leak; forwarding foreign errors is the snapshot lane's job, since a foreign
  * State always also traveled through the splitter). Snapshot-lane States
  * (loop envelopes, foreign errors) pass through into the catch subgraph.
  */
class CatchGateOpExec(descString: String) extends OperatorExecutor {

  private val config: CatchGateConfig =
    objectMapper.readValue(descString, classOf[CatchGateConfig])
  private val ownCone: Set[String] = config.ownConeOpIds.toSet
  private val snapshotPortId: Int = TryCatchOpDesc.SNAPSHOT_IN.id
  private val catchPort = PortIdentity(1)
  private val errorInfoPort = TryCatchOpDesc.ERROR_INFO_PORT

  private var failed = false
  // one report per failure event: drain guards make each worker emit at most
  // one error State per execution, so (operatorId, workerId) identifies the
  // event however many signal ports (fan-out) it arrived on
  private val reportedErrors = mutable.LinkedHashMap[(String, String), TupleLike]()
  // the error to RETHROW (catch unconnected): held until the snapshot lane,
  // because signal ports are dependees and during the dependee phase this
  // operator has no output ports yet — emitting there would go nowhere
  private var rethrow: Option[State] = None

  override def processState(state: State, port: Int): Option[State] = {
    if (port == snapshotPortId) {
      // snapshot lane: pass through (loop envelopes, foreign errors)
      Some(state)
    } else if (State.isError(state) && State.errorOperatorId(state).exists(ownCone.contains)) {
      if (config.catchConnected) {
        // signal lane, own-cone failure: trigger the catch and absorb — a
        // caught error must not leak past the frame
        failed = true
        recordError(state)
      } else if (rethrow.isEmpty) {
        // no catch subgraph: nothing is handled here, so RETHROW — hold the
        // error for the snapshot lane (phase 2, output ports assigned); it
        // then travels this gate's dangling ports' signal edges to the
        // enclosing gate and Merger (own-cone there, by inclusive
        // ownership). No Error Info row either: the report belongs to
        // whoever catches it. One error suffices — like PL, a single
        // exception escapes the block.
        rethrow = Some(state)
      }
      None
    } else {
      // ordinary or foreign States on a signal lane are absorbed; a foreign
      // error always also traveled the splitter, so the snapshot lane
      // forwards it
      None
    }
  }

  override def produceStateOnFinish(port: Int): Option[State] =
    if (port == snapshotPortId) rethrow else None

  private def recordError(state: State): Unit = {
    val envelope = state.values.get(State.ErrorKey) match {
      case Some(m: Map[_, _]) => m.asInstanceOf[Map[String, Any]]
      case _                  => Map.empty[String, Any]
    }
    def field(key: String): String = envelope.get(key).map(_.toString).getOrElse("")
    val key = (field("operatorId"), field("workerId"))
    if (!reportedErrors.contains(key)) {
      reportedErrors(key) =
        TupleLike(field("errorType"), field("message"), field("operatorId"), field("workerId"))
    }
  }

  // two output ports: every emission must be port-targeted
  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    if (port == snapshotPortId && failed) Iterator((tuple, Some(catchPort)))
    else Iterator.empty
  }

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    // emitted at snapshot-lane completion (phase 2, output ports assigned),
    // which also covers the empty-input case
    if (port == snapshotPortId) {
      reportedErrors.values.iterator.map(row => (row, Some(errorInfoPort)))
    } else {
      Iterator.empty
    }
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = ???
}
