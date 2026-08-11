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

import com.fasterxml.jackson.annotation.JsonProperty

/**
  * Executor config for the Finally Merger physical operator — not a user-facing
  * descriptor. The compiler pass fills `ownConeOpIds` with the paired frame's
  * cone (canonical physical-op id strings) so the Merger can absorb *caught*
  * try-side error States (they must not leak past the frame) while forwarding
  * catch-side and foreign ones (escalation).
  */
class FinallyMergerConfig {
  @JsonProperty
  var ownConeOpIds: List[String] = List()

  // Signal-port ids (internal, starting at 2 so they cannot collide with the
  // external From Try = 0 / From Catch = 1, since executors see only the int).
  // One per cone ending NOT wired into this Merger: the gate aggregates every
  // ending of the try cone for ITS decision, and the Merger must see the same
  // evidence, or a failure on an unwired ending leaves the two disagreeing —
  // the gate releasing the catch replay while the Merger still flushes the
  // try side (or flushing a "recovery" whose unwired fork died).
  @JsonProperty
  var trySignalPortIds: List[Int] = List()
  @JsonProperty
  var catchSignalPortIds: List[Int] = List()
}
