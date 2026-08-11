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

package org.apache.texera.amber.engine.e2e

import com.twitter.util.Duration
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, OperatorIdentity}
import org.apache.texera.amber.core.workflow.{ExecutionMode, PortIdentity, WorkflowSettings}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadResults,
  setUpWorkflowExecutionData,
  workflowContext
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.trycatch.{FinallyOpDesc, TryCatchOpDesc}
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.common.compiler.model.LogicalLink
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource.getResultUriByLogicalPortId
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}

import scala.concurrent.duration.DurationInt

/**
  * End-to-end try/catch frame tests: run real
  * TextInput -> TryCatch -> {try, catch} -> Finally workflows through the
  * engine and assert the Finally's materialized results — the all-or-nothing
  * contract means exactly one branch's complete output comes out, and it comes
  * out of the port named for the winner (`Try Result` / `Catch Result`).
  *
  * The failing operator is a Filter whose predicate references a nonexistent
  * attribute: it compiles cleanly (Filter does no compile-time predicate
  * validation) and throws on the first tuple at runtime, exercising the whole
  * failure path — error State emission, per-port drain, signal edges into the
  * CatchGate, snapshot replay through the catch subgraph, and the Merger's
  * release decision.
  *
  * All operators are JVM-based, so no Python workers are involved.
  */
@IntegrationTest
class TryCatchIntegrationSpec
    extends TestKit(ActorSystem("TryCatchIntegrationSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Retries {

  override def withFixture(test: NoArgTest): Outcome =
    withRetry { super.withFixture(test) }

  implicit val timeout: Timeout = Timeout(5.seconds)

  // Unique per-suite id (1-6 are taken by the other e2e/integration suites).
  private val specId = 7

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData(specId)

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData(specId)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private def runAndGetMaterializedRowCounts(
      operators: List[LogicalOp],
      links: List[LogicalLink]
  ): Map[OperatorIdentity, Long] =
    runWorkflowAndReadResults(
      system,
      buildWorkflow(operators, links, workflowContext(specId)),
      operators.map(_.operatorIdentifier),
      _.getCount,
      Duration.fromSeconds(90)
    )

  /**
    * Row count of one specific materialized output port, read after the run
    * completed (the harness's map only covers port 0). Finally routes the
    * winner by port — `Try Result` (0) on success, `Catch Result` (1) on
    * failure — so the frame tests assert both ports' counts.
    */
  private def portRowCount(op: LogicalOp, port: PortIdentity): Long =
    getResultUriByLogicalPortId(ExecutionIdentity(specId.toLong), op.operatorIdentifier, port)
      .map(uri =>
        DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]].getCount
      )
      .getOrElse(fail(s"no materialized result for '${op.operatorIdentifier.id}' port ${port.id}"))

  private def textInput(text: String): TextInputSourceOpDesc = {
    val op = new TextInputSourceOpDesc()
    op.textInput = text
    op
  }

  private def limit(n: Int): LimitOpDesc = {
    val op = new LimitOpDesc()
    op.limit = n
    op
  }

  /** a filter that compiles cleanly but throws on the first tuple at runtime */
  private def failingFilter(): SpecializedFilterOpDesc = {
    val op = new SpecializedFilterOpDesc()
    op.predicates = List(new FilterPredicate("no_such_attribute", ComparisonType.EQUAL_TO, "x"))
    op
  }

  private def link(
      from: LogicalOp,
      fromPort: PortIdentity,
      to: LogicalOp,
      toPort: PortIdentity
  ): LogicalLink =
    LogicalLink(from.operatorIdentifier, fromPort, to.operatorIdentifier, toPort)

  private val port0 = PortIdentity()
  private val port1 = PortIdentity(1)

  /** src -> TryCatch; Try -> tryBranch -> Finally.FromTry; Catch -> catchBranch -> Finally.FromCatch */
  private def frameWorkflow(
      tryBranch: LogicalOp,
      catchBranch: LogicalOp
  ): (List[LogicalOp], List[LogicalLink], FinallyOpDesc) = {
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val fin = new FinallyOpDesc()
    val operators = List(src, tryCatch, tryBranch, catchBranch, fin)
    val links = List(
      link(src, port0, tryCatch, port0),
      link(tryCatch, port0, tryBranch, port0), // Try
      link(tryCatch, port1, catchBranch, port0), // Catch
      link(tryBranch, port0, fin, port0), // From Try
      link(catchBranch, port0, fin, port1) // From Catch
    )
    (operators, links, fin)
  }

  "Engine" should "emit the try branch's results through Finally when the attempt succeeds" in {
    val (operators, links, fin) = frameWorkflow(tryBranch = limit(3), catchBranch = limit(2))
    val materialized = runAndGetMaterializedRowCounts(operators, links)
    // try side saw all 3 rows and succeeded => Finally emits exactly those,
    // out of Try Result (port 0) — the harness map reads port 0; the catch
    // subgraph ran empty (gate dropped the snapshot) and Catch Result is empty
    assert(materialized(fin.operatorIdentifier) == 3)
    assert(portRowCount(fin, port1) == 0)
  }

  it should "replay the input through the catch branch when the try branch fails" in {
    val (operators, links, fin) = frameWorkflow(
      tryBranch = failingFilter(),
      catchBranch = limit(2)
    )
    val materialized = runAndGetMaterializedRowCounts(operators, links)
    // the failing filter poisons the try side; the gate releases the snapshot
    // (all 3 input rows) into the catch branch, whose limit(2) passes 2 rows;
    // the Merger flushes the catch side only — never a mix — and routes it out
    // of Catch Result (port 1), leaving Try Result (port 0) empty
    assert(materialized(fin.operatorIdentifier) == 0)
    assert(portRowCount(fin, port1) == 2)
  }

  it should "continue downstream from BOTH result ports (the loser's subgraph completes empty)" in {
    // The seal on the split: each result port is a real continuation point.
    // Downstream of the loser port must run on zero rows and complete (the
    // If-operator untaken-branch pattern), never hang; downstream of the
    // winner sees the full release. Failure path: winner = Catch Result.
    val (operators, links, fin) = frameWorkflow(
      tryBranch = failingFilter(),
      catchBranch = limit(2)
    )
    val onSuccess = limit(10) // downstream of Try Result: must complete empty
    val onRecovery = limit(10) // downstream of Catch Result: gets the release
    val materialized = runAndGetMaterializedRowCounts(
      operators ++ List(onSuccess, onRecovery),
      links ++ List(
        link(fin, port0, onSuccess, port0),
        link(fin, port1, onRecovery, port0)
      )
    )
    assert(materialized(onSuccess.operatorIdentifier) == 0)
    assert(materialized(onRecovery.operatorIdentifier) == 2)
  }

  it should "complete a frame whose Catch port is unconnected when the attempt succeeds" in {
    // A frame without a catch subgraph (and without a Finally) is legal: it
    // guards a pipeline that ends in its own result table. On success the
    // guarded tail materializes normally.
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val tail = limit(3)
    val materialized = runAndGetMaterializedRowCounts(
      List(src, tryCatch, tail),
      List(link(src, port0, tryCatch, port0), link(tryCatch, port0, tail, port0))
    )
    assert(materialized(tail.operatorIdentifier) == 3)
  }

  it should "drain and terminate (not hang) when a guarded operator fails with no catch wired" in {
    // A frame need not have a catch subgraph to be useful: an error inside it
    // becomes an in-band error State and the execution terminates — the
    // guarded tail produces nothing and the run completes (an unguarded
    // failure would pause the worker instead).
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val boom = failingFilter()
    val materialized = runAndGetMaterializedRowCounts(
      List(src, tryCatch, boom),
      List(link(src, port0, tryCatch, port0), link(tryCatch, port0, boom, port0))
    )
    assert(materialized(boom.operatorIdentifier) == 0)
  }

  it should "handle an inner frame's failure without disturbing the outer frame" in {
    // outer.Try -> inner TryCatch -> {inner try fails, inner catch recovers}
    //           -> inner Finally -> outer Finally.FromTry
    // The inner frame catches its own failure, so the outer frame must see a
    // clean try side and emit the inner Finally's (catch-side) results.
    val src = textInput("1\n2\n3")
    val outer = new TryCatchOpDesc()
    val inner = new TryCatchOpDesc()
    val innerTry = failingFilter()
    val innerCatch = limit(2)
    val innerFin = new FinallyOpDesc()
    val outerCatch = limit(1)
    val outerFin = new FinallyOpDesc()

    val operators =
      List(src, outer, inner, innerTry, innerCatch, innerFin, outerCatch, outerFin)
    val links = List(
      link(src, port0, outer, port0),
      link(outer, port0, inner, port0), // outer Try -> inner frame
      link(inner, port0, innerTry, port0), // inner Try
      link(inner, port1, innerCatch, port0), // inner Catch
      link(innerTry, port0, innerFin, port0),
      link(innerCatch, port0, innerFin, port1),
      // outer From Try: union BOTH inner result ports — "whatever the inner
      // frame produced continues", whichever side won
      link(innerFin, port0, outerFin, port0),
      link(innerFin, port1, outerFin, port0),
      link(outer, port1, outerCatch, port0), // outer Catch
      link(outerCatch, port0, outerFin, port1) // outer From Catch
    )
    val materialized = runAndGetMaterializedRowCounts(operators, links)
    // inner catch recovered 2 rows (inner Catch Result); the outer frame's try
    // side is clean, so the outer Finally emits those 2 rows out its own
    // Try Result (NOT the outer catch's 1 row)
    assert(materialized(outerFin.operatorIdentifier) == 2)
    assert(portRowCount(outerFin, port1) == 0)
  }

  it should "escalate to the outer catch when both inner branches fail" in {
    // Same shape as above, but the inner CATCH branch also fails: the inner
    // frame cannot recover, so the failure escalates and the outer catch runs.
    val src = textInput("1\n2\n3")
    val outer = new TryCatchOpDesc()
    val inner = new TryCatchOpDesc()
    val innerTry = failingFilter()
    val innerCatch = failingFilter()
    val innerFin = new FinallyOpDesc()
    val outerCatch = limit(1)
    val outerFin = new FinallyOpDesc()

    val operators =
      List(src, outer, inner, innerTry, innerCatch, innerFin, outerCatch, outerFin)
    val links = List(
      link(src, port0, outer, port0),
      link(outer, port0, inner, port0),
      link(inner, port0, innerTry, port0),
      link(inner, port1, innerCatch, port0),
      link(innerTry, port0, innerFin, port0),
      link(innerCatch, port0, innerFin, port1),
      link(innerFin, port0, outerFin, port0),
      link(innerFin, port1, outerFin, port0),
      link(outer, port1, outerCatch, port0),
      link(outerCatch, port0, outerFin, port1)
    )
    val materialized = runAndGetMaterializedRowCounts(operators, links)
    // double inner failure => the outer catch's limit(1) result wins, and the
    // recovery is visible as rows on the outer Catch Result port
    assert(materialized(outerFin.operatorIdentifier) == 0)
    assert(portRowCount(outerFin, port1) == 1)
  }

  it should "rethrow from a nested frame with an unconnected Catch to the enclosing catch" in {
    // PL: try1 { x = A(); try2 { B() } /* no catch2 */ } catch1 { C() } — an
    // uncaught inner failure aborts the OUTER attempt. The inner gate, having
    // no catch subgraph, forwards the error instead of absorbing it; the
    // forward travels its dangling ports' signal edges to the outer gate
    // (replay) and the outer Merger (release agreement).
    val src = textInput("1\n2\n3")
    val outer = new TryCatchOpDesc()
    val outerTail = limit(3) // outer's own try path: completes CLEAN
    val inner = new TryCatchOpDesc() // Catch left unconnected: pure guard
    val innerTry = failingFilter() // terminal; fails => rethrow
    val outerCatch = limit(2)
    val fin = new FinallyOpDesc()

    val materialized = runAndGetMaterializedRowCounts(
      List(src, outer, outerTail, inner, innerTry, outerCatch, fin),
      List(
        link(src, port0, outer, port0),
        link(outer, port0, outerTail, port0),
        link(outer, port0, inner, port0),
        link(inner, port0, innerTry, port0), // inner Try; Catch unconnected
        link(outerTail, port0, fin, port0), // From Try (clean data!)
        link(outer, port1, outerCatch, port0), // outer Catch
        link(outerCatch, port0, fin, port1) // From Catch
      )
    )
    // the outer catch replayed all 3 rows through limit(2); and although the
    // wired try path completed cleanly, the Merger must NOT flush it — the
    // attempt as a whole failed
    assert(materialized(fin.operatorIdentifier) == 0)
    assert(portRowCount(fin, port1) == 2)
  }

  it should "release the recovery when a terminal try fork fails but the wired fork is clean" in {
    // The gate hears every ending of the try cone; the Merger must reach the
    // same verdict. A failing terminal fork used to be invisible to the
    // Merger (no error on From Try), which then flushed the clean fork's
    // rows while the gate was simultaneously releasing the catch replay.
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val okFork = limit(3) // wired into the Finally, completes clean
    val badFork = failingFilter() // terminal, dies
    val catchBranch = limit(1)
    val fin = new FinallyOpDesc()

    val materialized = runAndGetMaterializedRowCounts(
      List(src, tryCatch, okFork, badFork, catchBranch, fin),
      List(
        link(src, port0, tryCatch, port0),
        link(tryCatch, port0, okFork, port0),
        link(tryCatch, port0, badFork, port0),
        link(okFork, port0, fin, port0), // From Try
        link(tryCatch, port1, catchBranch, port0),
        link(catchBranch, port0, fin, port1) // From Catch
      )
    )
    assert(materialized(fin.operatorIdentifier) == 0) // NOT okFork's 3 rows
    assert(portRowCount(fin, port1) == 1) // the replay through limit(1)
  }

  it should "release nothing when a terminal fork of the CATCH branch fails" in {
    // The recovery itself forked and one fork died: the recovery is not
    // whole, so the Merger must not release the surviving fork's rows as if
    // it were. Both result ports stay empty and the run still terminates.
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val tryBranch = failingFilter() // the attempt fails => catch replays
    val catchOk = limit(2) // surviving fork, wired into the Finally
    val catchBad = failingFilter() // terminal fork of the recovery, dies
    val fin = new FinallyOpDesc()

    val materialized = runAndGetMaterializedRowCounts(
      List(src, tryCatch, tryBranch, catchOk, catchBad, fin),
      List(
        link(src, port0, tryCatch, port0),
        link(tryCatch, port0, tryBranch, port0),
        link(tryBranch, port0, fin, port0), // From Try (poisoned)
        link(tryCatch, port1, catchOk, port0),
        link(tryCatch, port1, catchBad, port0),
        link(catchOk, port0, fin, port1) // From Catch
      )
    )
    assert(materialized(fin.operatorIdentifier) == 0)
    assert(portRowCount(fin, port1) == 0) // suppressed: half a recovery is no recovery
  }

  it should "support an inner frame inside the CATCH branch: try1 {} catch1 { try2 {} catch2 {} } finally1" in {
    // The PL shape `try1 { attempt } catch1 { try2 { A } catch2 { B } } finally1`:
    // the outer recovery is itself guarded. The inner construct's closing
    // brace is its own Finally, whose result ports union into the outer
    // From Catch. Here BOTH attempts fail, so the rows that reach the outer
    // Finally are the inner frame's recovery — a catch inside a catch.
    val src = textInput("1\n2\n3")
    val outer = new TryCatchOpDesc()
    val outerTry = failingFilter() // outer attempt fails => catch1 runs
    val inner = new TryCatchOpDesc() // catch1's body IS an inner frame
    val innerTry = failingFilter() // inner attempt fails too => catch2 runs
    val innerCatch = limit(2)
    val innerFin = new FinallyOpDesc()
    val outerFin = new FinallyOpDesc()

    val operators =
      List(src, outer, outerTry, inner, innerTry, innerCatch, innerFin, outerFin)
    val links = List(
      link(src, port0, outer, port0),
      link(outer, port0, outerTry, port0), // try1
      link(outerTry, port0, outerFin, port0), // From Try (poisoned, discarded)
      link(outer, port1, inner, port0), // catch1 = inner frame's input
      link(inner, port0, innerTry, port0), // try2
      link(inner, port1, innerCatch, port0), // catch2
      link(innerTry, port0, innerFin, port0),
      link(innerCatch, port0, innerFin, port1),
      // inner closing brace: winner (whichever side) -> outer From Catch
      link(innerFin, port0, outerFin, port1),
      link(innerFin, port1, outerFin, port1)
    )
    val materialized = runAndGetMaterializedRowCounts(operators, links)
    // outer attempt failed; the recovery's own attempt failed; the inner
    // catch replayed all 3 rows through limit(2) => the outer construct's
    // value is those 2 rows, out its Catch Result port
    assert(materialized(outerFin.operatorIdentifier) == 0)
    assert(portRowCount(outerFin, port1) == 2)
  }

  it should "recover independently in sibling Finally-less inner frames (self-contained branches)" in {
    // Two inner TryCatch frames WITHOUT Finallys, side by side inside an
    // outer frame. A Finally-less frame is terminal: its branches end in
    // their own result tables. Each inner frame owns its own failure (the
    // innermost-frame rule), recovers independently, and a successful
    // recovery is invisible to the outer frame — whose own try path and
    // Finally proceed as a clean run.
    val src = textInput("1\n2\n3")
    val outer = new TryCatchOpDesc()
    val outerTail = limit(3) // outer's own try path, feeds the outer Finally
    val inner1 = new TryCatchOpDesc()
    val inner1Try = failingFilter() // fails => inner1's catch replays
    val inner1Catch = limit(2) // terminal: its result table is the recovery
    val inner2 = new TryCatchOpDesc()
    val inner2Try = limit(1) // clean => inner2's catch stays empty
    val inner2Catch = limit(3)
    val outerCatch = limit(1)
    val fin = new FinallyOpDesc()

    val operators = List(
      src,
      outer,
      outerTail,
      inner1,
      inner1Try,
      inner1Catch,
      inner2,
      inner2Try,
      inner2Catch,
      outerCatch,
      fin
    )
    val links = List(
      link(src, port0, outer, port0),
      link(outer, port0, outerTail, port0), // outer try path
      link(outer, port0, inner1, port0), // fan-out into inner frame 1
      link(outer, port0, inner2, port0), // fan-out into inner frame 2
      link(inner1, port0, inner1Try, port0),
      link(inner1, port1, inner1Catch, port0),
      link(inner2, port0, inner2Try, port0),
      link(inner2, port1, inner2Catch, port0),
      link(outerTail, port0, fin, port0), // outer From Try
      link(outer, port1, outerCatch, port0), // outer Catch
      link(outerCatch, port0, fin, port1) // outer From Catch
    )
    val materialized = runAndGetMaterializedRowCounts(operators, links)
    // inner1 failed and recovered: its catch replayed all 3 rows, limit(2)
    assert(materialized(inner1Catch.operatorIdentifier) == 2)
    // the failing branch's own table is empty
    assert(materialized(inner1Try.operatorIdentifier) == 0)
    // inner2 was clean: try table filled, catch ran empty
    assert(materialized(inner2Try.operatorIdentifier) == 1)
    assert(materialized(inner2Catch.operatorIdentifier) == 0)
    // both inner outcomes are invisible to the outer frame: clean try side
    assert(materialized(fin.operatorIdentifier) == 3)
    assert(portRowCount(fin, port1) == 0)
  }

  it should "route the catch branch by error type: catch(SpecificError) via Error Info + If" in {
    // Simulates `catch (SpecificError e)`: a classifier UDF consumes the
    // frame's Error Info rows and emits a boolean State; an If routes the
    // Catch replay to the specific handler when the error matches, and to the
    // generic handler otherwise. The failing filter's message mentions the
    // missing attribute, so the specific branch (limit 2) must win over the
    // generic one (limit 1).
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val classifier = new PythonUDFOpDescV2()
    classifier.code = """
from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):
    matched = False

    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        if "no_such_attribute" in str(tuple_["message"]):
            self.matched = True
        yield

    def produce_state_on_finish(self, port: int):
        return {"isSpecific": self.matched}
"""
    classifier.workers = 1
    val ifOp = new org.apache.texera.amber.operator.ifStatement.IfOpDesc()
    ifOp.conditionName = "isSpecific"
    val specificHandler = limit(2) // runs on isSpecific == true (If port 1)
    val genericHandler = limit(1) // runs otherwise (If port 0)
    val fin = new FinallyOpDesc()
    val failing = failingFilter()

    val operators =
      List(src, tryCatch, failing, classifier, ifOp, specificHandler, genericHandler, fin)
    val links = List(
      link(src, port0, tryCatch, port0),
      link(tryCatch, port0, failing, port0), // Try: fails on first tuple
      link(failing, port0, fin, port0), // From Try (poisoned, discarded)
      link(tryCatch, PortIdentity(2), classifier, port0), // Error Info
      link(classifier, port0, ifOp, port0), // If condition (dependee)
      link(tryCatch, port1, ifOp, port1), // If data = Catch replay
      link(ifOp, port1, specificHandler, port0), // True branch
      link(ifOp, port0, genericHandler, port0), // False branch
      link(specificHandler, port0, fin, port1), // From Catch (union)
      link(genericHandler, port0, fin, port1)
    )
    val materialized = runAndGetMaterializedRowCounts(operators, links)
    // specific handler passed 2 of the 3 replayed rows; generic saw none;
    // the recovery leaves Finally through Catch Result (port 1)
    assert(materialized(fin.operatorIdentifier) == 0)
    assert(portRowCount(fin, port1) == 2)
  }

  it should "emit recovery rows only when the recovery's own inner attempt fails: catch1 { try2 { sideEffect } catch2 { B } } finally1" in {
    // A nested frame inside the catch block whose CATCH branch alone feeds
    // the enclosing Finally (its try branch is a terminal side effect):
    // real-life "in the recovery, attempt a side-effecting operation; if IT
    // fails, route fallback rows onward instead". The Finally is fed only
    // through the nested frame's gate, which the strict pairing pass cannot
    // see — the two-pass pairing resolves it. Here BOTH attempts fail, so B
    // replays the recovery and its rows are the outer construct's value.
    val src = textInput("1\n2\n3")
    val outer = new TryCatchOpDesc()
    val outerTry = failingFilter() // outer attempt fails => catch1 runs
    val inner = new TryCatchOpDesc() // catch1's body: a guarded side effect
    val innerSide = failingFilter() // terminal side effect; fails => B runs
    val fallback = limit(2)
    val fin = new FinallyOpDesc()

    val materialized = runAndGetMaterializedRowCounts(
      List(src, outer, outerTry, inner, innerSide, fallback, fin),
      List(
        link(src, port0, outer, port0),
        link(outer, port0, outerTry, port0), // try1
        link(outerTry, port0, fin, port0), // From Try (poisoned, discarded)
        link(outer, port1, inner, port0), // catch1 = the nested frame
        link(inner, port0, innerSide, port0), // try2, terminal
        link(inner, port1, fallback, port0), // catch2
        link(fallback, port0, fin, port1) // ONLY catch2 continues onward
      )
    )
    assert(materialized(fin.operatorIdentifier) == 0)
    assert(portRowCount(fin, port1) == 2) // the fallback's replay
    assert(materialized(innerSide.operatorIdentifier) == 0)
  }

  it should "keep the Finally intact when the Error Info branch itself fails" in {
    // try fails -> catch recovers -> the REPORTER lane (Error Info consumers)
    // fails too. The reporter sits outside both cones, so its failure cannot
    // reach the Finally (forward-only poison + provenance): the recovery is
    // delivered untouched and the run terminates. The reporter lane is
    // wrapped in its own guard here — an UNGUARDED reporter failure pauses
    // the workflow instead, by the pause-preservation rule.
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val failing = failingFilter()
    val catchBranch = limit(2)
    val fin = new FinallyOpDesc()
    val reporterGuard = new TryCatchOpDesc() // Catch unconnected: pure guard
    val reporterBoom = failingFilter() // dies on the first Error Info row

    val materialized = runAndGetMaterializedRowCounts(
      List(src, tryCatch, failing, catchBranch, fin, reporterGuard, reporterBoom),
      List(
        link(src, port0, tryCatch, port0),
        link(tryCatch, port0, failing, port0), // Try
        link(failing, port0, fin, port0), // From Try (poisoned, discarded)
        link(tryCatch, port1, catchBranch, port0), // Catch
        link(catchBranch, port0, fin, port1), // From Catch
        link(tryCatch, PortIdentity(2), reporterGuard, port0), // Error Info
        link(reporterGuard, port0, reporterBoom, port0)
      )
    )
    // the recovery is exactly the catch rows, despite the reporter dying
    assert(materialized(fin.operatorIdentifier) == 0)
    assert(portRowCount(fin, port1) == 2)
    assert(materialized(reporterBoom.operatorIdentifier) == 0)
  }

  it should "catch and recover on EVERY iteration of a loop around the frame" in {
    // A frame inside a loop body: each iteration replays one row through the
    // frame, the attempt fails, the catch recovers it, and the loop takes the
    // back-edge. Workers are recreated per iteration, so this exercises the
    // whole frame apparatus (signal edges, snapshot release, staging) being
    // rebuilt every time; the LoopEnd's materialized table accumulates across
    // iterations (reuseStorage), so 3 rows == the frame recovered 3 times.
    val src = textInput("1\n2\n3")
    val start = new LoopStartOpDesc()
    start.initialization = "i = 0"
    start.output = "table.iloc[i]"
    val tryCatch = new TryCatchOpDesc()
    val tryBranch = failingFilter()
    val catchBranch = limit(1)
    val fin = new FinallyOpDesc()
    val end = new LoopEndOpDesc()
    end.update = "i += 1"
    end.condition = "i < len(table)"

    val operators = List(src, start, tryCatch, tryBranch, catchBranch, fin, end)
    val links = List(
      link(src, port0, start, port0),
      link(start, port0, tryCatch, port0),
      link(tryCatch, port0, tryBranch, port0),
      link(tryBranch, port0, fin, port0),
      link(tryCatch, port1, catchBranch, port0),
      link(catchBranch, port0, fin, port1),
      // the construct's value continues into the loop tail, whichever side won
      link(fin, port0, end, port0),
      link(fin, port1, end, port0)
    )
    // loops require MATERIALIZED execution (the back-edge is a cross-region
    // state channel), so this test runs under its own context settings
    val materialized = runWorkflowAndReadResults(
      system,
      buildWorkflow(
        operators,
        links,
        workflowContext(
          specId,
          WorkflowSettings(dataTransferBatchSize = 400, executionMode = ExecutionMode.MATERIALIZED)
        )
      ),
      operators.map(_.operatorIdentifier),
      _.getCount,
      Duration.fromSeconds(90)
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 3,
      s"the frame must recover once per iteration and the loop must run all 3: got $endRows " +
        s"(all: $materialized)"
    )
  }

  it should "guard a Python UDF failure and fall back to the catch branch" in {
    // The common real-world case: user code in a Python UDF raises. This
    // exercises the pyamber side of the design (error State emission +
    // per-port drain in MainLoop) end to end.
    val src = textInput("1\n2\n3")
    val tryCatch = new TryCatchOpDesc()
    val udf = new PythonUDFOpDescV2()
    // pass the input schema through, so the Finally's two branches match
    udf.retainInputColumns = true
    udf.code = """
from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):
    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        raise ValueError("boom from python udf")
        yield
"""
    udf.workers = 1
    val catchBranch = limit(2)
    val fin = new FinallyOpDesc()
    val materialized = runAndGetMaterializedRowCounts(
      List(src, tryCatch, udf, catchBranch, fin),
      List(
        link(src, port0, tryCatch, port0),
        link(tryCatch, port0, udf, port0),
        link(tryCatch, port1, catchBranch, port0),
        link(udf, port0, fin, port0),
        link(catchBranch, port0, fin, port1)
      )
    )
    assert(materialized(fin.operatorIdentifier) == 0)
    assert(portRowCount(fin, port1) == 2)
  }
}
