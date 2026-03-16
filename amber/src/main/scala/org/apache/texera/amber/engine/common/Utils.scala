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

package org.apache.texera.amber.engine.common

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.locks.Lock
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Using

object Utils extends LazyLogging {

  /**
    * Gets the real path of the amber home directory by:
    * 1): checking whether the current directory is `texera/amber`
    * if it's not then:
    * 2): searching siblings and children for an amber home path, preferring matches under the
    *     current working directory before falling back to the first discovered match
    *
    * @return the real absolute path to amber home directory
    */
  lazy val amberHomePath: Path = {
    resolveAmberHomePath(Paths.get(".").toRealPath())
  }

  private[common] def resolveAmberHomePath(currentWorkingDirectory: Path): Path = {
    val realCurrentWorkingDirectory = currentWorkingDirectory.toRealPath()

    if (isAmberHomePath(realCurrentWorkingDirectory)) {
      realCurrentWorkingDirectory
    } else {
      val parent = Option(realCurrentWorkingDirectory.getParent).getOrElse {
        throw new RuntimeException(
          s"Cannot search for amber home from filesystem root: $realCurrentWorkingDirectory"
        )
      }

      val amberCandidates =
        Using.resource(Files.walk(parent, 2)) { stream =>
          stream.iterator().asScala.flatMap(normalizeAmberHomePath).toVector
        }

      // Sort candidates to avoid dependence on Files.walk traversal order.
      val amberCandidatesSorted = amberCandidates.sortBy(_.toString)

      // Preserve the current behavior by preferring an amber directory discovered under the CWD.
      amberCandidatesSorted
        .filter(_.startsWith(realCurrentWorkingDirectory))
        .maxByOption(_.getNameCount)
        .orElse(amberCandidatesSorted.headOption)
        .getOrElse {
          throw new RuntimeException(
            s"Finding amber home path failed. Current working directory is $realCurrentWorkingDirectory"
          )
        }
    }
  }
  val AMBER_HOME_FOLDER_NAME = "amber";

  /**
    * Retry the given logic with a backoff time interval. The attempts are executed sequentially, thus blocking the thread.
    * Backoff time is doubled after each attempt.
    *
    * @param attempts            total number of attempts. if n <= 1 then it will not retry at all, decreased by 1 for each recursion.
    * @param baseBackoffTimeInMS time to wait before next attempt, started with the base time, and doubled after each attempt.
    * @param fn                  the target function to execute.
    * @tparam T any return type from the provided function fn.
    * @return the provided function fn's return, or any exception that still being raised after n attempts.
    */
  @tailrec
  def retry[T](attempts: Int, baseBackoffTimeInMS: Long)(fn: => T): T = {
    try {
      fn
    } catch {
      case e: Throwable =>
        if (attempts > 1) {
          logger.warn(
            "retrying after " + baseBackoffTimeInMS + "ms, number of attempts left: " + (attempts - 1),
            e
          )
          Thread.sleep(baseBackoffTimeInMS)
          retry(attempts - 1, baseBackoffTimeInMS * 2)(fn)
        } else throw e
    }
  }

  private def isAmberHomePath(path: Path): Boolean = {
    normalizeAmberHomePath(path).nonEmpty
  }

  private def normalizeAmberHomePath(path: Path): Option[Path] = {
    val realPath = path.toRealPath()
    Option.when(realPath.endsWith(AMBER_HOME_FOLDER_NAME))(realPath)
  }

  def aggregatedStateToString(state: WorkflowAggregatedState): String = {
    state match {
      case WorkflowAggregatedState.UNINITIALIZED => "Uninitialized"
      case WorkflowAggregatedState.READY         => "Initializing"
      case WorkflowAggregatedState.RUNNING       => "Running"
      case WorkflowAggregatedState.PAUSING       => "Pausing"
      case WorkflowAggregatedState.PAUSED        => "Paused"
      case WorkflowAggregatedState.RESUMING      => "Resuming"
      case WorkflowAggregatedState.COMPLETED     => "Completed"
      case WorkflowAggregatedState.TERMINATED    => "Terminated"
      case WorkflowAggregatedState.FAILED        => "Failed"
      case WorkflowAggregatedState.KILLED        => "Killed"
      case WorkflowAggregatedState.UNKNOWN       => "Unknown"
      case WorkflowAggregatedState.Unrecognized(unrecognizedValue) =>
        s"Unrecognized($unrecognizedValue)"
    }
  }

  def stringToAggregatedState(str: String): WorkflowAggregatedState = {
    str.trim.toLowerCase match {
      case "uninitialized" => WorkflowAggregatedState.UNINITIALIZED
      case "ready"         => WorkflowAggregatedState.READY
      case "initializing"  => WorkflowAggregatedState.READY // accept alias
      case "running"       => WorkflowAggregatedState.RUNNING
      case "pausing"       => WorkflowAggregatedState.PAUSING
      case "paused"        => WorkflowAggregatedState.PAUSED
      case "resuming"      => WorkflowAggregatedState.RESUMING
      case "completed"     => WorkflowAggregatedState.COMPLETED
      case "failed"        => WorkflowAggregatedState.FAILED
      case "killed"        => WorkflowAggregatedState.KILLED
      case "terminated"    => WorkflowAggregatedState.TERMINATED
      case "unknown"       => WorkflowAggregatedState.UNKNOWN
      case other           => throw new IllegalArgumentException(s"Unrecognized state: $other")
    }
  }

  /**
    * @param state indicates the workflow state
    * @return code indicates the status of the execution in the DB it is 0 by default for any unused states.
    *         This code is stored in the DB and read in the frontend.
    *         If these codes are changed, they also have to be changed in the frontend `ngbd-modal-workflow-executions.component.ts`
    */
  def maptoStatusCode(state: WorkflowAggregatedState): Byte = {
    state match {
      case WorkflowAggregatedState.UNINITIALIZED => 0
      case WorkflowAggregatedState.READY         => 0
      case WorkflowAggregatedState.RUNNING       => 1
      case WorkflowAggregatedState.PAUSED        => 2
      case WorkflowAggregatedState.COMPLETED     => 3
      case WorkflowAggregatedState.FAILED        => 4
      case WorkflowAggregatedState.KILLED        => 5
      case other                                 => -1
    }
  }

  def withLock[X](instructions: => X)(implicit lock: Lock): X = {
    lock.lock()
    try {
      instructions
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      lock.unlock()
    }
  }
}
