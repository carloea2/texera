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

package org.apache.texera.web.resource.dashboard.user.workflow

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll

import java.sql.Timestamp
import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import javax.ws.rs.NotAcceptableException

class WorkflowResourceDashboardUserSpec extends AnyFlatSpec with BeforeAndAfterAll with MockTexeraDB {

  private val widSeq = new AtomicInteger(3000)
  private val uidSeq = new AtomicInteger(1000)

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def seedUser(uid: Int, name: String): Unit = {
    val userDao = new UserDao(getDSLContext.configuration())
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"$name@example.com")
    user.setPassword("password")
    userDao.insert(user)
  }

  private def seedWorkflow(wid: Int): Unit = {
    val workflowDao = new WorkflowDao(getDSLContext.configuration())
    val wf = new Workflow
    wf.setWid(wid)
    wf.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    wf.setContent("{}")
    wf.setDescription("test description")
    wf.setCreationTime(new Timestamp(System.currentTimeMillis()))
    wf.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(wf)
  }

  /** Setup row needed by /owner_info */
  private def linkWorkflowOwner(wid: Int, uid: Int): Unit = {
    getDSLContext
      .insertInto(WORKFLOW_OF_USER)
      .set(WORKFLOW_OF_USER.WID, Integer.valueOf(wid))
      .set(WORKFLOW_OF_USER.UID, Integer.valueOf(uid))
      .execute()
  }

  // --- Owner-info test fixture (reusable for any owner_info-related tests) ---
  private def withOwnerInfoFixture(testCode: (Int, Int) => Any): Unit = {
    val wid = widSeq.getAndIncrement()
    val uid = uidSeq.getAndIncrement()

    seedUser(uid, "test_user")
    seedWorkflow(wid)
    linkWorkflowOwner(wid, uid)

    try testCode(wid, uid)
    finally cleanupOwnerInfoFixture(wid, uid)
  }

  private def cleanupOwnerInfoFixture(wid: Int, uid: Int): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_OF_USER)
      .where(WORKFLOW_OF_USER.WID.eq(wid))
      .and(WORKFLOW_OF_USER.UID.eq(uid))
      .execute()

    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(wid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(uid)).execute()
  }

  // --- Tests ---

  "WorkflowResource /owner_info" should "return owner info name when fields includes name" in
    withOwnerInfoFixture { (wid, _) =>
      val resource = new WorkflowResource
      val owner = resource.getOwnerInfo(wid, util.Arrays.asList("name"))
      assert(owner.getName == "test_user")
    }

  it should "default owner info fields to name when fields is missing" in
    withOwnerInfoFixture { (wid, _) =>
      val resource = new WorkflowResource
      val owner = resource.getOwnerInfo(wid, null)
      assert(owner.getName == "test_user")
    }

  it should "reject unsupported owner info fields" in
    withOwnerInfoFixture { (wid, _) =>
      val resource = new WorkflowResource
      assertThrows[NotAcceptableException] {
        resource.getOwnerInfo(wid, util.Arrays.asList("password"))
      }
    }
}
