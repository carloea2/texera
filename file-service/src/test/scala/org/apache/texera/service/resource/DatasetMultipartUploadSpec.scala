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

package org.apache.texera.service.resource

import jakarta.ws.rs._
import jakarta.ws.rs.core.{Cookie, HttpHeaders, MediaType, MultivaluedHashMap, Response}

import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import io.lakefs.clients.sdk.ApiException
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import org.apache.texera.service.MockLakeFS
import org.apache.texera.service.util.S3StorageClient

import org.jooq.SQLDialect
import org.jooq.impl.DSL

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import java.io.ByteArrayInputStream
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.{Collections, Date, Locale, Optional}
import java.util.concurrent.CyclicBarrier
import scala.util.Random

class DatasetMultipartUploadSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // ---------- execution context (for race tests) ----------
  private implicit val ec: ExecutionContext = ExecutionContext.global

  // ---------- test fixtures ----------
  private val testUser: User = {
    val u = new User
    u.setName("multipart_user")
    u.setPassword("123")
    u.setEmail("multipart_user@test.com")
    u.setRole(UserRoleEnum.ADMIN)
    u
  }

  // Make user2 a REGULAR user (so @RolesAllowed still allows calling),
  // but they should not have WRITE access to someone else's dataset.
  private val testUser2: User = {
    val u = new User
    u.setName("multipart_user2")
    u.setPassword("123")
    u.setEmail("multipart_user2@test.com")
    u.setRole(UserRoleEnum.REGULAR)
    u
  }

  // Use a unique LakeFS repo per test run to avoid 409 Conflict when re-running tests.
  private val testRepoName: String =
    s"multipart-ds-${System.nanoTime()}-${Random.alphanumeric.take(6).mkString.toLowerCase}"

  private val testDataset: Dataset = {
    val ds = new Dataset
    ds.setName("multipart-ds")
    ds.setRepositoryName(testRepoName)
    ds.setIsPublic(true)
    ds.setIsDownloadable(true)
    ds.setDescription("dataset for multipart upload tests")
    ds
  }

  lazy val datasetDao = new DatasetDao(getDSLContext.configuration())
  lazy val datasetResource = new DatasetResource()

  lazy val sessionUser = new SessionUser(testUser)
  lazy val sessionUser2 = new SessionUser(testUser2)

  // ---------- lifecycle ----------
  override protected def beforeAll(): Unit = {
    super.beforeAll()

    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(testUser)
    userDao.insert(testUser2)

    testDataset.setOwnerUid(testUser.getUid)
    datasetDao.insert(testDataset)
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // Repo must exist for presigned multipart init to succeed.
    // If it already exists (e.g., repeated local test runs against the same LakeFS instance), ignore 409.
    try LakeFSStorageClient.initRepo(testDataset.getRepositoryName)
    catch {
      case e: ApiException if e.getCode == 409 => // already exists; OK
    }
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }

  // ---------- helpers ----------
  private def enc(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.name())

  /**
    * Minimum part-size rule (S3-style):
    * every part except the LAST must be >= 5 MiB.
    *
    * Keep this aligned with the server-side constant.
    */
  private val MinNonFinalPartBytes: Int = 5 * 1024 * 1024

  private def minPartBytes(b: Byte): Array[Byte] =
    Array.fill[Byte](MinNonFinalPartBytes)(b)

  private def tinyBytes(b: Byte, n: Int = 1): Array[Byte] =
    Array.fill[Byte](n)(b)

  /** Minimal HttpHeaders impl needed by DatasetResource.uploadPart:
    * it reads Content-Length via getHeaderString + getRequestHeader.
    */
  private def mkHeaders(contentLength: Long): HttpHeaders =
    new HttpHeaders {
      private val headers = new MultivaluedHashMap[String, String]()
      headers.putSingle(HttpHeaders.CONTENT_LENGTH, contentLength.toString)

      override def getHeaderString(name: String): String = headers.getFirst(name)

      override def getRequestHeaders = headers

      override def getRequestHeader(name: String) =
        Option(headers.get(name)).getOrElse(Collections.emptyList[String]())

      override def getAcceptableMediaTypes = Collections.emptyList[MediaType]()
      override def getAcceptableLanguages = Collections.emptyList[Locale]()
      override def getMediaType: MediaType = null
      override def getLanguage: Locale = null
      override def getCookies = Collections.emptyMap[String, Cookie]()
      override def getDate: Date = null
      override def getLength: Int = contentLength.toInt
    }

  private def mkHeadersMissingContentLength: HttpHeaders =
    new HttpHeaders {
      private val headers = new MultivaluedHashMap[String, String]()

      override def getHeaderString(name: String): String = null
      override def getRequestHeaders = headers
      override def getRequestHeader(name: String) = Collections.emptyList[String]()

      override def getAcceptableMediaTypes = Collections.emptyList[MediaType]()
      override def getAcceptableLanguages = Collections.emptyList[Locale]()
      override def getMediaType: MediaType = null
      override def getLanguage: Locale = null
      override def getCookies = Collections.emptyMap[String, Cookie]()
      override def getDate: Date = null
      override def getLength: Int = -1
    }

  private def uniqueFilePath(prefix: String): String =
    s"$prefix/${System.nanoTime()}-${Random.alphanumeric.take(8).mkString}.bin"

  private def initUpload(
      filePath: String,
      numParts: Int,
      user: SessionUser = sessionUser
  ): Response =
    datasetResource.multipartUpload(
      "init",
      testUser.getEmail,
      testDataset.getName,
      enc(filePath),
      Optional.of(numParts),
      user
    )

  private def finishUpload(filePath: String, user: SessionUser = sessionUser): Response =
    datasetResource.multipartUpload(
      "finish",
      testUser.getEmail,
      testDataset.getName,
      enc(filePath),
      Optional.empty(),
      user
    )

  private def abortUpload(filePath: String, user: SessionUser = sessionUser): Response =
    datasetResource.multipartUpload(
      "abort",
      testUser.getEmail,
      testDataset.getName,
      enc(filePath),
      Optional.empty(),
      user
    )

  private def uploadPart(
      filePath: String,
      partNumber: Int,
      bytes: Array[Byte],
      user: SessionUser = sessionUser,
      contentLengthOverride: Option[Long] = None,
      missingContentLength: Boolean = false
  ): Response = {
    val hdrs =
      if (missingContentLength) mkHeadersMissingContentLength
      else mkHeaders(contentLengthOverride.getOrElse(bytes.length.toLong))

    datasetResource.uploadPart(
      testUser.getEmail,
      testDataset.getName,
      enc(filePath),
      partNumber,
      new ByteArrayInputStream(bytes),
      hdrs,
      user
    )
  }

  private def fetchSession(filePath: String) =
    getDSLContext
      .selectFrom(DATASET_UPLOAD_SESSION)
      .where(
        DATASET_UPLOAD_SESSION.UID
          .eq(testUser.getUid)
          .and(DATASET_UPLOAD_SESSION.DID.eq(testDataset.getDid))
          .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
      )
      .fetchOne()

  // ---------------------------------------------------------------------------
  // INIT TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload?type=init" should "create an upload session row (happy path)" in {
    val filePath = uniqueFilePath("init-happy")
    val resp = initUpload(filePath, numParts = 3)

    resp.getStatus shouldEqual 200

    val s = fetchSession(filePath)
    s should not be null
    s.getNumPartsRequested shouldEqual 3
    s.getUploadId should not be null
    s.getPhysicalAddress should not be null
  }

  it should "reject invalid numParts (0, negative, too large)" in {
    val filePath = uniqueFilePath("init-bad-numparts")

    assertThrows[BadRequestException] { initUpload(filePath, 0) }
    assertThrows[BadRequestException] { initUpload(filePath, -1) }

    // choose a very large number to exceed max parts
    assertThrows[BadRequestException] { initUpload(filePath, 1000000000) }
  }

  it should "reject invalid filePath" in {
    // DatasetResource validates filePath (rejects empty, absolute, '.' segments, etc.).
    assertThrows[BadRequestException] { initUpload("./nope.bin", 2) }
    assertThrows[BadRequestException] { initUpload("/absolute.bin", 2) }
    assertThrows[BadRequestException] { initUpload("a/./b.bin", 2) }
  }

  it should "reject init when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("init-forbidden")
    assertThrows[ForbiddenException] {
      initUpload(filePath, numParts = 2, user = sessionUser2)
    }
  }

  it should "handle init race: exactly one succeeds, one gets 409 CONFLICT" in {
    val filePath = uniqueFilePath("init-race")
    val barrier = new CyclicBarrier(2)

    def callInit(): Either[Throwable, Response] =
      try {
        barrier.await()
        Right(initUpload(filePath, numParts = 2))
      } catch {
        case t: Throwable => Left(t)
      }

    val f1 = Future(callInit())
    val f2 = Future(callInit())
    val results = Await.result(Future.sequence(Seq(f1, f2)), 30.seconds)

    val oks = results.collect { case Right(r) if r.getStatus == 200 => r }
    val fails = results.collect { case Left(t) => t }

    oks.size shouldEqual 1
    fails.size shouldEqual 1

    fails.head match {
      case e: WebApplicationException =>
        e.getResponse.getStatus shouldEqual 409
      case other =>
        fail(
          s"Expected WebApplicationException(CONFLICT), got: ${other.getClass} / ${other.getMessage}"
        )
    }

    fetchSession(filePath) should not be null
  }

  // ---------------------------------------------------------------------------
  // PART UPLOAD TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload/part" should "reject uploadPart if init was not called" in {
    val filePath = uniqueFilePath("part-no-init")
    assertThrows[NotFoundException] {
      uploadPart(filePath, partNumber = 1, bytes = Array[Byte](1, 2, 3))
    }
  }

  it should "reject missing/invalid Content-Length" in {
    val filePath = uniqueFilePath("part-bad-cl")
    initUpload(filePath, numParts = 2)

    // Missing header
    assertThrows[BadRequestException] {
      uploadPart(
        filePath,
        partNumber = 1,
        bytes = Array[Byte](1, 2, 3),
        missingContentLength = true
      )
    }

    // Present but invalid (<= 0)
    assertThrows[BadRequestException] {
      uploadPart(
        filePath,
        partNumber = 1,
        bytes = Array[Byte](1, 2, 3),
        contentLengthOverride = Some(0L)
      )
    }
    assertThrows[BadRequestException] {
      uploadPart(
        filePath,
        partNumber = 1,
        bytes = Array[Byte](1, 2, 3),
        contentLengthOverride = Some(-5L)
      )
    }
  }

  it should "reject invalid partNumber (< 1) and partNumber > requested" in {
    val filePath = uniqueFilePath("part-bad-pn")
    initUpload(filePath, numParts = 2)

    assertThrows[BadRequestException] {
      uploadPart(filePath, partNumber = 0, bytes = tinyBytes(1.toByte))
    }

    // Ensure we don't accidentally fail the min-size check before we hit the "invalid partNumber" path.
    assertThrows[BadRequestException] {
      uploadPart(filePath, partNumber = 3, bytes = minPartBytes(2.toByte))
    }
  }

  it should "upload a part successfully and make it visible in S3 listParts" in {
    val filePath = uniqueFilePath("part-happy")
    initUpload(filePath, numParts = 2)

    // part 1 is NON-FINAL when numParts=2
    val bytes = minPartBytes(7.toByte)
    val resp = uploadPart(filePath, partNumber = 1, bytes = bytes)
    resp.getStatus shouldEqual 200

    val s = fetchSession(filePath)
    val (bucket, key) = LakeFSStorageClient.parsePhysicalAddress(s.getPhysicalAddress)

    val parts = S3StorageClient.listAllParts(bucket, key, s.getUploadId)
    parts.map(_.partNumber).toSet should contain(1)
  }

  // ---- NEW: minimum part size tests ----
  it should "reject a non-final part smaller than the minimum size" in {
    val filePath = uniqueFilePath("part-too-small-nonfinal")
    initUpload(filePath, numParts = 2) // part 1 is NON-FINAL

    val ex = intercept[BadRequestException] {
      uploadPart(filePath, partNumber = 1, bytes = tinyBytes(1.toByte)) // contentLength = 1
    }

    ex.getMessage should include(s"Part 1 is too small (1 bytes).")
    ex.getMessage should include("All non-final parts must be >=")
    ex.getMessage should include(s">= $MinNonFinalPartBytes bytes.")
  }

  it should "validate non-final part size using Content-Length header" in {
    val filePath = uniqueFilePath("part-too-small-by-header")
    initUpload(filePath, numParts = 2) // part 1 is NON-FINAL

    val declaredLen = (MinNonFinalPartBytes - 1).toLong
    val body = minPartBytes(9.toByte) // body is big enough, but header lies

    val ex = intercept[BadRequestException] {
      uploadPart(
        filePath,
        partNumber = 1,
        bytes = body,
        contentLengthOverride = Some(declaredLen)
      )
    }

    ex.getMessage should include(s"Part 1 is too small ($declaredLen bytes).")
    ex.getMessage should include("All non-final parts must be >=")
    ex.getMessage should include(s">= $MinNonFinalPartBytes bytes.")
  }

  // ---------------------------------------------------------------------------
  // FINISH TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload?type=finish" should "reject finish when no parts were uploaded" in {
    val filePath = uniqueFilePath("finish-no-parts")
    initUpload(filePath, numParts = 2)

    assertThrows[BadRequestException] {
      finishUpload(filePath)
    }
  }

  it should "reject finish when parts are missing" in {
    val filePath = uniqueFilePath("finish-missing")
    initUpload(filePath, numParts = 3)

    // part 1 is NON-FINAL when numParts=3
    uploadPart(filePath, partNumber = 1, bytes = minPartBytes(1.toByte))

    val ex = intercept[BadRequestException] {
      finishUpload(filePath)
    }
    ex.getMessage should include("Missing partNumbers")
    ex.getMessage should include("2")
    ex.getMessage should include("3")
  }

  it should "reject finish if extra parts exist in S3 (even if endpoint prevented them)" in {
    val filePath = uniqueFilePath("finish-extra")
    initUpload(filePath, numParts = 2)

    // part 1 NON-FINAL, part 2 FINAL
    uploadPart(filePath, partNumber = 1, bytes = minPartBytes(1.toByte))
    uploadPart(filePath, partNumber = 2, bytes = tinyBytes(2.toByte))

    // Bypass endpoint validation: insert extra part directly.
    val s = fetchSession(filePath)
    val (bucket, key) = LakeFSStorageClient.parsePhysicalAddress(s.getPhysicalAddress)

    val extra = Array(3.toByte)
    S3StorageClient.uploadPart(
      bucket = bucket,
      key = key,
      uploadId = s.getUploadId,
      partNumber = 3,
      inputStream = new ByteArrayInputStream(extra),
      contentLength = Some(extra.length.toLong)
    )

    val ex = intercept[BadRequestException] {
      finishUpload(filePath)
    }
    ex.getMessage should include("Unexpected partNumbers")
    ex.getMessage should include("3")
  }

  it should "finish successfully when all parts exist, and delete the upload session" in {
    val filePath = uniqueFilePath("finish-happy")
    initUpload(filePath, numParts = 3)

    // parts 1 and 2 are NON-FINAL, must be >= 5 MiB
    uploadPart(filePath, 1, minPartBytes(1.toByte))
    uploadPart(filePath, 2, minPartBytes(2.toByte))
    // part 3 is FINAL, can be small
    uploadPart(filePath, 3, tinyBytes(3.toByte))

    val resp = finishUpload(filePath)
    resp.getStatus shouldEqual 200

    fetchSession(filePath) shouldBe null
  }

  it should "return 409 CONFLICT if the session row is locked by another finalizer/aborter (race test)" in {
    val filePath = uniqueFilePath("finish-lock-race")
    initUpload(filePath, numParts = 1)

    // only part is FINAL, can be small
    uploadPart(filePath, 1, tinyBytes(1.toByte))

    // Hold row lock open in another connection.
    val cp = getDSLContext.configuration().connectionProvider()
    val conn = cp.acquire()
    conn.setAutoCommit(false)

    try {
      val locking = DSL.using(conn, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION)
        .where(
          DATASET_UPLOAD_SESSION.UID
            .eq(testUser.getUid)
            .and(DATASET_UPLOAD_SESSION.DID.eq(testDataset.getDid))
            .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        )
        .forUpdate()
        .fetchOne()

      val ex = intercept[WebApplicationException] {
        finishUpload(filePath)
      }
      ex.getResponse.getStatus shouldEqual 409
    } finally {
      conn.rollback()
      cp.release(conn)
    }
  }

  // ---------------------------------------------------------------------------
  // ABORT TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload?type=abort" should "abort successfully and delete the upload session" in {
    val filePath = uniqueFilePath("abort-happy")
    initUpload(filePath, numParts = 2)

    // part 1 is NON-FINAL when numParts=2
    uploadPart(filePath, 1, minPartBytes(1.toByte))

    val resp = abortUpload(filePath)
    resp.getStatus shouldEqual 200

    fetchSession(filePath) shouldBe null
  }

  it should "return 409 CONFLICT if the session row is locked by another finalizer/aborter (race test)" in {
    val filePath = uniqueFilePath("abort-lock-race")
    initUpload(filePath, numParts = 1)

    val cp = getDSLContext.configuration().connectionProvider()
    val conn = cp.acquire()
    conn.setAutoCommit(false)

    try {
      val locking = DSL.using(conn, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION)
        .where(
          DATASET_UPLOAD_SESSION.UID
            .eq(testUser.getUid)
            .and(DATASET_UPLOAD_SESSION.DID.eq(testDataset.getDid))
            .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        )
        .forUpdate()
        .fetchOne()

      val ex = intercept[WebApplicationException] {
        abortUpload(filePath)
      }
      ex.getResponse.getStatus shouldEqual 409
    } finally {
      conn.rollback()
      cp.release(conn)
    }
  }

  // ---------------------------------------------------------------------------
  // “VARIOUS FILE LENGTHS” END-TO-END CASES (UPDATED FOR MIN PART SIZE RULE)
  // ---------------------------------------------------------------------------

  behavior of "multipart upload end-to-end across varied sizes"

  // Model file sizes as: (numParts - 1) * 5MiB + tailBytes,
  // since the LAST part is allowed to be smaller.
  private val tailMatrix: Seq[(String, Int, Int)] = Seq(
    ("tail-1b", 1, 2),
    ("tail-8b", 8, 3),
    ("tail-4k", 4096, 4),
    ("tail-odd", 4096 + 7, 5)
  )

  tailMatrix.foreach {
    case (label, tailBytes, numParts) =>
      it should s"finish correctly for $label tailBytes=$tailBytes numParts=$numParts" in {
        val filePath = uniqueFilePath(s"e2e-$label")

        initUpload(filePath, numParts = numParts)

        // Upload non-final parts at minimum size
        (1 until numParts).foreach { pn =>
          uploadPart(
            filePath,
            partNumber = pn,
            bytes = minPartBytes(pn.toByte)
          ).getStatus shouldEqual 200
        }

        // Upload final tail (can be small)
        val tail = Array.fill[Byte](tailBytes)((Random.nextInt(256) - 128).toByte)
        uploadPart(filePath, partNumber = numParts, bytes = tail).getStatus shouldEqual 200

        val resp = finishUpload(filePath)
        resp.getStatus shouldEqual 200
        fetchSession(filePath) shouldBe null
      }
  }
}
