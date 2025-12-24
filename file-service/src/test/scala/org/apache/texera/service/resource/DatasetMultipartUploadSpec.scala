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

import io.lakefs.clients.sdk.ApiException
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSessionPart.DATASET_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import org.apache.texera.service.MockLakeFS
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Tag}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.concurrent.CyclicBarrier
import java.util.{Collections, Date, Locale, Optional}
import scala.util.Random

object StressMultipart extends Tag("org.apache.texera.stress.multipart")

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

  // REGULAR user, but no WRITE access to someone else's dataset.
  private val testUser2: User = {
    val u = new User
    u.setName("multipart_user2")
    u.setPassword("123")
    u.setEmail("multipart_user2@test.com")
    u.setRole(UserRoleEnum.REGULAR)
    u
  }

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
    // If it already exists, ignore 409.
    try LakeFSStorageClient.initRepo(testDataset.getRepositoryName)
    catch {
      case e: ApiException if e.getCode == 409 => // ok
    }
  }

  // ---------- helpers ----------
  private def enc(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.name())

  /** Minimum part-size rule (S3-style): every part except the LAST must be >= 5 MiB. */
  private val MinNonFinalPartBytes: Int = 5 * 1024 * 1024
  private def minPartBytes(b: Byte): Array[Byte] =
    Array.fill[Byte](MinNonFinalPartBytes)(b)

  private def tinyBytes(b: Byte, n: Int = 1): Array[Byte] =
    Array.fill[Byte](n)(b)

  /** Minimal HttpHeaders impl needed by DatasetResource.uploadPart */
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

  private def uploadPartWithStream(
      filePath: String,
      partNumber: Int,
      stream: InputStream,
      contentLength: Long,
      user: SessionUser = sessionUser
  ): Response =
    datasetResource.uploadPart(
      testUser.getEmail,
      testDataset.getName,
      enc(filePath),
      partNumber,
      stream,
      mkHeaders(contentLength),
      user
    )

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

  private def fetchPartRows(uploadId: String) =
    getDSLContext
      .selectFrom(DATASET_UPLOAD_SESSION_PART)
      .where(DATASET_UPLOAD_SESSION_PART.UPLOAD_ID.eq(uploadId))
      .fetch()
      .asScala
      .toList

  private def fetchUploadIdOrFail(filePath: String): String = {
    val s = fetchSession(filePath)
    s should not be null
    s.getUploadId
  }

  private def assertPlaceholdersCreated(uploadId: String, expectedParts: Int): Unit = {
    val rows = fetchPartRows(uploadId).sortBy(_.getPartNumber)
    rows.size shouldEqual expectedParts
    rows.head.getPartNumber shouldEqual 1
    rows.last.getPartNumber shouldEqual expectedParts
    rows.foreach { r =>
      r.getEtag should not be null
      r.getEtag shouldEqual "" // placeholder convention
    }
  }

  private def assertStatus(ex: WebApplicationException, status: Int): Unit =
    ex.getResponse.getStatus shouldEqual status

  // ---------------------------------------------------------------------------
  // INIT TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload?type=init" should "create an upload session row + precreate part placeholders (happy path)" in {
    val filePath = uniqueFilePath("init-happy")
    val resp = initUpload(filePath, numParts = 3)

    resp.getStatus shouldEqual 200

    val s = fetchSession(filePath)
    s should not be null
    s.getNumPartsRequested shouldEqual 3
    s.getUploadId should not be null
    s.getPhysicalAddress should not be null

    assertPlaceholdersCreated(s.getUploadId, expectedParts = 3)
  }

  it should "reject missing numParts" in {
    val filePath = uniqueFilePath("init-missing-numparts")
    val ex = intercept[BadRequestException] {
      datasetResource.multipartUpload(
        "init",
        testUser.getEmail,
        testDataset.getName,
        enc(filePath),
        Optional.empty(),
        sessionUser
      )
    }
    assertStatus(ex, 400)
  }

  it should "reject invalid numParts (0, negative, too large)" in {
    val filePath = uniqueFilePath("init-bad-numparts")
    assertStatus(intercept[BadRequestException] { initUpload(filePath, 0) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload(filePath, -1) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload(filePath, 1000000000) }, 400)
  }

  it should "reject invalid filePath (empty, absolute, '.', '..', control chars)" in {
    assertStatus(intercept[BadRequestException] { initUpload("./nope.bin", 2) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload("/absolute.bin", 2) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload("a/./b.bin", 2) }, 400)

    // traversal-like '..'
    assertStatus(intercept[BadRequestException] { initUpload("../escape.bin", 2) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload("a/../escape.bin", 2) }, 400)

    // control char (0x00)
    assertStatus(
      intercept[BadRequestException] {
        initUpload(s"a/${0.toChar}b.bin", 2)
      },
      400
    )
  }

  it should "reject invalid type parameter" in {
    val filePath = uniqueFilePath("init-bad-type")
    val ex = intercept[BadRequestException] {
      datasetResource.multipartUpload(
        "not-a-real-type",
        testUser.getEmail,
        testDataset.getName,
        enc(filePath),
        Optional.empty(),
        sessionUser
      )
    }
    assertStatus(ex, 400)
  }

  it should "reject init when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("init-forbidden")
    val ex = intercept[ForbiddenException] {
      initUpload(filePath, numParts = 2, user = sessionUser2)
    }
    assertStatus(ex, 403)
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
      case e: WebApplicationException => assertStatus(e, 409)
      case other =>
        fail(
          s"Expected WebApplicationException(CONFLICT), got: ${other.getClass} / ${other.getMessage}"
        )
    }

    val s = fetchSession(filePath)
    s should not be null
    assertPlaceholdersCreated(s.getUploadId, expectedParts = 2)
  }

  it should "reject sequential double init with 409 CONFLICT" in {
    val filePath = uniqueFilePath("init-double")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

    val ex = intercept[WebApplicationException] { initUpload(filePath, numParts = 2) }
    assertStatus(ex, 409)
  }

  // ---------------------------------------------------------------------------
  // PART UPLOAD TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload/part" should "reject uploadPart if init was not called" in {
    val filePath = uniqueFilePath("part-no-init")
    val ex = intercept[NotFoundException] {
      uploadPart(filePath, partNumber = 1, bytes = Array[Byte](1, 2, 3))
    }
    assertStatus(ex, 404)
  }

  it should "reject missing/invalid Content-Length" in {
    val filePath = uniqueFilePath("part-bad-cl")
    initUpload(filePath, numParts = 2)

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(
          filePath,
          partNumber = 1,
          bytes = Array[Byte](1, 2, 3),
          missingContentLength = true
        )
      },
      400
    )

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(
          filePath,
          partNumber = 1,
          bytes = Array[Byte](1, 2, 3),
          contentLengthOverride = Some(0L)
        )
      },
      400
    )

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(
          filePath,
          partNumber = 1,
          bytes = Array[Byte](1, 2, 3),
          contentLengthOverride = Some(-5L)
        )
      },
      400
    )
  }

  it should "reject null/empty filePath param early without depending on error text" in {
    val hdrs = mkHeaders(1L)

    val ex1 = intercept[BadRequestException] {
      datasetResource.uploadPart(
        testUser.getEmail,
        testDataset.getName,
        null, // encodedFilePath null
        1,
        new ByteArrayInputStream(Array.emptyByteArray),
        hdrs,
        sessionUser
      )
    }
    assertStatus(ex1, 400)

    val ex2 = intercept[BadRequestException] {
      datasetResource.uploadPart(
        testUser.getEmail,
        testDataset.getName,
        "", // empty
        1,
        new ByteArrayInputStream(Array.emptyByteArray),
        hdrs,
        sessionUser
      )
    }
    assertStatus(ex2, 400)
  }

  it should "reject invalid partNumber (< 1) and partNumber > requested" in {
    val filePath = uniqueFilePath("part-bad-pn")
    initUpload(filePath, numParts = 2)

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(filePath, partNumber = 0, bytes = tinyBytes(1.toByte))
      },
      400
    )

    // Ensure we don't fail min-size check before we hit range validation.
    assertStatus(
      intercept[BadRequestException] {
        uploadPart(filePath, partNumber = 3, bytes = minPartBytes(2.toByte))
      },
      400
    )
  }

  it should "reject a non-final part smaller than the minimum size (without checking message)" in {
    val filePath = uniqueFilePath("part-too-small-nonfinal")
    initUpload(filePath, numParts = 2) // part 1 is NON-FINAL

    val ex = intercept[BadRequestException] {
      uploadPart(filePath, partNumber = 1, bytes = tinyBytes(1.toByte))
    }
    assertStatus(ex, 400)

    // DB should remain unchanged (etag still empty)
    val uploadId = fetchUploadIdOrFail(filePath)
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""
  }

  it should "upload a part successfully and persist its ETag into DATASET_UPLOAD_SESSION_PART" in {
    val filePath = uniqueFilePath("part-happy-db")
    initUpload(filePath, numParts = 2)

    val uploadId = fetchUploadIdOrFail(filePath)

    // Before upload: placeholder etag empty
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""

    val bytes = minPartBytes(7.toByte)
    uploadPart(filePath, partNumber = 1, bytes = bytes).getStatus shouldEqual 200

    val after = fetchPartRows(uploadId).find(_.getPartNumber == 1).get
    after.getEtag should not equal ""
  }

  it should "allow retrying the same part sequentially (no duplicates, etag ends non-empty)" in {
    val filePath = uniqueFilePath("part-retry")
    initUpload(filePath, numParts = 2)
    val uploadId = fetchUploadIdOrFail(filePath)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 1, minPartBytes(2.toByte)).getStatus shouldEqual 200

    val rows = fetchPartRows(uploadId).filter(_.getPartNumber == 1)
    rows.size shouldEqual 1
    rows.head.getEtag should not equal ""
  }

  it should "apply per-part locking: return 409 if that part row is locked by another uploader" in {
    val filePath = uniqueFilePath("part-lock")
    initUpload(filePath, numParts = 2)
    val uploadId = fetchUploadIdOrFail(filePath)

    val cp = getDSLContext.configuration().connectionProvider()
    val conn = cp.acquire()
    conn.setAutoCommit(false)

    try {
      val locking = DSL.using(conn, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION_PART)
        .where(
          DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
            .eq(uploadId)
            .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(1))
        )
        .forUpdate()
        .fetchOne()

      val ex = intercept[WebApplicationException] {
        uploadPart(filePath, 1, minPartBytes(1.toByte))
      }
      assertStatus(ex, 409)
    } finally {
      conn.rollback()
      cp.release(conn)
    }

    // After releasing lock, upload should succeed
    uploadPart(filePath, 1, minPartBytes(3.toByte)).getStatus shouldEqual 200
  }

  it should "not block other parts: locking part 1 does not prevent uploading part 2" in {
    val filePath = uniqueFilePath("part-lock-other-part")
    initUpload(filePath, numParts = 2)
    val uploadId = fetchUploadIdOrFail(filePath)

    val cp = getDSLContext.configuration().connectionProvider()
    val conn = cp.acquire()
    conn.setAutoCommit(false)

    try {
      val locking = DSL.using(conn, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION_PART)
        .where(
          DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
            .eq(uploadId)
            .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(1))
        )
        .forUpdate()
        .fetchOne()

      // part 2 is FINAL, can be tiny
      uploadPart(filePath, 2, tinyBytes(9.toByte)).getStatus shouldEqual 200
    } finally {
      conn.rollback()
      cp.release(conn)
    }
  }

  it should "reject uploadPart when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("part-forbidden")
    initUpload(filePath, numParts = 2)

    val ex = intercept[ForbiddenException] {
      uploadPart(filePath, 1, minPartBytes(1.toByte), user = sessionUser2)
    }
    assertStatus(ex, 403)
  }

  // ---------------------------------------------------------------------------
  // FINISH TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload?type=finish" should "reject finish if init was not called" in {
    val filePath = uniqueFilePath("finish-no-init")
    val ex = intercept[NotFoundException] { finishUpload(filePath) }
    assertStatus(ex, 404)
  }

  it should "reject finish when no parts were uploaded (all placeholders empty) without checking messages" in {
    val filePath = uniqueFilePath("finish-no-parts")
    initUpload(filePath, numParts = 2)

    val ex = intercept[WebApplicationException] { finishUpload(filePath) }
    assertStatus(ex, 409)

    // session remains
    fetchSession(filePath) should not be null
  }

  it should "reject finish when some parts are missing (etag empty treated as missing)" in {
    val filePath = uniqueFilePath("finish-missing")
    initUpload(filePath, numParts = 3)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200

    val ex = intercept[WebApplicationException] { finishUpload(filePath) }
    assertStatus(ex, 409)

    val uploadId = fetchUploadIdOrFail(filePath)
    fetchPartRows(uploadId).find(_.getPartNumber == 2).get.getEtag shouldEqual ""
    fetchPartRows(uploadId).find(_.getPartNumber == 3).get.getEtag shouldEqual ""
  }

  it should "reject finish when extra part rows exist in DB (bypass endpoint) without checking messages" in {
    val filePath = uniqueFilePath("finish-extra-db")
    initUpload(filePath, numParts = 2)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(2.toByte)).getStatus shouldEqual 200

    val s = fetchSession(filePath)
    val uploadId = s.getUploadId

    // Bypass: insert extra row partNumber=3
    getDSLContext
      .insertInto(DATASET_UPLOAD_SESSION_PART)
      .set(DATASET_UPLOAD_SESSION_PART.UPLOAD_ID, uploadId)
      .set(DATASET_UPLOAD_SESSION_PART.PART_NUMBER, Integer.valueOf(3))
      .set(DATASET_UPLOAD_SESSION_PART.ETAG, "bogus-etag")
      .execute()

    val ex = intercept[WebApplicationException] { finishUpload(filePath) }
    assertStatus(ex, 500)

    // Ensure nothing got deleted
    fetchSession(filePath) should not be null
    fetchPartRows(uploadId).nonEmpty shouldEqual true
  }

  it should "finish successfully when all parts have non-empty etags; delete session + part rows" in {
    val filePath = uniqueFilePath("finish-happy")
    initUpload(filePath, numParts = 3)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, minPartBytes(2.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 3, tinyBytes(3.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    val resp = finishUpload(filePath)
    resp.getStatus shouldEqual 200

    fetchSession(filePath) shouldBe null
    fetchPartRows(uploadId) shouldBe empty
  }

  it should "be idempotent-ish: second finish should return NotFound after successful finish" in {
    val filePath = uniqueFilePath("finish-twice")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

    finishUpload(filePath).getStatus shouldEqual 200

    val ex = intercept[NotFoundException] { finishUpload(filePath) }
    assertStatus(ex, 404)
  }

  it should "reject finish when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("finish-forbidden")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

    val ex = intercept[ForbiddenException] { finishUpload(filePath, user = sessionUser2) }
    assertStatus(ex, 403)
  }

  it should "return 409 CONFLICT if the session row is locked by another finalizer/aborter" in {
    val filePath = uniqueFilePath("finish-lock-race")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

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

      val ex = intercept[WebApplicationException] { finishUpload(filePath) }
      assertStatus(ex, 409)
    } finally {
      conn.rollback()
      cp.release(conn)
    }
  }

  // ---------------------------------------------------------------------------
  // ABORT TESTS
  // ---------------------------------------------------------------------------

  "multipart-upload?type=abort" should "reject abort if init was not called" in {
    val filePath = uniqueFilePath("abort-no-init")
    val ex = intercept[NotFoundException] { abortUpload(filePath) }
    assertStatus(ex, 404)
  }

  it should "abort successfully; delete session + part rows" in {
    val filePath = uniqueFilePath("abort-happy")
    initUpload(filePath, numParts = 2)
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    abortUpload(filePath).getStatus shouldEqual 200

    fetchSession(filePath) shouldBe null
    fetchPartRows(uploadId) shouldBe empty
  }

  it should "reject abort when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("abort-forbidden")
    initUpload(filePath, numParts = 1)

    val ex = intercept[ForbiddenException] { abortUpload(filePath, user = sessionUser2) }
    assertStatus(ex, 403)
  }

  it should "return 409 CONFLICT if the session row is locked by another finalizer/aborter" in {
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

      val ex = intercept[WebApplicationException] { abortUpload(filePath) }
      assertStatus(ex, 409)
    } finally {
      conn.rollback()
      cp.release(conn)
    }
  }

  it should "be consistent: abort after finish should return NotFound" in {
    val filePath = uniqueFilePath("abort-after-finish")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

    finishUpload(filePath).getStatus shouldEqual 200

    val ex = intercept[NotFoundException] { abortUpload(filePath) }
    assertStatus(ex, 404)
  }

  // ---------------------------------------------------------------------------
  // FAILURE / RESILIENCE (still unit tests; simulated failures)
  // ---------------------------------------------------------------------------

  "multipart upload implementation" should "release locks and keep DB consistent if the incoming stream fails mid-upload (simulated network drop)" in {
    val filePath = uniqueFilePath("netfail-upload-stream")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200
    val uploadId = fetchUploadIdOrFail(filePath)

    val payload = minPartBytes(5.toByte)

    // InputStream that throws after a few reads
    val flaky = new InputStream {
      private var pos = 0
      override def read(): Int = {
        if (pos >= 1024) throw new IOException("simulated network drop")
        val b = payload(pos) & 0xff
        pos += 1
        b
      }
    }

    intercept[Throwable] {
      uploadPartWithStream(
        filePath,
        partNumber = 1,
        stream = flaky,
        contentLength = payload.length.toLong
      )
    }

    // ETag should still be empty (no partial DB commit)
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""

    // And the lock must be released (retry should succeed)
    uploadPart(filePath, 1, payload).getStatus shouldEqual 200
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag should not equal ""
  }

  it should "not delete session/parts if finalize fails downstream (simulate by corrupting an ETag)" in {
    val filePath = uniqueFilePath("netfail-finish")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(2.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    // Corrupt one ETag to force backend finalize failure (S3/LakeFS should reject).
    getDSLContext
      .update(DATASET_UPLOAD_SESSION_PART)
      .set(DATASET_UPLOAD_SESSION_PART.ETAG, "definitely-not-a-real-etag")
      .where(
        DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
          .eq(uploadId)
          .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(1))
      )
      .execute()

    intercept[Throwable] { finishUpload(filePath) }

    // Nothing should be deleted on failure
    fetchSession(filePath) should not be null
    fetchPartRows(uploadId).nonEmpty shouldEqual true
  }

  // ---------------------------------------------------------------------------
  // STRESS / SOAK TESTS (tagged;)
  // ---------------------------------------------------------------------------

  it should "survive 2 concurrent multipart uploads (fan-out)" taggedAs (StressMultipart, Slow) in {
    val parallelUploads = 2
    val maxParts = 3

    def oneUpload(i: Int): Future[Unit] =
      Future {
        val filePath = uniqueFilePath(s"stress-$i")
        val numParts = 2 + Random.nextInt(maxParts - 1)

        initUpload(filePath, numParts).getStatus shouldEqual 200

        // Upload parts concurrently (different parts, so no per-part conflicts expected)
        val sharedMin = minPartBytes((i % 127).toByte)
        val partFuts = (1 to numParts).map { pn =>
          Future {
            val bytes =
              if (pn < numParts) sharedMin
              else tinyBytes((pn % 127).toByte, n = 1024) // final tail, 1KiB
            uploadPart(filePath, pn, bytes).getStatus shouldEqual 200
          }
        }

        Await.result(Future.sequence(partFuts), 60.seconds)

        finishUpload(filePath).getStatus shouldEqual 200
        fetchSession(filePath) shouldBe null
      }

    val all = Future.sequence((1 to parallelUploads).map(oneUpload))
    Await.result(all, 180.seconds)
  }

  it should "throttle concurrent uploads of the SAME part via per-part locks" taggedAs (StressMultipart, Slow) in {
    val filePath = uniqueFilePath("stress-same-part")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

    val contenders = 4
    val barrier = new CyclicBarrier(contenders)

    def tryUploadStatus(): Future[Int] =
      Future {
        barrier.await()
        try {
          uploadPart(filePath, 1, minPartBytes(7.toByte)).getStatus
        } catch {
          case e: WebApplicationException => e.getResponse.getStatus
        }
      }

    val statuses =
      Await.result(Future.sequence((1 to contenders).map(_ => tryUploadStatus())), 60.seconds)

    statuses.foreach { s => s should (be(200) or be(409)) }
    statuses.count(_ == 200) should be >= 1

    val uploadId = fetchUploadIdOrFail(filePath)
    val part1 = fetchPartRows(uploadId).find(_.getPartNumber == 1).get
    part1.getEtag.trim should not be ""
  }

}
