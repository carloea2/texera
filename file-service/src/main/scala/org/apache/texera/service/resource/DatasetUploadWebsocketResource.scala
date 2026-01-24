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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.amber.util.JSONUtils
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSessionPart.DATASET_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.pojos.Dataset
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.service.ServletAwareConfigurator
import org.apache.texera.service.resource.DatasetAccessResource.userHasWriteAccess
import org.apache.texera.service.resource.DatasetResource.validateAndNormalizeFilePathOrThrow
import org.apache.texera.service.util.S3StorageClient.{
  MAXIMUM_NUM_OF_MULTIPART_S3_PARTS,
  MINIMUM_NUM_OF_MULTIPART_S3_PART
}
import org.jooq.impl.DSL
import org.jooq.impl.DSL.{inline => inl}

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.UUID
import jakarta.websocket.server.ServerEndpoint
import jakarta.websocket.{OnClose, OnMessage, OnOpen, Session}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import jakarta.ws.rs.{BadRequestException, ForbiddenException, WebApplicationException}
import jakarta.ws.rs.core.Response

object DatasetUploadWebsocketManager extends LazyLogging {
  private val objectMapper = JSONUtils.objectMapper
  private val context = SqlServer.getInstance().createDSLContext()
  private val wsIdField = DSL.field("ws_id", classOf[String])
  private val validUntilField = DSL.field("valid_until_ms", classOf[java.lang.Long])
  private val reservationTtlMs = 15000L
  private val retryAfterMs = 500L

  private val uploadIdSessions = new mutable.HashMap[String, mutable.Set[Session]]()
  private val sessionIdToUploadId = new mutable.HashMap[String, String]()
  private val sessionIdToWsId = new mutable.HashMap[String, String]()

  case class InitResponse(
      uploadId: String,
      partCount: Int,
      bytesCompleted: Long,
      percentage: Int,
      resumed: Boolean
  )

  def registerSession(session: Session, uploadId: String, wsId: String): Unit = synchronized {
    val set = uploadIdSessions.getOrElseUpdate(uploadId, mutable.Set.empty)
    set.add(session)
    sessionIdToUploadId(session.getId) = uploadId
    sessionIdToWsId(session.getId) = wsId
  }

  def wsIdForSession(session: Session): Option[String] = synchronized {
    sessionIdToWsId.get(session.getId)
  }

  def unregisterSession(session: Session): Unit = synchronized {
    val sessionId = session.getId
    sessionIdToUploadId.remove(sessionId).foreach { uploadId =>
      uploadIdSessions.get(uploadId).foreach { set =>
        set.remove(session)
        if (set.isEmpty) {
          uploadIdSessions.remove(uploadId)
        }
      }
    }
    sessionIdToWsId.remove(sessionId).foreach(releaseReservationsForWsId)
  }

  def releaseReservationsForWsId(wsId: String): Unit = {
    withTransaction(context) { ctx =>
      ctx
        .update(DATASET_UPLOAD_SESSION_PART)
        .set(wsIdField, null.asInstanceOf[String])
        .set(validUntilField, null.asInstanceOf[java.lang.Long])
        .where(
          wsIdField
            .eq(wsId)
            .and(DATASET_UPLOAD_SESSION_PART.ETAG.eq(""))
        )
        .execute()
    }
  }

  def reserveParts(uploadId: String, wsId: String, limit: Int): Either[Long, List[(Int, Long)]] = {
    if (limit <= 0) {
      return Left(retryAfterMs)
    }
    val nowMs = System.currentTimeMillis()
    val reserved = withTransaction(context) { ctx =>
      ctx
        .update(DATASET_UPLOAD_SESSION_PART)
        .set(wsIdField, null.asInstanceOf[String])
        .set(validUntilField, null.asInstanceOf[java.lang.Long])
        .where(
          DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
            .eq(uploadId)
            .and(DATASET_UPLOAD_SESSION_PART.ETAG.eq(""))
            .and(validUntilField.isNotNull)
            .and(validUntilField.lt(nowMs))
        )
        .execute()

      val available =
        ctx
          .select(DATASET_UPLOAD_SESSION_PART.PART_NUMBER)
          .from(DATASET_UPLOAD_SESSION_PART)
          .where(
            DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
              .eq(uploadId)
              .and(DATASET_UPLOAD_SESSION_PART.ETAG.eq(""))
              .and(wsIdField.isNull.or(validUntilField.lt(nowMs)))
          )
          .orderBy(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.asc())
          .limit(limit)
          .forUpdate()
          .skipLocked()
          .fetch(DATASET_UPLOAD_SESSION_PART.PART_NUMBER)
          .asScala
          .toList

      if (available.nonEmpty) {
        val validUntil = nowMs + reservationTtlMs
        ctx
          .update(DATASET_UPLOAD_SESSION_PART)
          .set(wsIdField, wsId)
          .set(validUntilField, java.lang.Long.valueOf(validUntil))
          .where(
            DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
              .eq(uploadId)
              .and(
                DATASET_UPLOAD_SESSION_PART.PART_NUMBER
                  .in(available.asJava)
              )
          )
          .execute()
        available.map(partNumber => (partNumber.intValue(), validUntil))
      } else {
        List.empty[(Int, Long)]
      }
    }

    if (reserved.isEmpty) {
      Left(retryAfterMs)
    } else {
      Right(reserved)
    }
  }

  def initOrResumeUpload(
      ownerEmail: String,
      datasetName: String,
      filePathRaw: String,
      fileSizeBytes: Long,
      partSizeBytes: Long,
      uid: Integer
  ): InitResponse = {
    val filePath = validateAndNormalizeFilePathOrThrow(
      URLDecoder.decode(filePathRaw, StandardCharsets.UTF_8.name())
    )
    withTransaction(context) { ctx =>
      val dataset = fetchDatasetBy(ctx, ownerEmail, datasetName)
      val did = dataset.getDid
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException("User has no access to this dataset")
      }

      val session =
        ctx
          .selectFrom(DATASET_UPLOAD_SESSION)
          .where(
            DATASET_UPLOAD_SESSION.UID
              .eq(uid)
              .and(DATASET_UPLOAD_SESSION.DID.eq(did))
              .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
          )
          .fetchOne()

      if (session != null) {
        val expectedFileSize = session.getFileSizeBytes
        val expectedPartSize = session.getPartSizeBytes
        if (expectedFileSize != fileSizeBytes || expectedPartSize != partSizeBytes) {
          throw new BadRequestException(
            s"Upload session does not match file metadata. " +
              s"Expected fileSizeBytes=$expectedFileSize partSizeBytes=$expectedPartSize"
          )
        }
        val progress = computeProgress(
          ctx,
          session.getUploadId,
          session.getNumPartsRequested,
          expectedFileSize,
          expectedPartSize
        )
        InitResponse(
          uploadId = session.getUploadId,
          partCount = session.getNumPartsRequested,
          bytesCompleted = progress._1,
          percentage = progress._2,
          resumed = true
        )
      } else {
        if (fileSizeBytes <= 0L) {
          throw new BadRequestException("fileSizeBytes must be > 0")
        }
        if (partSizeBytes <= 0L) {
          throw new BadRequestException("partSizeBytes must be > 0")
        }

        val totalMaxBytes: Long = DatasetResource.singleFileUploadMaxBytes(ctx)
        if (totalMaxBytes <= 0L) {
          throw new WebApplicationException(
            "singleFileUploadMaxBytes must be > 0",
            Response.Status.INTERNAL_SERVER_ERROR
          )
        }
        if (fileSizeBytes > totalMaxBytes) {
          throw new BadRequestException(
            s"fileSizeBytes=$fileSizeBytes exceeds singleFileUploadMaxBytes=$totalMaxBytes"
          )
        }

        val addend: Long = partSizeBytes - 1L
        if (addend < 0L || fileSizeBytes > Long.MaxValue - addend) {
          throw new WebApplicationException(
            "Overflow while computing numParts",
            Response.Status.INTERNAL_SERVER_ERROR
          )
        }

        val numPartsLong: Long = (fileSizeBytes + addend) / partSizeBytes
        if (numPartsLong < 1L || numPartsLong > MAXIMUM_NUM_OF_MULTIPART_S3_PARTS.toLong) {
          throw new BadRequestException(
            s"Computed numParts=$numPartsLong is out of range 1..$MAXIMUM_NUM_OF_MULTIPART_S3_PARTS"
          )
        }
        val numPartsValue: Int = numPartsLong.toInt

        if (numPartsValue > 1 && partSizeBytes < MINIMUM_NUM_OF_MULTIPART_S3_PART) {
          throw new BadRequestException(
            s"partSizeBytes=$partSizeBytes is too small. " +
              s"All non-final parts must be >= $MINIMUM_NUM_OF_MULTIPART_S3_PART bytes."
          )
        }

        val presign = LakeFSStorageClient.initiatePresignedMultipartUploads(
          dataset.getRepositoryName,
          filePath,
          numPartsValue
        )

        val uploadIdStr = presign.getUploadId
        val physicalAddr = presign.getPhysicalAddress

        try {
          val rowsInserted = ctx
            .insertInto(DATASET_UPLOAD_SESSION)
            .set(DATASET_UPLOAD_SESSION.FILE_PATH, filePath)
            .set(DATASET_UPLOAD_SESSION.DID, did)
            .set(DATASET_UPLOAD_SESSION.UID, uid)
            .set(DATASET_UPLOAD_SESSION.UPLOAD_ID, uploadIdStr)
            .set(DATASET_UPLOAD_SESSION.PHYSICAL_ADDRESS, physicalAddr)
            .set(DATASET_UPLOAD_SESSION.NUM_PARTS_REQUESTED, Integer.valueOf(numPartsValue))
            .set(DATASET_UPLOAD_SESSION.FILE_SIZE_BYTES, java.lang.Long.valueOf(fileSizeBytes))
            .set(DATASET_UPLOAD_SESSION.PART_SIZE_BYTES, java.lang.Long.valueOf(partSizeBytes))
            .onDuplicateKeyIgnore()
            .execute()

          if (rowsInserted != 1) {
            LakeFSStorageClient.abortPresignedMultipartUploads(
              dataset.getRepositoryName,
              filePath,
              uploadIdStr,
              physicalAddr
            )
            throw new WebApplicationException(
              "Upload already in progress for this filePath",
              Response.Status.CONFLICT
            )
          }

          val partNumberSeries = DSL.generateSeries(1, numPartsValue).asTable("gs", "pn")
          val partNumberField = partNumberSeries.field("pn", classOf[Integer])

          ctx
            .insertInto(
              DATASET_UPLOAD_SESSION_PART,
              DATASET_UPLOAD_SESSION_PART.UPLOAD_ID,
              DATASET_UPLOAD_SESSION_PART.PART_NUMBER,
              DATASET_UPLOAD_SESSION_PART.ETAG
            )
            .select(
              ctx
                .select(
                  inl(uploadIdStr),
                  partNumberField,
                  inl("")
                )
                .from(partNumberSeries)
            )
            .execute()

          InitResponse(
            uploadId = uploadIdStr,
            partCount = numPartsValue,
            bytesCompleted = 0L,
            percentage = 0,
            resumed = false
          )
        } catch {
          case e: Exception =>
            try {
              LakeFSStorageClient.abortPresignedMultipartUploads(
                dataset.getRepositoryName,
                filePath,
                uploadIdStr,
                physicalAddr
              )
            } catch { case _: Throwable => () }
            throw e
        }
      }
    }
  }

  def notifyPartUploaded(uploadId: String, partNumber: Int): Unit = {
    withTransaction(context) { ctx =>
      val session = ctx
        .selectFrom(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(uploadId))
        .fetchOne()
      if (session != null) {
        val (bytesCompleted, percentage) = computeProgress(
          ctx,
          uploadId,
          session.getNumPartsRequested,
          session.getFileSizeBytes,
          session.getPartSizeBytes
        )
        broadcastUploadedParts(uploadId, List(partNumber), bytesCompleted, percentage)
      }
    }
  }

  def broadcastUploadedParts(
      uploadId: String,
      partsUploaded: Seq[Int],
      bytesCompleted: Long,
      percentage: Int
  ): Unit = synchronized {
    val data = objectMapper.createObjectNode()
    val partsArray = objectMapper.createArrayNode()
    partsUploaded.foreach(partsArray.add)
    data.set("partsUploaded", partsArray)
    data.put("bytesCompleted", bytesCompleted)
    data.put("percentage", percentage)
    broadcast(uploadId, "uploaded_parts", data, Some(uploadId), None)
  }

  def broadcastGoodbye(
      uploadId: String,
      reason: String,
      bytesCompleted: Option[Long],
      percentage: Option[Int]
  ): Unit = synchronized {
    val data = objectMapper.createObjectNode()
    data.put("reason", reason)
    bytesCompleted.foreach(data.put("bytesCompleted", _))
    percentage.foreach(data.put("percentage", _))
    broadcast(uploadId, "goodbye", data, Some(uploadId), None, closeAfter = true)
  }

  private def broadcast(
      uploadId: String,
      messageType: String,
      data: ObjectNode,
      uploadIdOpt: Option[String],
      wsIdOpt: Option[String],
      closeAfter: Boolean = false
  ): Unit = {
    uploadIdSessions.get(uploadId).foreach { sessions =>
      sessions.foreach { session =>
        val sessionWsId = sessionIdToWsId.get(session.getId).orElse(wsIdOpt)
        val payload = buildEnvelope(messageType, uploadIdOpt, sessionWsId, data)
        try {
          session.getBasicRemote.sendText(payload)
          if (closeAfter) {
            session.close()
          }
        } catch {
          case e: Exception =>
            logger.warn("Failed sending dataset upload websocket message", e)
        }
      }
    }
  }

  def sendError(session: Session, code: String, message: String): Unit = {
    val data = objectMapper.createObjectNode()
    data.put("code", code)
    data.put("message", message)
    send(session, "error", data, None, None)
  }

  def sendNoParts(session: Session, uploadId: String, wsId: String): Unit = {
    val data = objectMapper.createObjectNode()
    data.put("retryAfterMs", retryAfterMs)
    send(session, "no_parts", data, Some(uploadId), Some(wsId))
  }

  def sendParts(
      session: Session,
      uploadId: String,
      wsId: String,
      parts: List[(Int, Long)]
  ): Unit = {
    val data = objectMapper.createObjectNode()
    val partsArray = objectMapper.createArrayNode()
    parts.foreach { case (partNumber, validUntil) =>
      val partNode = objectMapper.createObjectNode()
      partNode.put("partNumber", partNumber)
      partNode.put("validUntil", validUntil)
      partsArray.add(partNode)
    }
    data.set("parts", partsArray)
    data.put("granted", parts.length)
    send(session, "parts", data, Some(uploadId), Some(wsId))
  }

  def sendInitAck(
      session: Session,
      uploadId: String,
      wsId: String,
      init: InitResponse,
      concurrencyAccepted: Int
  ): Unit = {
    val data = objectMapper.createObjectNode()
    data.put("resumed", init.resumed)
    data.put("partCount", init.partCount)
    data.put("bytesCompleted", init.bytesCompleted)
    data.put("percentage", init.percentage)
    data.put("concurrencyAccepted", concurrencyAccepted)
    send(session, "init_ack", data, Some(uploadId), Some(wsId))
  }

  def sendGoodbye(
      session: Session,
      uploadId: String,
      wsId: String,
      reason: String,
      bytesCompleted: Option[Long],
      percentage: Option[Int]
  ): Unit = {
    val data = objectMapper.createObjectNode()
    data.put("reason", reason)
    bytesCompleted.foreach(data.put("bytesCompleted", _))
    percentage.foreach(data.put("percentage", _))
    send(session, "goodbye", data, Some(uploadId), Some(wsId))
  }

  private def send(
      session: Session,
      messageType: String,
      data: ObjectNode,
      uploadId: Option[String],
      wsId: Option[String]
  ): Unit = {
    session.getBasicRemote.sendText(buildEnvelope(messageType, uploadId, wsId, data))
  }

  private def buildEnvelope(
      messageType: String,
      uploadId: Option[String],
      wsId: Option[String],
      data: ObjectNode
  ): String = {
    val root = objectMapper.createObjectNode()
    root.put("type", messageType)
    root.put("v", 1)
    root.put("ts", System.currentTimeMillis())
    uploadId.foreach(root.put("uploadId", _))
    wsId.foreach(root.put("wsId", _))
    root.set("data", data)
    objectMapper.writeValueAsString(root)
  }

  private def fetchDatasetBy(ctx: org.jooq.DSLContext, ownerEmail: String, datasetName: String): Dataset = {
    val dataset = ctx
      .select(DATASET.fields: _*)
      .from(DATASET)
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))
      .where(USER.EMAIL.eq(ownerEmail))
      .and(DATASET.NAME.eq(datasetName))
      .fetchOneInto(classOf[Dataset])
    if (dataset == null) {
      throw new BadRequestException("Dataset not found")
    }
    dataset
  }

  private def computeProgress(
      ctx: org.jooq.DSLContext,
      uploadId: String,
      numParts: Int,
      fileSizeBytes: Long,
      partSizeBytes: Long
  ): (Long, Int) = {
    val uploadedCount = ctx
      .selectCount()
      .from(DATASET_UPLOAD_SESSION_PART)
      .where(
        DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
          .eq(uploadId)
          .and(DATASET_UPLOAD_SESSION_PART.ETAG.ne(""))
      )
      .fetchOne(0, classOf[Int])
    val lastPartUploaded = ctx.fetchExists(
      ctx
        .selectOne()
        .from(DATASET_UPLOAD_SESSION_PART)
        .where(
          DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
            .eq(uploadId)
            .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(numParts))
            .and(DATASET_UPLOAD_SESSION_PART.ETAG.ne(""))
        )
    )
    val lastPartSize = fileSizeBytes - partSizeBytes * (numParts - 1L)
    val bytesCompleted =
      if (uploadedCount <= 0) {
        0L
      } else {
        val base = (uploadedCount - (if (lastPartUploaded) 1 else 0)) * partSizeBytes
        val last = if (lastPartUploaded) lastPartSize else 0L
        base + last
      }
    val percentage =
      if (fileSizeBytes <= 0L) {
        0
      } else {
        Math.min(100, Math.round(bytesCompleted.toDouble * 100.0 / fileSizeBytes).toInt)
      }
    (bytesCompleted, percentage)
  }
}

@ServerEndpoint(
  value = "/wsapi/dataset-upload",
  configurator = classOf[ServletAwareConfigurator]
)
class DatasetUploadWebsocketResource extends LazyLogging {
  private val objectMapper = JSONUtils.objectMapper

  @OnOpen
  def onOpen(session: Session): Unit = {
    logger.debug(s"Dataset upload websocket opened: ${session.getId}")
  }

  @OnClose
  def onClose(session: Session): Unit = {
    DatasetUploadWebsocketManager.unregisterSession(session)
    logger.debug(s"Dataset upload websocket closed: ${session.getId}")
  }

  @OnMessage
  def onMessage(session: Session, message: String): Unit = {
    try {
      val root = objectMapper.readTree(message)
      val messageType = root.path("type").asText("")
      messageType match {
        case "init" =>
          handleInit(session, root)
        case "request_parts" =>
          handleRequestParts(session, root)
        case "abort" =>
          handleAbort(session, root)
        case "pong" =>
          ()
        case other =>
          DatasetUploadWebsocketManager.sendError(session, "INVALID_TYPE", s"Unknown type: $other")
      }
    } catch {
      case e: Exception =>
        DatasetUploadWebsocketManager.sendError(session, "BAD_REQUEST", e.getMessage)
    }
  }

  private def handleInit(session: Session, root: JsonNode): Unit = {
    val data = root.path("data")
    val ownerEmail = requiredText(data, "ownerEmail")
    val datasetName = requiredText(data, "datasetName")
    val filePath = requiredText(data, "filePath")
    val fileSizeBytes = requiredLong(data, "fileSizeBytes")
    val partSizeBytes = requiredLong(data, "partSizeBytes")
    val concurrency = requiredInt(data, "concurrency")

    val user = getUser(session)
    if (user == null) {
      DatasetUploadWebsocketManager.sendError(session, "UNAUTHORIZED", "Missing user")
      return
    }

    val init = DatasetUploadWebsocketManager.initOrResumeUpload(
      ownerEmail,
      datasetName,
      filePath,
      fileSizeBytes,
      partSizeBytes,
      user.getUid
    )
    val wsId = UUID.randomUUID().toString
    DatasetUploadWebsocketManager.registerSession(session, init.uploadId, wsId)
    DatasetUploadWebsocketManager.sendInitAck(session, init.uploadId, wsId, init, concurrency)
  }

  private def handleRequestParts(session: Session, root: JsonNode): Unit = {
    val uploadId = requiredEnvelopeText(root, "uploadId")
    val wsId = requiredEnvelopeText(root, "wsId")
    val data = root.path("data")
    val limit = requiredInt(data, "limit")
    if (DatasetUploadWebsocketManager.wsIdForSession(session).exists(_ != wsId)) {
      DatasetUploadWebsocketManager.sendError(session, "UNAUTHORIZED", "wsId mismatch")
      return
    }

    DatasetUploadWebsocketManager.reserveParts(uploadId, wsId, limit) match {
      case Right(parts) =>
        DatasetUploadWebsocketManager.sendParts(session, uploadId, wsId, parts)
      case Left(_) =>
        DatasetUploadWebsocketManager.sendNoParts(session, uploadId, wsId)
    }
  }

  private def handleAbort(session: Session, root: JsonNode): Unit = {
    val uploadId = requiredEnvelopeText(root, "uploadId")
    val wsId = requiredEnvelopeText(root, "wsId")
    if (DatasetUploadWebsocketManager.wsIdForSession(session).exists(_ != wsId)) {
      DatasetUploadWebsocketManager.sendError(session, "UNAUTHORIZED", "wsId mismatch")
      return
    }
    val user = getUser(session)
    if (user == null) {
      DatasetUploadWebsocketManager.sendError(session, "UNAUTHORIZED", "Missing user")
      return
    }
    withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
      val sessionRow = ctx
        .selectFrom(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(uploadId))
        .fetchOne()
      if (sessionRow == null) {
        DatasetUploadWebsocketManager.sendGoodbye(
          session,
          uploadId,
          wsId,
          "aborted",
          None,
          None
        )
        session.close()
        return
      }
      if (sessionRow.getUid != user.getUid) {
        throw new ForbiddenException("User has no access to this upload session")
      }
      val dataset =
        ctx
          .select(DATASET.fields: _*)
          .from(DATASET)
          .where(DATASET.DID.eq(sessionRow.getDid))
          .fetchOneInto(classOf[Dataset])
      val filePath = sessionRow.getFilePath
      val physicalAddr = Option(sessionRow.getPhysicalAddress).map(_.trim).getOrElse("")
      if (dataset != null && physicalAddr.nonEmpty) {
        LakeFSStorageClient.abortPresignedMultipartUploads(
          dataset.getRepositoryName,
          filePath,
          sessionRow.getUploadId,
          physicalAddr
        )
      }
      ctx
        .deleteFrom(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(uploadId))
        .execute()
    }
    DatasetUploadWebsocketManager.sendGoodbye(
      session,
      uploadId,
      wsId,
      "aborted",
      None,
      None
    )
    session.close()
  }

  private def getUser(session: Session): User = {
    session.getUserProperties.asScala
      .get(classOf[User].getName)
      .map(_.asInstanceOf[User])
      .orNull
  }

  private def requiredText(node: JsonNode, field: String): String = {
    val value = node.path(field).asText("").trim
    if (value.isEmpty) {
      throw new BadRequestException(s"$field is required")
    }
    value
  }

  private def requiredEnvelopeText(node: JsonNode, field: String): String = {
    val value = node.path(field).asText("").trim
    if (value.isEmpty) {
      throw new BadRequestException(s"$field is required")
    }
    value
  }

  private def requiredLong(node: JsonNode, field: String): Long = {
    if (!node.hasNonNull(field)) {
      throw new BadRequestException(s"$field is required")
    }
    node.get(field).asLong()
  }

  private def requiredInt(node: JsonNode, field: String): Int = {
    if (!node.hasNonNull(field)) {
      throw new BadRequestException(s"$field is required")
    }
    node.get(field).asInt()
  }
}
