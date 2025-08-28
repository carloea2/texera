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

package edu.uci.ics.texera.service.resource

import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.amber.core.storage.model.OnDataset
import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient
import edu.uci.ics.amber.core.storage.{DocumentFactory, FileResolver}
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.Model.MODEL
import edu.uci.ics.texera.dao.jooq.generated.tables.ModelUserAccess.MODEL_USER_ACCESS
import edu.uci.ics.texera.dao.jooq.generated.tables.ModelVersion.MODEL_VERSION
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  ModelDao,
  ModelUserAccessDao,
  ModelVersionDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Model, ModelUserAccess, ModelVersion}
import edu.uci.ics.texera.service.`type`.DatasetFileNode
import edu.uci.ics.texera.service.resource.ModelAccessResource._
import edu.uci.ics.texera.service.resource.ModelResource.{context, _}
import edu.uci.ics.texera.service.util.S3StorageClient
import edu.uci.ics.texera.service.util.S3StorageClient.{
  MAXIMUM_NUM_OF_MULTIPART_S3_PARTS,
  MINIMUM_NUM_OF_MULTIPART_S3_PART
}
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core._
import org.jooq.{DSLContext, EnumType}

import java.io.{InputStream, OutputStream}
import java.net.{HttpURLConnection, URL, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.Optional
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

object ModelResource {

  private val context = SqlServer
    .getInstance()
    .createDSLContext()

  /**
    * Helper function to get the model from DB using mid
    */
  private def getModelByID(ctx: DSLContext, mid: Integer): Model = {
    val modelDao = new ModelDao(ctx.configuration())
    val model = modelDao.fetchOneByMid(mid)
    if (model == null) {
      throw new NotFoundException(f"Model $mid not found")
    }
    model
  }

  /**
    * Helper function to PUT exactly len bytes from buf to presigned URL, return the ETag
    */
  private def put(buf: Array[Byte], len: Int, url: String, partNum: Int): String = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT")
    conn.setFixedLengthStreamingMode(len)
    val out = conn.getOutputStream
    out.write(buf, 0, len);
    out.close()

    val code = conn.getResponseCode
    if (code != HttpURLConnection.HTTP_OK && code != HttpURLConnection.HTTP_CREATED)
      throw new RuntimeException(s"Part $partNum upload failed (HTTP $code)")

    val etag = conn.getHeaderField("ETag").replace("\"", "")
    conn.disconnect()
    etag
  }

  /**
    * Helper function to get the model version from DB using dvid
    */
  private def getModelVersionByID(
      ctx: DSLContext,
      mvid: Integer
  ): ModelVersion = {
    val modelVersionDao = new ModelVersionDao(ctx.configuration())
    val version = modelVersionDao.fetchOneByMvid(mvid)
    if (version == null) {
      throw new NotFoundException("Model Version not found")
    }
    version
  }

  /**
    * Helper function to get the latest model version from the DB
    */
  private def getLatestModelVersion(
      ctx: DSLContext,
      mid: Integer
  ): Option[ModelVersion] = {
    ctx
      .selectFrom(MODEL_VERSION)
      .where(MODEL_VERSION.MID.eq(mid))
      .orderBy(MODEL_VERSION.CREATION_TIME.desc())
      .limit(1)
      .fetchOptionalInto(classOf[ModelVersion])
      .toScala
  }

  case class DashboardModel(
      model: Model,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean,
      size: Long
  )

  case class DashboardModelVersion(
      modelVersion: ModelVersion,
      fileNodes: List[DatasetFileNode]
  )

  case class CreateModelRequest(
      modelName: String,
      modelDescription: String,
      isModelPublic: Boolean,
      isModelDownloadable: Boolean
  )

  case class Diff(
      path: String,
      pathType: String,
      diffType: String, // "added", "removed", "changed", etc.
      sizeBytes: Option[Long] // Size of the changed file (None for directories)
  )

  case class ModelDescriptionModification(mid: Integer, description: String)

  case class ModelVersionRootFileNodesResponse(
      fileNodes: List[DatasetFileNode],
      size: Long
  )
}

@Produces(Array(MediaType.APPLICATION_JSON, "image/jpeg", "application/pdf"))
@Path("/model")
class ModelResource {
  private val ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE = "User has no access to this model"
  private val ERR_MODEL_VERSION_NOT_FOUND_MESSAGE = "The version of the model not found"
  private val EXPIRATION_MINUTES = 5

  /**
    * Helper function to get the model from DB with additional information including user access privilege and owner email
    */
  private def getDashboardModel(
      ctx: DSLContext,
      mid: Integer,
      requesterUid: Option[Integer]
  ): DashboardModel = {
    val targetModel = getModelByID(ctx, mid)

    if (requesterUid.isEmpty && !targetModel.getIsPublic) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
    } else if (requesterUid.exists(uid => !userHasReadAccess(ctx, mid, uid))) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
    }

    val userAccessPrivilege = requesterUid
      .map(uid => getModelUserAccessPrivilege(ctx, mid, uid))
      .getOrElse(PrivilegeEnum.READ)

    val isOwner = requesterUid.contains(targetModel.getOwnerUid)

    DashboardModel(
      targetModel,
      getOwner(ctx, mid).getEmail,
      userAccessPrivilege,
      isOwner,
      LakeFSStorageClient.retrieveRepositorySize(targetModel.getName)
    )
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/create")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def createDataset(
      request: CreateModelRequest,
      @Auth user: SessionUser
  ): DashboardModel = {

    withTransaction(context) { ctx =>
      val uid = user.getUid
      val modelDao: ModelDao = new ModelDao(ctx.configuration())
      val modelUserAccessDao: ModelUserAccessDao = new ModelUserAccessDao(ctx.configuration())

      val modelName = request.modelName
      val modelDescription = request.modelDescription
      val isModelPublic = request.isModelPublic
      val isModelDownloadable = request.isModelDownloadable

      // Check if a model with the same name already exists
      if (!modelDao.fetchByName(modelName).isEmpty) {
        throw new BadRequestException("model with the same name already exists")
      }

      // Initialize the repository in LakeFS
      try {
        LakeFSStorageClient.initRepo(modelName)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(
            s"Failed to create the model: ${e.getMessage}"
          )
      }

      // Insert the model into the database
      val model = new Model()
      model.setName(modelName)
      model.setDescription(modelDescription)
      model.setIsPublic(isModelPublic)
      model.setIsDownloadable(isModelDownloadable)
      model.setOwnerUid(uid)

      val createdModel = ctx
        .insertInto(MODEL)
        .set(ctx.newRecord(MODEL, model))
        .returning()
        .fetchOne()

      // Insert the requester as the WRITE access user for this model
      val modelUserAccess = new ModelUserAccess()
      modelUserAccess.setMid(createdModel.getMid)
      modelUserAccess.setUid(uid)
      modelUserAccess.setPrivilege(PrivilegeEnum.WRITE)
      modelUserAccessDao.insert(modelUserAccess)

      DashboardModel(
        new Model(
          createdModel.getMid,
          createdModel.getOwnerUid,
          createdModel.getName,
          createdModel.getIsPublic,
          createdModel.getIsDownloadable,
          createdModel.getDescription,
          createdModel.getCreationTime
        ),
        user.getEmail,
        PrivilegeEnum.WRITE,
        isOwner = true,
        0
      )
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/create")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def createDatasetVersion(
      versionName: String,
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): DashboardModelVersion = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val model = getModelByID(ctx, mid)
      val modelName = model.getName

      // Check if there are any changes in LakeFS before creating a new version
      val diffs = LakeFSStorageClient.retrieveUncommittedObjects(repoName = modelName)

      if (diffs.isEmpty) {
        throw new WebApplicationException(
          "No changes detected in model. Version creation aborted.",
          Response.Status.BAD_REQUEST
        )
      }

      // Generate a new version name
      val versionCount = ctx
        .selectCount()
        .from(MODEL_VERSION)
        .where(MODEL_VERSION.MID.eq(mid))
        .fetchOne(0, classOf[Int])

      val sanitizedVersionName = Option(versionName).filter(_.nonEmpty).getOrElse("")
      val newVersionName = if (sanitizedVersionName.isEmpty) {
        s"v${versionCount + 1}"
      } else {
        s"v${versionCount + 1} - $sanitizedVersionName"
      }

      // Create a commit in LakeFS
      val commit = LakeFSStorageClient.createCommit(
        repoName = modelName,
        branch = "main",
        commitMessage = s"Created model version: $newVersionName"
      )

      if (commit == null || commit.getId == null) {
        throw new WebApplicationException(
          "Failed to create commit in LakeFS. Version creation aborted.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      // Create a new model version entry in the database
      val modelVersion = new ModelVersion()
      modelVersion.setMid(mid)
      modelVersion.setCreatorUid(uid)
      modelVersion.setName(newVersionName)
      modelVersion.setVersionHash(commit.getId) // Store LakeFS version hash

      val insertedVersion = ctx
        .insertInto(MODEL_VERSION)
        .set(ctx.newRecord(MODEL_VERSION, modelVersion))
        .returning()
        .fetchOne()
        .into(classOf[ModelVersion])

      // Retrieve committed file structure
      val fileNodes = LakeFSStorageClient.retrieveObjectsOfVersion(modelName, commit.getId)

      DashboardModelVersion(
        insertedVersion,
        DatasetFileNode
          .fromLakeFSRepositoryCommittedObjects(
            Map((user.getEmail, modelName, newVersionName) -> fileNodes)
          )
      )
    }
  }

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}")
  def deleteDataset(@PathParam("mid") mid: Integer, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      val model = getModelByID(ctx, mid)
      if (!userOwnDataset(ctx, model.getMid, uid)) {
        // throw the exception that user has no access to certain model
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      try {
        LakeFSStorageClient.deleteRepo(model.getName)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(
            s"Failed to delete a repository in LakeFS: ${e.getMessage}",
            e
          )
      }

      // delete the directory on S3
      if (S3StorageClient.directoryExists(StorageConfig.lakefsBucketName, model.getName)) {
        S3StorageClient.deleteDirectory(StorageConfig.lakefsBucketName, model.getName)
      }

      // delete the model from the DB
      modelDao.deleteById(model.getMid)

      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/description")
  def updateModelDescription(
      modificator: ModelDescriptionModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid
      val modelDao = new ModelDao(ctx.configuration())
      val model = getModelByID(ctx, modificator.mid)
      if (!userHasWriteAccess(ctx, modificator.mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      model.setDescription(modificator.description)
      modelDao.update(model)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/upload")
  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  def uploadOneFileToModel(
      @PathParam("mid") mid: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @QueryParam("message") message: String,
      fileStream: InputStream,
      @Context headers: HttpHeaders,
      @Auth user: SessionUser
  ): Response = {
    // These variables are defined at the top so catch block can access them
    val uid = user.getUid
    var repoName: String = null
    var filePath: String = null
    var uploadId: String = null
    var physicalAddress: String = null

    try {
      withTransaction(context) { ctx =>
        if (!userHasWriteAccess(ctx, mid, uid))
          throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)

        val model = getModelByID(ctx, mid)
        repoName = model.getName
        filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name)

        // ---------- decide part-size & number-of-parts ----------
        val declaredLen = Option(headers.getHeaderString(HttpHeaders.CONTENT_LENGTH)).map(_.toLong)
        var partSize = StorageConfig.s3MultipartUploadPartSize

        declaredLen.foreach { ln =>
          val needed = ((ln + partSize - 1) / partSize).toInt
          if (needed > MAXIMUM_NUM_OF_MULTIPART_S3_PARTS)
            partSize = math.max(
              MINIMUM_NUM_OF_MULTIPART_S3_PART,
              ln / (MAXIMUM_NUM_OF_MULTIPART_S3_PARTS - 1)
            )
        }

        val expectedParts = declaredLen
          .map(ln =>
            ((ln + partSize - 1) / partSize).toInt + 1
          ) // “+1” for the last (possibly small) part
          .getOrElse(MAXIMUM_NUM_OF_MULTIPART_S3_PARTS)

        // ---------- ask LakeFS for presigned URLs ----------
        val presign = LakeFSStorageClient
          .initiatePresignedMultipartUploads(repoName, filePath, expectedParts)
        uploadId = presign.getUploadId
        val presignedUrls = presign.getPresignedUrls.asScala.iterator
        physicalAddress = presign.getPhysicalAddress

        // ---------- stream & upload parts ----------
        /*
        1. Reads the input stream in chunks of 'partSize' bytes by stacking them in a buffer
        2. Uploads each chunk (part) using a presigned URL
        3. Tracks each part number and ETag returned from S3
        4. After all parts are uploaded, completes the multipart upload
         */
        val buf = new Array[Byte](partSize.toInt)
        var buffered = 0
        var partNumber = 1
        val completedParts = ListBuffer[(Int, String)]()

        @inline def flush(): Unit = {
          if (buffered == 0) return
          if (!presignedUrls.hasNext)
            throw new WebApplicationException("Ran out of presigned part URLs – ask for more parts")

          val etag = put(buf, buffered, presignedUrls.next(), partNumber)
          completedParts += ((partNumber, etag))
          partNumber += 1
          buffered = 0
        }

        var read = fileStream.read(buf, buffered, buf.length - buffered)
        while (read != -1) {
          buffered += read
          if (buffered == buf.length) flush() // buffer full
          read = fileStream.read(buf, buffered, buf.length - buffered)
        }
        fileStream.close()
        flush()

        // ---------- complete upload ----------
        LakeFSStorageClient.completePresignedMultipartUploads(
          repoName,
          filePath,
          uploadId,
          completedParts.toList,
          physicalAddress
        )

        Response.ok(Map("message" -> s"Uploaded $filePath in ${completedParts.size} parts")).build()
      }
    } catch {
      case e: Exception =>
        if (repoName != null && filePath != null && uploadId != null && physicalAddress != null) {
          LakeFSStorageClient.abortPresignedMultipartUploads(
            repoName,
            filePath,
            uploadId,
            physicalAddress
          )
        }
        throw new WebApplicationException(
          s"Failed to upload file to model: ${e.getMessage}",
          e
        )
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download")
  def getPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("modelName") modelName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    generatePresignedResponse(encodedUrl, modelName, commitHash, uid)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download-s3")
  def getPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("modelName") modelName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    generatePresignedResponse(encodedUrl, modelName, commitHash, uid)
  }

  @GET
  @Path("/public-presign-download")
  def getPublicPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("modelName") modelName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response = {
    generatePresignedResponse(encodedUrl, modelName, commitHash, null)
  }

  @GET
  @Path("/public-presign-download-s3")
  def getPublicPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("modelName") modelName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response = {
    generatePresignedResponse(encodedUrl, modelName, commitHash, null)
  }

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/file")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def deleteDatasetFile(
      @PathParam("mid") mid: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      val modelName = getModelByID(ctx, mid).getName

      // Decode the file path
      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      // Try to initialize the repository in LakeFS
      try {
        LakeFSStorageClient.deleteObject(modelName, filePath)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(
            s"Failed to delete the file from repo in LakeFS: ${e.getMessage}"
          )
      }

      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/multipart-upload")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def multipartUpload(
      @QueryParam("modelName") modelName: String,
      @QueryParam("type") operationType: String,
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("uploadId") uploadId: Optional[String],
      @QueryParam("numParts") numParts: Optional[Integer],
      payload: Map[
        String,
        Any
      ], // Expecting {"parts": [...], "physicalAddress": "s3://bucket/path"}
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid

    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      val models = modelDao.fetchByName(modelName).asScala.toList
      if (models.isEmpty || !userHasWriteAccess(ctx, models.head.getMid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      // Decode the file path
      val filePath = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name())

      operationType.toLowerCase match {
        case "init" =>
          val numPartsValue = numParts.toScala.getOrElse(
            throw new BadRequestException("numParts is required for initialization")
          )

          val presignedResponse = LakeFSStorageClient.initiatePresignedMultipartUploads(
            modelName,
            filePath,
            numPartsValue
          )
          Response
            .ok(
              Map(
                "uploadId" -> presignedResponse.getUploadId,
                "presignedUrls" -> presignedResponse.getPresignedUrls,
                "physicalAddress" -> presignedResponse.getPhysicalAddress
              )
            )
            .build()

        case "finish" =>
          val uploadIdValue = uploadId.toScala.getOrElse(
            throw new BadRequestException("uploadId is required for completion")
          )

          // Extract parts from the payload
          val partsList = payload.get("parts") match {
            case Some(rawList: List[_]) =>
              try {
                rawList.map {
                  case part: Map[_, _] =>
                    val partMap = part.asInstanceOf[Map[String, Any]]
                    val partNumber = partMap.get("PartNumber") match {
                      case Some(i: Int)    => i
                      case Some(s: String) => s.toInt
                      case _               => throw new BadRequestException("Invalid or missing PartNumber")
                    }
                    val eTag = partMap.get("ETag") match {
                      case Some(s: String) => s
                      case _               => throw new BadRequestException("Invalid or missing ETag")
                    }
                    (partNumber, eTag)

                  case _ =>
                    throw new BadRequestException("Each part must be a Map[String, Any]")
                }
              } catch {
                case e: NumberFormatException =>
                  throw new BadRequestException("PartNumber must be an integer", e)
              }

            case _ =>
              throw new BadRequestException("Missing or invalid 'parts' list in payload")
          }

          // Extract physical address from payload
          val physicalAddress = payload.get("physicalAddress") match {
            case Some(address: String) => address
            case _                     => throw new BadRequestException("Missing physicalAddress in payload")
          }

          // Complete the multipart upload with parts and physical address
          val objectStats = LakeFSStorageClient.completePresignedMultipartUploads(
            modelName,
            filePath,
            uploadIdValue,
            partsList,
            physicalAddress
          )

          Response
            .ok(
              Map(
                "message" -> "Multipart upload completed successfully",
                "filePath" -> objectStats.getPath
              )
            )
            .build()

        case "abort" =>
          val uploamidValue = uploadId.toScala.getOrElse(
            throw new BadRequestException("uploamid is required for abortion")
          )

          // Extract physical address from payload
          val physicalAddress = payload.get("physicalAddress") match {
            case Some(address: String) => address
            case _                     => throw new BadRequestException("Missing physicalAddress in payload")
          }

          // Abort the multipart upload
          LakeFSStorageClient.abortPresignedMultipartUploads(
            modelName,
            filePath,
            uploamidValue,
            physicalAddress
          )

          Response.ok(Map("message" -> "Multipart upload aborted successfully")).build()

        case _ =>
          throw new BadRequestException("Invalid type parameter. Use 'init', 'finish', or 'abort'.")
      }
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/update/publicity")
  def toggleDatasetPublicity(
      @PathParam("mid") mid: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      val existedModel = getModelByID(ctx, mid)
      val newPublicStatus = !existedModel.getIsPublic
      existedModel.setIsPublic(newPublicStatus)

      modelDao.update(existedModel)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/update/downloadable")
  def toggleDatasetDownloadable(
      @PathParam("mid") mid: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val modelDao = new ModelDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userOwnDataset(ctx, mid, uid)) {
        throw new ForbiddenException("Only model owners can modify download permissions")
      }

      val existedModel = getModelByID(ctx, mid)
      val newDownloadableStatus = !existedModel.getIsDownloadable

      existedModel.setIsDownloadable(newDownloadableStatus)

      modelDao.update(existedModel)
      Response.ok().build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/diff")
  def getDatasetDiff(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): List[Diff] = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasReadAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      // Retrieve staged (uncommitted) changes from LakeFS
      val model = getModelByID(ctx, mid)
      val lakefsDiffs = LakeFSStorageClient.retrieveUncommittedObjects(model.getName)

      // Convert LakeFS Diff objects to our custom Diff case class
      lakefsDiffs.map(d =>
        Diff(
          d.getPath,
          d.getPathType.getValue,
          d.getType.getValue,
          Option(d.getSizeBytes).map(_.longValue())
        )
      )
    }
  }

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/diff")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def resetDatasetFileDiff(
      @PathParam("mid") mid: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      val modelName = getModelByID(ctx, mid).getName

      // Decode the file path
      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      // Try to reset the file change in LakeFS
      try {
        LakeFSStorageClient.resetObjectUploadOrDeletion(modelName, filePath)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(
            s"Failed to reset the changes from repo in LakeFS: ${e.getMessage}"
          )
      }
      Response.ok().build()
    }
  }

  /**
    * This method returns a list of Dashboardmodels objects that are accessible by current user.
    *
    * @param user the session user
    * @return list of user accessible Dashboardmodel objects
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
  def listDatasets(
      @Auth user: SessionUser
  ): List[DashboardModel] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      var accessibleModels: ListBuffer[DashboardModel] = ListBuffer()
      // first fetch all models user have explicit access to
      accessibleModels = ListBuffer.from(
        ctx
          .select()
          .from(
            MODEL
              .leftJoin(MODEL_USER_ACCESS)
              .on(MODEL_USER_ACCESS.MID.eq(MODEL.MID))
              .leftJoin(USER)
              .on(USER.UID.eq(MODEL.OWNER_UID))
          )
          .where(MODEL_USER_ACCESS.UID.eq(uid))
          .fetch()
          .map(record => {
            val model = record.into(MODEL).into(classOf[Model])
            val modelAccess = record.into(MODEL_USER_ACCESS).into(classOf[ModelUserAccess])
            val ownerEmail = record.into(USER).getEmail
            DashboardModel(
              isOwner = model.getOwnerUid == uid,
              model = model,
              accessPrivilege = modelAccess.getPrivilege,
              ownerEmail = ownerEmail,
              size = 0
            )
          })
          .asScala
      )

      // then we fetch the public models and merge it as a part of the result if not exist
      val publicModels = ctx
        .select()
        .from(
          MODEL
            .leftJoin(USER)
            .on(USER.UID.eq(MODEL.OWNER_UID))
        )
        .where(MODEL.IS_PUBLIC.eq(true))
        .fetch()
        .map(record => {
          val model = record.into(MODEL).into(classOf[Model])
          val ownerEmail = record.into(USER).getEmail
          DashboardModel(
            isOwner = false,
            model = model,
            accessPrivilege = PrivilegeEnum.READ,
            ownerEmail = ownerEmail,
            size = LakeFSStorageClient.retrieveRepositorySize(model.getName)
          )
        })
      publicModels.forEach { publicModel =>
        if (!accessibleModels.exists(_.model.getMid == publicModel.model.getMid)) {
          val dashboardDataset = DashboardModel(
            isOwner = false,
            model = publicModel.model,
            ownerEmail = publicModel.ownerEmail,
            accessPrivilege = PrivilegeEnum.READ,
            size = LakeFSStorageClient.retrieveRepositorySize(publicModel.model.getName)
          )
          accessibleModels = accessibleModels :+ dashboardDataset
        }
      }
      accessibleModels.toList
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/list")
  def getDatasetVersionList(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): List[ModelVersion] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val model = getModelByID(ctx, mid)
      if (!userHasReadAccess(ctx, model.getMid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      fetchModelVersions(ctx, model.getMid)
    })
  }

  @GET
  @Path("/{name}/publicVersion/list")
  def getPublicDatasetVersionList(
      @PathParam("name") mid: Integer
  ): List[ModelVersion] = {
    withTransaction(context)(ctx => {
      if (!isDatasetPublic(ctx, mid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      fetchModelVersions(ctx, mid)
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/latest")
  def retrieveLatestDatasetVersion(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): DashboardModelVersion = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      if (!userHasReadAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }
      val model = getModelByID(ctx, mid)
      val latestVersion = getLatestModelVersion(ctx, mid).getOrElse(
        throw new NotFoundException(ERR_MODEL_VERSION_NOT_FOUND_MESSAGE)
      )

      val ownerNode = DatasetFileNode
        .fromLakeFSRepositoryCommittedObjects(
          Map(
            (user.getEmail, model.getName, latestVersion.getName) ->
              LakeFSStorageClient
                .retrieveObjectsOfVersion(model.getName, latestVersion.getVersionHash)
          )
        )
        .head

      DashboardModelVersion(
        latestVersion,
        ownerNode.children.get
          .find(_.getName == model.getName)
          .head
          .children
          .get
          .find(_.getName == latestVersion.getName)
          .head
          .children
          .get
      )
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/versionZip")
  def getDatasetVersionZip(
      @PathParam("mid") mid: Integer,
      @QueryParam("dvid") dvid: Integer, // model version ID, nullable
      @QueryParam("latest") latest: java.lang.Boolean, // Flag to get latest version, nullable
      @Auth user: SessionUser
  ): Response = {

    withTransaction(context) { ctx =>
      if ((dvid != null && latest != null) || (dvid == null && latest == null)) {
        throw new BadRequestException("Specify exactly one: dvid=<ID> OR latest=true")
      }

      // Check read access and download permission
      val uid = user.getUid
      if (!userHasReadAccess(ctx, mid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)
      }

      // Retrieve model and check download permission
      val model = getModelByID(ctx, mid)
      // Non-owners can download if model is downloadable and they have read access
      if (!userOwnDataset(ctx, mid, uid) && !model.getIsDownloadable) {
        throw new ForbiddenException("model download is not allowed")
      }

      // Determine which version to retrieve
      val modelVersion = if (dvid != null) {
        getModelVersionByID(ctx, dvid)
      } else if (java.lang.Boolean.TRUE.equals(latest)) {
        getLatestModelVersion(ctx, mid).getOrElse(
          throw new NotFoundException(ERR_MODEL_VERSION_NOT_FOUND_MESSAGE)
        )
      } else {
        throw new BadRequestException("Invalid parameters")
      }

      // Retrieve model and version details
      val modelName = model.getName
      val versionHash = modelVersion.getVersionHash
      val objects = LakeFSStorageClient.retrieveObjectsOfVersion(modelName, versionHash)

      if (objects.isEmpty) {
        return Response
          .status(Response.Status.NOT_FOUND)
          .entity(s"No objects found in version $versionHash of repository $modelName")
          .build()
      }

      // StreamingOutput for ZIP download
      val streamingOutput = new StreamingOutput {
        override def write(outputStream: OutputStream): Unit = {
          val zipOut = new ZipOutputStream(outputStream)
          try {
            objects.foreach { obj =>
              val filePath = obj.getPath
              val file = LakeFSStorageClient.getFileFromRepo(modelName, versionHash, filePath)

              zipOut.putNextEntry(new ZipEntry(filePath))
              Files.copy(Paths.get(file.toURI), zipOut)
              zipOut.closeEntry()
            }
          } finally {
            zipOut.close()
          }
        }
      }

      val zipFilename = s"""attachment; filename="$modelName-${modelVersion.getName}.zip""""

      Response
        .ok(streamingOutput, "application/zip")
        .header("Content-Disposition", zipFilename)
        .build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}/version/{dvid}/rootFileNodes")
  def retrieveDatasetVersionRootFileNodes(
      @PathParam("mid") mid: Integer,
      @PathParam("dvid") dvid: Integer,
      @Auth user: SessionUser
  ): ModelVersionRootFileNodesResponse = {
    val uid = user.getUid
    withTransaction(context)(ctx => fetchModelVersionRootFileNodes(ctx, mid, dvid, Some(uid)))
  }

  @GET
  @Path("/{mid}/publicVersion/{dvid}/rootFileNodes")
  def retrievePublicDatasetVersionRootFileNodes(
      @PathParam("mid") mid: Integer,
      @PathParam("dvid") dvid: Integer
  ): ModelVersionRootFileNodesResponse = {
    withTransaction(context)(ctx => fetchModelVersionRootFileNodes(ctx, mid, dvid, None))
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{mid}")
  def getDataset(
      @PathParam("mid") mid: Integer,
      @Auth user: SessionUser
  ): DashboardModel = {
    val uid = user.getUid
    withTransaction(context)(ctx => getDashboardModel(ctx, mid, Some(uid)))
  }

  @GET
  @Path("/public/{mid}")
  def getPublicDataset(
      @PathParam("mid") mid: Integer
  ): DashboardModel = {
    withTransaction(context)(ctx => getDashboardModel(ctx, mid, None))
  }

  @GET
  @Path("/file")
  def retrieveDatasetSingleFile(
      @QueryParam("path") pathStr: String
  ): Response = {
    val decodedPathStr = URLDecoder.decode(pathStr, StandardCharsets.UTF_8.name())

    withTransaction(context)(_ => {
      val fileUri = FileResolver.resolve(decodedPathStr)
      val streamingOutput = new StreamingOutput() {
        override def write(output: OutputStream): Unit = {
          val inputStream = DocumentFactory.openReadonlyDocument(fileUri).asInputStream()
          try {
            val buffer = new Array[Byte](8192) // buffer size
            var bytesRead = inputStream.read(buffer)
            while (bytesRead != -1) {
              output.write(buffer, 0, bytesRead)
              bytesRead = inputStream.read(buffer)
            }
          } finally {
            inputStream.close()
          }
        }
      }

      val contentType = decodedPathStr.split("\\.").lastOption.map(_.toLowerCase) match {
        case Some("jpg") | Some("jpeg") => "image/jpeg"
        case Some("png")                => "image/png"
        case Some("csv")                => "text/csv"
        case Some("md")                 => "text/markdown"
        case Some("txt")                => "text/plain"
        case Some("html") | Some("htm") => "text/html"
        case Some("json")               => "application/json"
        case Some("pdf")                => "application/pdf"
        case Some("doc") | Some("docx") => "application/msword"
        case Some("xls") | Some("xlsx") => "application/vnd.ms-excel"
        case Some("ppt") | Some("pptx") => "application/vnd.ms-powerpoint"
        case Some("mp4")                => "video/mp4"
        case Some("mp3")                => "audio/mpeg"
        case _                          => "application/octet-stream" // default binary format
      }

      Response.ok(streamingOutput).`type`(contentType).build()
    })
  }

  /**
    * This method returns all owner user names of the model that the user has access to
    *
    * @return OwnerName[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-model-owners")
  def retrieveOwners(@Auth user: SessionUser): util.List[String] = {
    context
      .selectDistinct(USER.EMAIL)
      .from(USER)
      .join(MODEL)
      .on(MODEL.OWNER_UID.eq(USER.UID))
      .join(MODEL_USER_ACCESS)
      .on(MODEL_USER_ACCESS.MID.eq(MODEL.MID))
      .where(MODEL_USER_ACCESS.UID.eq(user.getUid))
      .fetchInto(classOf[String])
  }

  private def fetchModelVersions(ctx: DSLContext, mid: Integer): List[ModelVersion] = {
    ctx
      .selectFrom(MODEL_VERSION)
      .where(MODEL_VERSION.MID.eq(mid))
      .orderBy(MODEL_VERSION.CREATION_TIME.desc()) // Change to .asc() for ascending order
      .fetchInto(classOf[ModelVersion])
      .asScala
      .toList
  }

  private def fetchModelVersionRootFileNodes(
      ctx: DSLContext,
      mid: Integer,
      dvid: Integer,
      uid: Option[Integer]
  ): ModelVersionRootFileNodesResponse = {
    val model = getDashboardModel(ctx, mid, uid)
    val modelVersion = getModelVersionByID(ctx, dvid)
    val modelName = model.model.getName

    val ownerFileNode = DatasetFileNode
      .fromLakeFSRepositoryCommittedObjects(
        Map(
          (model.ownerEmail, modelName, modelVersion.getName) -> LakeFSStorageClient
            .retrieveObjectsOfVersion(modelName, modelVersion.getVersionHash)
        )
      )
      .head

    ModelVersionRootFileNodesResponse(
      ownerFileNode.children.get
        .find(_.getName == modelName)
        .head
        .children
        .get
        .find(_.getName == modelVersion.getName)
        .head
        .children
        .get,
      DatasetFileNode.calculateTotalSize(List(ownerFileNode))
    )
  }

  private def generatePresignedResponse(
      encodedUrl: String,
      modelName: String,
      commitHash: String,
      uid: Integer
  ): Response = {
    resolveModelAndPath(encodedUrl, modelName, commitHash, uid) match {
      case Left(errorResponse) =>
        errorResponse

      case Right((resolvedModelName, resolvedCommitHash, resolvedFilePath)) =>
        val url = LakeFSStorageClient.getFilePresignedUrl(
          resolvedModelName,
          resolvedCommitHash,
          resolvedFilePath
        )

        Response.ok(Map("presignedUrl" -> url)).build()
    }
  }

  private def resolveModelAndPath(
      encodedUrl: String,
      modelName: String,
      commitHash: String,
      uid: Integer
  ): Either[Response, (String, String, String)] = {
    val decodedPathStr = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name())

    (Option(modelName), Option(commitHash)) match {
      case (Some(_), None) | (None, Some(_)) =>
        // Case 1: Only one parameter is provided (error case)
        Left(
          Response
            .status(Response.Status.BAD_REQUEST)
            .entity(
              "Both modelName and commitHash must be provided together, or neither should be provided."
            )
            .build()
        )

      case (Some(dsName), Some(commit)) =>
        // Case 2: modelName and commitHash are provided, validate access
        val response = withTransaction(context) { ctx =>
          val modelDao = new ModelDao(ctx.configuration())
          val models = modelDao.fetchByName(dsName).asScala.toList

          if (models.isEmpty || !userHasReadAccess(ctx, models.head.getMid, uid))
            throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)

          val model = models.head
          // Standard read access check only - download restrictions handled per endpoint
          // Non-download operations (viewing) should work for all public models

          (dsName, commit, decodedPathStr)
        }
        Right(response)

      case (None, None) =>
        // Case 3: Neither modelName nor commitHash are provided, resolve normally
        val response = withTransaction(context) { ctx =>
          println("Resolving file path without modelName and commitHash")
          val fileUri = FileResolver.resolve(decodedPathStr)
          println(s"Resolved file URI: $fileUri")
          val document = DocumentFactory.openReadonlyDocument(fileUri).asInstanceOf[OnDataset]
          println(
            s"Extracted model: ${document.getDatasetName()}, versionHash: ${document.getVersionHash()}, fileRelativePath: ${document.getFileRelativePath()}"
          )
          val modelDao = new ModelDao(ctx.configuration())

          val models = modelDao.fetchByName(document.getDatasetName()).asScala.toList

          if (models.isEmpty || !userHasReadAccess(ctx, models.head.getMid, uid))
            throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_MODEL_MESSAGE)

          // Standard read access check only - download restrictions handled per endpoint
          // Non-download operations (viewing) should work for all public models

          (
            document.getDatasetName(),
            document.getVersionHash(),
            document.getFileRelativePath()
          )
        }
        Right(response)
    }
  }
}
