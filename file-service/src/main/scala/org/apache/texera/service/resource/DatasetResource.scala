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

import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core._
import org.apache.texera.amber.config.StorageConfig
import org.apache.texera.amber.core.storage.model.OnDataset
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.amber.core.storage.{DocumentFactory, FileResolver}
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  DatasetUserAccessDao,
  DatasetVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  DatasetUserAccess,
  DatasetVersion
}
import org.apache.texera.service.`type`.DatasetFileNode
import org.apache.texera.service.resource.DatasetAccessResource._
import org.apache.texera.service.resource.DatasetResource.{context, _}
import org.apache.texera.service.util.S3StorageClient
import org.apache.texera.service.util.S3StorageClient.{
  MAXIMUM_NUM_OF_MULTIPART_S3_PARTS,
  MINIMUM_NUM_OF_MULTIPART_S3_PART
}
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

object DatasetResource {

  private val context = SqlServer
    .getInstance()
    .createDSLContext()

  /**
    * Helper function to get the dataset from DB using did
    */
  private def getDatasetByID(ctx: DSLContext, did: Integer): Dataset = {
    val datasetDao = new DatasetDao(ctx.configuration())
    val dataset = datasetDao.fetchOneByDid(did)
    if (dataset == null) {
      throw new NotFoundException(f"Dataset $did not found")
    }
    dataset
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
    * Helper function to get the dataset version from DB using dvid
    */
  private def getDatasetVersionByID(
      ctx: DSLContext,
      dvid: Integer
  ): DatasetVersion = {
    val datasetVersionDao = new DatasetVersionDao(ctx.configuration())
    val version = datasetVersionDao.fetchOneByDvid(dvid)
    if (version == null) {
      throw new NotFoundException("Dataset Version not found")
    }
    version
  }

  /**
    * Helper function to get the latest dataset version from the DB
    */
  private def getLatestDatasetVersion(
      ctx: DSLContext,
      did: Integer
  ): Option[DatasetVersion] = {
    ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(DATASET_VERSION.CREATION_TIME.desc())
      .limit(1)
      .fetchOptionalInto(classOf[DatasetVersion])
      .toScala
  }

  case class DashboardDataset(
      dataset: Dataset,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean,
      size: Long
  )

  case class DashboardDatasetVersion(
      datasetVersion: DatasetVersion,
      fileNodes: List[DatasetFileNode]
  )

  case class CreateDatasetRequest(
      datasetName: String,
      datasetDescription: String,
      isDatasetPublic: Boolean,
      isDatasetDownloadable: Boolean
  )

  case class Diff(
      path: String,
      pathType: String,
      diffType: String, // "added", "removed", "changed", etc.
      sizeBytes: Option[Long] // Size of the changed file (None for directories)
  )

  case class DatasetDescriptionModification(did: Integer, description: String)

  case class DatasetVersionRootFileNodesResponse(
      fileNodes: List[DatasetFileNode],
      size: Long
  )
}

@Produces(Array(MediaType.APPLICATION_JSON, "image/jpeg", "application/pdf"))
@Path("/dataset")
class DatasetResource {
  private val ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE = "User has no access to this dataset"
  private val ERR_DATASET_VERSION_NOT_FOUND_MESSAGE = "The version of the dataset not found"
  private val EXPIRATION_MINUTES = 5

  /**
    * Helper function to get the dataset from DB with additional information including user access privilege and owner email
    */
  private def getDashboardDataset(
      ctx: DSLContext,
      did: Integer,
      requesterUid: Option[Integer]
  ): DashboardDataset = {
    val targetDataset = getDatasetByID(ctx, did)

    if (requesterUid.isEmpty && !targetDataset.getIsPublic) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    } else if (requesterUid.exists(uid => !userHasReadAccess(ctx, did, uid))) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }

    val userAccessPrivilege = requesterUid
      .map(uid => getDatasetUserAccessPrivilege(ctx, did, uid))
      .getOrElse(PrivilegeEnum.READ)

    val isOwner = requesterUid.contains(targetDataset.getOwnerUid)

    DashboardDataset(
      targetDataset,
      getOwner(ctx, did).getEmail,
      userAccessPrivilege,
      isOwner,
      LakeFSStorageClient.retrieveRepositorySize(targetDataset.getRepositoryName)
    )
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/create")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def createDataset(
      request: CreateDatasetRequest,
      @Auth user: SessionUser
  ): DashboardDataset = {

    withTransaction(context) { ctx =>
      val uid = user.getUid
      val datasetUserAccessDao: DatasetUserAccessDao = new DatasetUserAccessDao(ctx.configuration())

      val datasetName = request.datasetName
      val datasetDescription = request.datasetDescription
      val isDatasetPublic = request.isDatasetPublic
      val isDatasetDownloadable = request.isDatasetDownloadable

      // validate dataset name
      try {
        validateDatasetName(datasetName)
      } catch {
        case e: IllegalArgumentException =>
          throw new BadRequestException(e.getMessage)
      }

      // Check if a dataset with the same name already exists
      val existingDatasets = context
        .selectFrom(DATASET)
        .where(DATASET.OWNER_UID.eq(uid))
        .and(DATASET.NAME.eq(datasetName))
        .fetch()
      if (!existingDatasets.isEmpty) {
        throw new BadRequestException("Dataset with the same name already exists")
      }

      // insert the dataset into the database
      val dataset = new Dataset()
      dataset.setName(datasetName)
      dataset.setDescription(datasetDescription)
      dataset.setIsPublic(isDatasetPublic)
      dataset.setIsDownloadable(isDatasetDownloadable)
      dataset.setOwnerUid(uid)

      // insert record and get created dataset with did
      val createdDataset = ctx
        .insertInto(DATASET)
        .set(ctx.newRecord(DATASET, dataset))
        .returning()
        .fetchOne()

      // Initialize the repository in LakeFS
      val repositoryName = s"dataset-${createdDataset.getDid}"
      try {
        LakeFSStorageClient.initRepo(repositoryName)
      } catch {
        case e: Exception =>
          ctx
            .deleteFrom(DATASET)
            .where(DATASET.DID.eq(createdDataset.getDid))
            .execute()
          throw new WebApplicationException(
            s"Failed to create the dataset: ${e.getMessage}"
          )
      }

      // update repository name of the created dataset
      createdDataset.setRepositoryName(repositoryName)
      createdDataset.update()

      // Insert the requester as the WRITE access user for this dataset
      val datasetUserAccess = new DatasetUserAccess()
      datasetUserAccess.setDid(createdDataset.getDid)
      datasetUserAccess.setUid(uid)
      datasetUserAccess.setPrivilege(PrivilegeEnum.WRITE)
      datasetUserAccessDao.insert(datasetUserAccess)

      DashboardDataset(
        createdDataset.into(classOf[Dataset]),
        user.getEmail,
        PrivilegeEnum.WRITE,
        isOwner = true,
        0
      )
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/create")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def createDatasetVersion(
      versionName: String,
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val dataset = getDatasetByID(ctx, did)
      val datasetName = dataset.getName
      val repositoryName = dataset.getRepositoryName

      // Check if there are any changes in LakeFS before creating a new version
      val diffs = LakeFSStorageClient.retrieveUncommittedObjects(repoName = repositoryName)

      if (diffs.isEmpty) {
        throw new WebApplicationException(
          "No changes detected in dataset. Version creation aborted.",
          Response.Status.BAD_REQUEST
        )
      }

      // Generate a new version name
      val versionCount = ctx
        .selectCount()
        .from(DATASET_VERSION)
        .where(DATASET_VERSION.DID.eq(did))
        .fetchOne(0, classOf[Int])

      val sanitizedVersionName = Option(versionName).filter(_.nonEmpty).getOrElse("")
      val newVersionName = if (sanitizedVersionName.isEmpty) {
        s"v${versionCount + 1}"
      } else {
        s"v${versionCount + 1} - $sanitizedVersionName"
      }

      // Create a commit in LakeFS
      val commit = LakeFSStorageClient.createCommit(
        repoName = repositoryName,
        branch = "main",
        commitMessage = s"Created dataset version: $newVersionName"
      )

      if (commit == null || commit.getId == null) {
        throw new WebApplicationException(
          "Failed to create commit in LakeFS. Version creation aborted.",
          Response.Status.INTERNAL_SERVER_ERROR
        )
      }

      // Create a new dataset version entry in the database
      val datasetVersion = new DatasetVersion()
      datasetVersion.setDid(did)
      datasetVersion.setCreatorUid(uid)
      datasetVersion.setName(newVersionName)
      datasetVersion.setVersionHash(commit.getId) // Store LakeFS version hash

      val insertedVersion = ctx
        .insertInto(DATASET_VERSION)
        .set(ctx.newRecord(DATASET_VERSION, datasetVersion))
        .returning()
        .fetchOne()
        .into(classOf[DatasetVersion])

      // Retrieve committed file structure
      val fileNodes = LakeFSStorageClient.retrieveObjectsOfVersion(repositoryName, commit.getId)

      DashboardDatasetVersion(
        insertedVersion,
        DatasetFileNode
          .fromLakeFSRepositoryCommittedObjects(
            Map((user.getEmail, datasetName, newVersionName) -> fileNodes)
          )
      )
    }
  }

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}")
  def deleteDataset(@PathParam("did") did: Integer, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val dataset = getDatasetByID(ctx, did)
      if (!userOwnDataset(ctx, dataset.getDid, uid)) {
        // throw the exception that user has no access to certain dataset
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      try {
        LakeFSStorageClient.deleteRepo(dataset.getRepositoryName)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(
            s"Failed to delete a repository in LakeFS: ${e.getMessage}",
            e
          )
      }

      // delete the directory on S3
      if (
        S3StorageClient.directoryExists(StorageConfig.lakefsBucketName, dataset.getRepositoryName)
      ) {
        S3StorageClient.deleteDirectory(StorageConfig.lakefsBucketName, dataset.getRepositoryName)
      }

      // delete the dataset from the DB
      datasetDao.deleteById(dataset.getDid)

      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/description")
  def updateDatasetDescription(
      modificator: DatasetDescriptionModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid
      val datasetDao = new DatasetDao(ctx.configuration())
      val dataset = getDatasetByID(ctx, modificator.did)
      if (!userHasWriteAccess(ctx, modificator.did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      dataset.setDescription(modificator.description)
      datasetDao.update(dataset)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/upload")
  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  def uploadOneFileToDataset(
      @PathParam("did") did: Integer,
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
        if (!userHasWriteAccess(ctx, did, uid))
          throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)

        val dataset = getDatasetByID(ctx, did)
        repoName = dataset.getRepositoryName
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
          s"Failed to upload file to dataset: ${e.getMessage}",
          e
        )
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download")
  def getPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, uid)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/presign-download-s3")
  def getPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, uid)
  }

  @GET
  @Path("/public-presign-download")
  def getPublicPresignedUrl(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response = {
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, null)
  }

  @GET
  @Path("/public-presign-download-s3")
  def getPublicPresignedUrlWithS3(
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("repositoryName") repositoryName: String,
      @QueryParam("commitHash") commitHash: String
  ): Response = {
    generatePresignedResponse(encodedUrl, repositoryName, commitHash, null)
  }

  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/file")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def deleteDatasetFile(
      @PathParam("did") did: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val repositoryName = getDatasetByID(ctx, did).getRepositoryName

      // Decode the file path
      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      // Try to initialize the repository in LakeFS
      try {
        LakeFSStorageClient.deleteObject(repositoryName, filePath)
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
      @QueryParam("type") operationType: String,
      @QueryParam("ownerEmail") ownerEmail: String,
      @QueryParam("datasetName") datasetName: String,
      @QueryParam("filePath") encodedUrl: String,
      @QueryParam("numParts") numParts: Optional[Integer],
      payload: Map[String, Any],
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    operationType.toLowerCase match {
      case "init"   => initMultipartUpload(ownerEmail, datasetName, encodedUrl, numParts, uid)
      case "finish" => finishMultipartUpload(payload, uid)
      case "abort"  => abortMultipartUpload(payload, uid)
      case _ =>
        throw new BadRequestException("Invalid type parameter. Use 'init', 'finish', or 'abort'.")
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/multipart-upload/part")
  @Consumes(Array(MediaType.APPLICATION_OCTET_STREAM))
  def uploadPart(
      @QueryParam("token") uploadToken: String,
      @QueryParam("partNumber") partNumber: Int,
      partStream: InputStream,
      @Context headers: HttpHeaders,
      @Auth user: SessionUser
  ): Response = {

    if (uploadToken == null || uploadToken.isEmpty)
      throw new BadRequestException("token is required")

    if (partNumber < 1)
      throw new BadRequestException("partNumber must be >= 1")

    val decoded = parseUploadToken(uploadToken)
    val (_, key, uploadId) = findMultipartUploadForToken(decoded, user.getUid)
    val bucket = StorageConfig.lakefsBucketName

    val contentLenHeader = headers.getHeaderString(HttpHeaders.CONTENT_LENGTH)
    val contentLength = Option(contentLenHeader).map(_.toLong)

    S3StorageClient.uploadPart(
      bucket = bucket,
      key = key,
      uploadId = uploadId,
      partNumber = partNumber,
      inputStream = partStream,
      contentLength = contentLength
    )

    Response.ok().build()
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/update/publicity")
  def toggleDatasetPublicity(
      @PathParam("did") did: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val existedDataset = getDatasetByID(ctx, did)
      val newPublicStatus = !existedDataset.getIsPublic
      existedDataset.setIsPublic(newPublicStatus)

      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/update/downloadable")
  def toggleDatasetDownloadable(
      @PathParam("did") did: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userOwnDataset(ctx, did, uid)) {
        throw new ForbiddenException("Only dataset owners can modify download permissions")
      }

      val existedDataset = getDatasetByID(ctx, did)
      val newDownloadableStatus = !existedDataset.getIsDownloadable

      existedDataset.setIsDownloadable(newDownloadableStatus)

      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/diff")
  def getDatasetDiff(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): List[Diff] = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      // Retrieve staged (uncommitted) changes from LakeFS
      val dataset = getDatasetByID(ctx, did)
      val lakefsDiffs = LakeFSStorageClient.retrieveUncommittedObjects(dataset.getRepositoryName)

      // Convert LakeFS Diff objects to our custom Diff case class
      lakefsDiffs.map(d =>
        new Diff(
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
  @Path("/{did}/diff")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def resetDatasetFileDiff(
      @PathParam("did") did: Integer,
      @QueryParam("filePath") encodedFilePath: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val repositoryName = getDatasetByID(ctx, did).getRepositoryName

      // Decode the file path
      val filePath = URLDecoder.decode(encodedFilePath, StandardCharsets.UTF_8.name())
      // Try to reset the file change in LakeFS
      try {
        LakeFSStorageClient.resetObjectUploadOrDeletion(repositoryName, filePath)
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
    * This method returns a list of DashboardDatasets objects that are accessible by current user.
    *
    * @param user the session user
    * @return list of user accessible DashboardDataset objects
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
  def listDatasets(
      @Auth user: SessionUser
  ): List[DashboardDataset] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      var accessibleDatasets: ListBuffer[DashboardDataset] = ListBuffer()
      // first fetch all datasets user have explicit access to
      accessibleDatasets = ListBuffer.from(
        ctx
          .select()
          .from(
            DATASET
              .leftJoin(DATASET_USER_ACCESS)
              .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
              .leftJoin(USER)
              .on(USER.UID.eq(DATASET.OWNER_UID))
          )
          .where(DATASET_USER_ACCESS.UID.eq(uid))
          .fetch()
          .map(record => {
            val dataset = record.into(DATASET).into(classOf[Dataset])
            val datasetAccess = record.into(DATASET_USER_ACCESS).into(classOf[DatasetUserAccess])
            val ownerEmail = record.into(USER).getEmail
            DashboardDataset(
              isOwner = dataset.getOwnerUid == uid,
              dataset = dataset,
              accessPrivilege = datasetAccess.getPrivilege,
              ownerEmail = ownerEmail,
              size = 0
            )
          })
          .asScala
      )

      // then we fetch the public datasets and merge it as a part of the result if not exist
      val publicDatasets = ctx
        .select()
        .from(
          DATASET
            .leftJoin(USER)
            .on(USER.UID.eq(DATASET.OWNER_UID))
        )
        .where(DATASET.IS_PUBLIC.eq(true))
        .fetch()
        .map(record => {
          val dataset = record.into(DATASET).into(classOf[Dataset])
          val ownerEmail = record.into(USER).getEmail
          DashboardDataset(
            isOwner = false,
            dataset = dataset,
            accessPrivilege = PrivilegeEnum.READ,
            ownerEmail = ownerEmail,
            size = LakeFSStorageClient.retrieveRepositorySize(dataset.getRepositoryName)
          )
        })
      publicDatasets.forEach { publicDataset =>
        if (!accessibleDatasets.exists(_.dataset.getDid == publicDataset.dataset.getDid)) {
          val dashboardDataset = DashboardDataset(
            isOwner = false,
            dataset = publicDataset.dataset,
            ownerEmail = publicDataset.ownerEmail,
            accessPrivilege = PrivilegeEnum.READ,
            size =
              LakeFSStorageClient.retrieveRepositorySize(publicDataset.dataset.getRepositoryName)
          )
          accessibleDatasets = accessibleDatasets :+ dashboardDataset
        }
      }
      accessibleDatasets.toList
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/list")
  def getDatasetVersionList(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): List[DatasetVersion] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val dataset = getDatasetByID(ctx, did)
      if (!userHasReadAccess(ctx, dataset.getDid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      fetchDatasetVersions(ctx, dataset.getDid)
    })
  }

  @GET
  @Path("/{name}/publicVersion/list")
  def getPublicDatasetVersionList(
      @PathParam("name") did: Integer
  ): List[DatasetVersion] = {
    withTransaction(context)(ctx => {
      if (!isDatasetPublic(ctx, did)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      fetchDatasetVersions(ctx, did)
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/latest")
  def retrieveLatestDatasetVersion(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val dataset = getDatasetByID(ctx, did)
      val latestVersion = getLatestDatasetVersion(ctx, did).getOrElse(
        throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
      )

      val ownerNode = DatasetFileNode
        .fromLakeFSRepositoryCommittedObjects(
          Map(
            (user.getEmail, dataset.getName, latestVersion.getName) ->
              LakeFSStorageClient
                .retrieveObjectsOfVersion(dataset.getRepositoryName, latestVersion.getVersionHash)
          )
        )
        .head

      DashboardDatasetVersion(
        latestVersion,
        ownerNode.children.get
          .find(_.getName == dataset.getName)
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
  @Path("/{did}/versionZip")
  def getDatasetVersionZip(
      @PathParam("did") did: Integer,
      @QueryParam("dvid") dvid: Integer, // Dataset version ID, nullable
      @QueryParam("latest") latest: java.lang.Boolean, // Flag to get latest version, nullable
      @Auth user: SessionUser
  ): Response = {

    withTransaction(context) { ctx =>
      if ((dvid != null && latest != null) || (dvid == null && latest == null)) {
        throw new BadRequestException("Specify exactly one: dvid=<ID> OR latest=true")
      }

      // Check read access and download permission
      val uid = user.getUid
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      // Retrieve dataset and check download permission
      val dataset = getDatasetByID(ctx, did)
      // Non-owners can download if dataset is downloadable and they have read access
      if (!userOwnDataset(ctx, did, uid) && !dataset.getIsDownloadable) {
        throw new ForbiddenException("Dataset download is not allowed")
      }

      // Determine which version to retrieve
      val datasetVersion = if (dvid != null) {
        getDatasetVersionByID(ctx, dvid)
      } else if (java.lang.Boolean.TRUE.equals(latest)) {
        getLatestDatasetVersion(ctx, did).getOrElse(
          throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
        )
      } else {
        throw new BadRequestException("Invalid parameters")
      }

      // Retrieve dataset and version details
      val datasetName = dataset.getName
      val repositoryName = dataset.getRepositoryName
      val versionHash = datasetVersion.getVersionHash
      val objects = LakeFSStorageClient.retrieveObjectsOfVersion(repositoryName, versionHash)

      if (objects.isEmpty) {
        return Response
          .status(Response.Status.NOT_FOUND)
          .entity(s"No objects found in version $versionHash of repository $repositoryName")
          .build()
      }

      // StreamingOutput for ZIP download
      val streamingOutput = new StreamingOutput {
        override def write(outputStream: OutputStream): Unit = {
          val zipOut = new ZipOutputStream(outputStream)
          try {
            objects.foreach { obj =>
              val filePath = obj.getPath
              val file = LakeFSStorageClient.getFileFromRepo(repositoryName, versionHash, filePath)

              zipOut.putNextEntry(new ZipEntry(filePath))
              Files.copy(Paths.get(file.toURI), zipOut)
              zipOut.closeEntry()
            }
          } finally {
            zipOut.close()
          }
        }
      }

      val zipFilename = s"""attachment; filename="$datasetName-${datasetVersion.getName}.zip""""

      Response
        .ok(streamingOutput, "application/zip")
        .header("Content-Disposition", zipFilename)
        .build()
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/{dvid}/rootFileNodes")
  def retrieveDatasetVersionRootFileNodes(
      @PathParam("did") did: Integer,
      @PathParam("dvid") dvid: Integer,
      @Auth user: SessionUser
  ): DatasetVersionRootFileNodesResponse = {
    val uid = user.getUid
    withTransaction(context)(ctx => fetchDatasetVersionRootFileNodes(ctx, did, dvid, Some(uid)))
  }

  @GET
  @Path("/{did}/publicVersion/{dvid}/rootFileNodes")
  def retrievePublicDatasetVersionRootFileNodes(
      @PathParam("did") did: Integer,
      @PathParam("dvid") dvid: Integer
  ): DatasetVersionRootFileNodesResponse = {
    withTransaction(context)(ctx => fetchDatasetVersionRootFileNodes(ctx, did, dvid, None))
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}")
  def getDataset(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): DashboardDataset = {
    val uid = user.getUid
    withTransaction(context)(ctx => getDashboardDataset(ctx, did, Some(uid)))
  }

  @GET
  @Path("/public/{did}")
  def getPublicDataset(
      @PathParam("did") did: Integer
  ): DashboardDataset = {
    withTransaction(context)(ctx => getDashboardDataset(ctx, did, None))
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
    * This method returns all owner user names of the dataset that the user has access to
    *
    * @return OwnerName[]
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/user-dataset-owners")
  def retrieveOwners(@Auth user: SessionUser): util.List[String] = {
    context
      .selectDistinct(USER.EMAIL)
      .from(USER)
      .join(DATASET)
      .on(DATASET.OWNER_UID.eq(USER.UID))
      .join(DATASET_USER_ACCESS)
      .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
      .where(DATASET_USER_ACCESS.UID.eq(user.getUid))
      .fetchInto(classOf[String])
  }

  /**
    * Validates the dataset name.
    *
    * Rules:
    * - Must be at least 1 character long.
    * - Only lowercase letters, numbers, underscores, and hyphens are allowed.
    * - Cannot start with a hyphen.
    *
    * @param name The dataset name to validate.
    * @throws IllegalArgumentException if the name is invalid.
    */
  private def validateDatasetName(name: String): Unit = {
    val datasetNamePattern = "^[A-Za-z0-9_-]+$".r
    if (!datasetNamePattern.matches(name)) {
      throw new IllegalArgumentException(
        s"Invalid dataset name: '$name'. " +
          "Dataset names must be at least 1 character long and " +
          "contain only lowercase letters, numbers, underscores, and hyphens, " +
          "and cannot start with a hyphen."
      )
    }
  }

  private def fetchDatasetVersions(ctx: DSLContext, did: Integer): List[DatasetVersion] = {
    ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(DATASET_VERSION.CREATION_TIME.desc()) // Change to .asc() for ascending order
      .fetchInto(classOf[DatasetVersion])
      .asScala
      .toList
  }

  private def fetchDatasetVersionRootFileNodes(
      ctx: DSLContext,
      did: Integer,
      dvid: Integer,
      uid: Option[Integer]
  ): DatasetVersionRootFileNodesResponse = {
    val dataset = getDashboardDataset(ctx, did, uid)
    val datasetVersion = getDatasetVersionByID(ctx, dvid)
    val datasetName = dataset.dataset.getName
    val repositoryName = dataset.dataset.getRepositoryName

    val ownerFileNode = DatasetFileNode
      .fromLakeFSRepositoryCommittedObjects(
        Map(
          (dataset.ownerEmail, datasetName, datasetVersion.getName) -> LakeFSStorageClient
            .retrieveObjectsOfVersion(repositoryName, datasetVersion.getVersionHash)
        )
      )
      .head

    DatasetVersionRootFileNodesResponse(
      ownerFileNode.children.get
        .find(_.getName == datasetName)
        .head
        .children
        .get
        .find(_.getName == datasetVersion.getName)
        .head
        .children
        .get,
      DatasetFileNode.calculateTotalSize(List(ownerFileNode))
    )
  }

  private def generatePresignedResponse(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Response = {
    resolveDatasetAndPath(encodedUrl, repositoryName, commitHash, uid) match {
      case Left(errorResponse) =>
        errorResponse

      case Right((resolvedRepositoryName, resolvedCommitHash, resolvedFilePath)) =>
        val url = LakeFSStorageClient.getFilePresignedUrl(
          resolvedRepositoryName,
          resolvedCommitHash,
          resolvedFilePath
        )

        Response.ok(Map("presignedUrl" -> url)).build()
    }
  }

  private def resolveDatasetAndPath(
      encodedUrl: String,
      repositoryName: String,
      commitHash: String,
      uid: Integer
  ): Either[Response, (String, String, String)] = {
    val decodedPathStr = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name())

    (Option(repositoryName), Option(commitHash)) match {
      case (Some(_), None) | (None, Some(_)) =>
        // Case 1: Only one parameter is provided (error case)
        Left(
          Response
            .status(Response.Status.BAD_REQUEST)
            .entity(
              "Both repositoryName and commitHash must be provided together, or neither should be provided."
            )
            .build()
        )

      case (Some(repositoryName), Some(commit)) =>
        // Case 2: repositoryName and commitHash are provided, validate access
        val response = withTransaction(context) { ctx =>
          val datasetDao = new DatasetDao(ctx.configuration())
          val datasets = datasetDao.fetchByRepositoryName(repositoryName).asScala.toList

          if (datasets.isEmpty || !userHasReadAccess(ctx, datasets.head.getDid, uid))
            throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)

          val dataset = datasets.head
          // Standard read access check only - download restrictions handled per endpoint
          // Non-download operations (viewing) should work for all public datasets

          (repositoryName, commit, decodedPathStr)
        }
        Right(response)

      case (None, None) =>
        // Case 3: Neither repositoryName nor commitHash are provided, resolve normally
        val response = withTransaction(context) { ctx =>
          val fileUri = FileResolver.resolve(decodedPathStr)
          val document = DocumentFactory.openReadonlyDocument(fileUri).asInstanceOf[OnDataset]
          val datasetDao = new DatasetDao(ctx.configuration())
          val datasets =
            datasetDao.fetchByRepositoryName(document.getRepositoryName()).asScala.toList

          if (datasets.isEmpty || !userHasReadAccess(ctx, datasets.head.getDid, uid))
            throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)

          val dataset = datasets.head
          // Standard read access check only - download restrictions handled per endpoint
          // Non-download operations (viewing) should work for all public datasets

          (
            document.getRepositoryName(),
            document.getVersionHash(),
            document.getFileRelativePath()
          )
        }
        Right(response)
    }
  }
  // === Multipart helpers (stateless, token-based) ===
  /**
    * Stateless uploadToken:
    *   inner format: uploadId|did|uid|filePathB64
    *   outer: base64-url of that string
    */
  private def buildUploadToken(
      did: Int,
      uid: Int,
      filePath: String,
      uploadId: String
  ): String = {
    val filePathB64 = java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(filePath.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    val raw = s"$uploadId|$did|$uid|$filePathB64"

    java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  }
  private case class DecodedUploadToken(
      uploadId: String,
      did: Int,
      uid: Int,
      filePath: String
  )

  private def parseUploadToken(token: String): DecodedUploadToken = {
    val raw = new String(
      java.util.Base64.getUrlDecoder.decode(token),
      java.nio.charset.StandardCharsets.UTF_8
    )

    // uploadId|did|uid|filePathB64
    val parts = raw.split("\\|", 4)
    if (parts.length != 4)
      throw new BadRequestException("Invalid uploadToken format")

    val filePath = new String(
      java.util.Base64.getUrlDecoder
        .decode(parts(3)),
      java.nio.charset.StandardCharsets.UTF_8
    )

    DecodedUploadToken(
      uploadId = parts(0),
      did = parts(1).toInt,
      uid = parts(2).toInt,
      filePath = filePath
    )
  }

  /**
    * Given a decoded token and current authenticated user, rediscover the
    * correct (dataset, key, uploadId) in S3/MinIO using uploadId directly.
    */
  private def findMultipartUploadForToken(
      token: DecodedUploadToken,
      currentUid: Int
  ): (Dataset, String, String) = {
    if (token.uid != currentUid) {
      throw new ForbiddenException("User has no access to this upload")
    }

    // 1) Check dataset and permissions
    val dataset = withTransaction(context) { ctx =>
      val ds = getDatasetByID(ctx, token.did)
      if (!userHasWriteAccess(ctx, token.did, currentUid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      ds
    }

    val bucket = StorageConfig.lakefsBucketName

    // 2) List all multipart uploads under this repo prefix and match by uploadId
    val uploads = S3StorageClient.listAllMultipartUploads(bucket, None)

    val candidates = uploads.filter(_.uploadId == token.uploadId)

    if (candidates.isEmpty) {
      throw new NotFoundException("No active multipart upload found for token")
    }
    if (candidates.size > 1) {
      throw new WebApplicationException(
        "Ambiguous multipart upload for token",
        Response.Status.CONFLICT
      )
    }

    val u = candidates.head
    (dataset, u.key, u.uploadId)
  }

  /**
    * Initialize a multipart upload for a given dataset + logical file path.
    *
    * Keeps the HTTP API the same but:
    *  - ignores numParts
    *  - does not use any presigned URLs from lakeFS
    *  - returns a stateless uploadToken instead of DB-backed session
    */
  private def initMultipartUpload(
      ownerEmail: String,
      datasetName: String,
      encodedUrl: String,
      numParts: Optional[Integer],
      uid: Int
  ): Response = {
    withTransaction(context) { ctx =>
      val dataset = ctx
        .select(DATASET.fields: _*)
        .from(DATASET)
        .leftJoin(USER)
        .on(USER.UID.eq(DATASET.OWNER_UID))
        .where(USER.EMAIL.eq(ownerEmail))
        .and(DATASET.NAME.eq(datasetName))
        .fetchOneInto(classOf[Dataset])

      if (dataset == null || !userHasWriteAccess(ctx, dataset.getDid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val repositoryName = dataset.getRepositoryName
      val filePath = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name())

      // We do NOT care about numParts or initial presigned URLs.
      // We only need uploadId + physicalAddress.
      val presign =
        LakeFSStorageClient.initiatePresignedMultipartUploads(repositoryName, filePath, 1)

      val uploadIdStr = presign.getUploadId

      val token =
        buildUploadToken(dataset.getDid.intValue(), uid, filePath, uploadIdStr)

      Response
        .ok(
          Map(
            "uploadToken" -> token
          )
        )
        .build()
    }
  }

  /**
    * Complete a multipart upload:
    *  - token -> dataset + (key, uploadId) via S3 multipart listing
    *  - list parts from S3 (ListParts)
    *  - call lakeFS completePresignMultipartUpload with physicalAddress
    */
  private def finishMultipartUpload(
      payload: Map[String, Any],
      uid: Int
  ): Response = {
    val tokenValueStr = payload
      .get("uploadToken")
      .map(_.asInstanceOf[String])
      .getOrElse {
        throw new BadRequestException("uploadToken is required for completion")
      }

    val decoded = parseUploadToken(tokenValueStr)
    val (dataset, key, uploadId) = findMultipartUploadForToken(decoded, uid)

    val bucket = StorageConfig.lakefsBucketName
    val partInfos =
      S3StorageClient.listAllParts(bucket, key, uploadId)

    if (partInfos.isEmpty) {
      throw new BadRequestException("No uploaded parts found for this upload")
    }

    val partsList: List[(Int, String)] =
      partInfos.map(pi => (pi.partNumber, pi.eTag)).toList

    val physicalAddress =
      s"${StorageConfig.lakefsBlockStorageType}://${bucket}/${key}"

    val objectStats = LakeFSStorageClient.completePresignedMultipartUploads(
      dataset.getRepositoryName,
      decoded.filePath,
      uploadId,
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
  }

  /**
    * Abort a multipart upload:
    *  - token -> dataset + (key, uploadId)
    *  - abort multipart in S3
    *  - abort in lakeFS
    */
  private def abortMultipartUpload(
      payload: Map[String, Any],
      uid: Int
  ): Response = {
    val tokenValueStr = payload
      .get("uploadToken")
      .map(_.asInstanceOf[String])
      .getOrElse {
        throw new BadRequestException("uploadToken is required for abortion")
      }

    val decoded = parseUploadToken(tokenValueStr)
    val (dataset, key, uploadId) = findMultipartUploadForToken(decoded, uid)

    val bucket = StorageConfig.lakefsBucketName

    val physicalAddress =
      s"${StorageConfig.lakefsBlockStorageType}://${bucket}/${key}"

    LakeFSStorageClient.abortPresignedMultipartUploads(
      dataset.getRepositoryName,
      decoded.filePath,
      uploadId,
      physicalAddress
    )

    Response.ok(Map("message" -> "Multipart upload aborted successfully")).build()
  }
}
