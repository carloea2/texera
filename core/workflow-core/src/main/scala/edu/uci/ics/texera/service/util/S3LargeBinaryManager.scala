package edu.uci.ics.texera.service.util

import software.amazon.awssdk.services.s3.model.{
  AbortMultipartUploadRequest,
  CompleteMultipartUploadRequest,
  CompletedMultipartUpload,
  CompletedPart,
  CreateMultipartUploadRequest,
  DeleteObjectRequest,
  UploadPartRequest,
  GetObjectRequest,
  PutObjectRequest
}
import edu.uci.ics.amber.core.storage.StorageConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import software.amazon.awssdk.core.sync.RequestBody

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import scala.collection.mutable

/**
  * A utility class for managing large binary files in S3 storage, including upload, reference counting, and cleanup.
  * This class provides a centralized way to handle large binary file operations in S3 with proper error handling
  * and type safety.
  */
object S3LargeBinaryManager {

  private val BUFFER_SIZE = 8192
  private val MIN_PART_SIZE = 5 * 1024 * 1024 // 5MB minimum part size for S3 multipart upload
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val referenceCountKey = "reference-counts.json"

  /**
    * Uploads a file to S3 and initializes its reference count
    *
    * @param inputStream The input stream of the file to upload
    * @return The S3 URI of the uploaded file
    */
  def uploadFile(inputStream: InputStream): String = {
    val bucketName = StorageConfig.s3LargeBinaryBucketName
    val key = s"${java.util.UUID.randomUUID()}"

    val bufferedStream = new BufferedInputStream(inputStream, BUFFER_SIZE)
    try {
      // Initialize multipart upload
      val createMultipartUploadResponse = S3StorageClient.getS3Client.createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build()
      )
      val uploadId = createMultipartUploadResponse.uploadId()

      try {
        val partSize = MIN_PART_SIZE
        var partNumber = 1
        val completedParts = new java.util.ArrayList[CompletedPart]()
        val buffer = new Array[Byte](BUFFER_SIZE)
        var currentPartBytes = 0L
        var totalBytes = 0L
        val partStream = new java.io.ByteArrayOutputStream()

        var bytesRead = bufferedStream.read(buffer)
        while (bytesRead != -1) {
          partStream.write(buffer, 0, bytesRead)
          currentPartBytes += bytesRead
          totalBytes += bytesRead

          if (currentPartBytes >= partSize) {
            val partData = partStream.toByteArray
            val uploadPartResponse = S3StorageClient.getS3Client.uploadPart(
              UploadPartRequest
                .builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build(),
              RequestBody.fromBytes(partData)
            )

            completedParts.add(
              CompletedPart
                .builder()
                .partNumber(partNumber)
                .eTag(uploadPartResponse.eTag())
                .build()
            )
            partNumber += 1
            currentPartBytes = 0
            partStream.reset()
          }
          bytesRead = bufferedStream.read(buffer)
        }

        // Upload the last part if there's any remaining data
        if (currentPartBytes > 0) {
          val partData = partStream.toByteArray
          val uploadPartResponse = S3StorageClient.getS3Client.uploadPart(
            UploadPartRequest
              .builder()
              .bucket(bucketName)
              .key(key)
              .uploadId(uploadId)
              .partNumber(partNumber)
              .build(),
            RequestBody.fromBytes(partData)
          )

          completedParts.add(
            CompletedPart
              .builder()
              .partNumber(partNumber)
              .eTag(uploadPartResponse.eTag())
              .build()
          )
        }

        // Complete multipart upload
        S3StorageClient.getS3Client.completeMultipartUpload(
          CompleteMultipartUploadRequest
            .builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
            .build()
        )

        // Initialize reference count
        val currentCounts = loadReferenceCounts(bucketName)
        currentCounts.put(key, 0)
        saveReferenceCounts(bucketName, currentCounts)

        s"s3://$bucketName/$key"
      } catch {
        case e: Exception =>
          // Abort multipart upload on failure
          S3StorageClient.getS3Client.abortMultipartUpload(
            AbortMultipartUploadRequest
              .builder()
              .bucket(bucketName)
              .key(key)
              .uploadId(uploadId)
              .build()
          )
          throw e
      }
    } finally {
      bufferedStream.close()
    }
  }

  /**
    * Increments the reference count for an S3 object
    *
    * @param s3Uri The S3 URI of the object
    */
  def incrementReferenceCount(s3Uri: String): Unit = {
    if (!s3Uri.startsWith("s3://")) {
      throw new IllegalArgumentException("Invalid S3 URI format")
    }

    val uri = new URI(s3Uri)
    val bucketName = uri.getHost
    val key = uri.getPath.stripPrefix("/")

    val currentCounts = loadReferenceCounts(bucketName)
    val newCount = currentCounts.getOrElse(key, 0) + 1
    currentCounts.put(key, newCount)
    saveReferenceCounts(bucketName, currentCounts)
  }

  /**
    * Decrements the reference count for an S3 object and deletes it if the count reaches zero
    *
    * @param s3Uri The S3 URI of the object
    * @return The new reference count
    */
  def decrementReferenceCount(s3Uri: String): Long = {
    if (!s3Uri.startsWith("s3://")) {
      throw new IllegalArgumentException("Invalid S3 URI format")
    }

    val uri = new URI(s3Uri)
    val bucketName = uri.getHost
    val key = uri.getPath.stripPrefix("/")

    val currentCounts = loadReferenceCounts(bucketName)
    val currentCount = currentCounts.getOrElse(key, 0)
    val newCount = Math.max(0, currentCount - 1)
    currentCounts.put(key, newCount)
    saveReferenceCounts(bucketName, currentCounts)

    if (newCount == 0) {
      S3StorageClient.getS3Client.deleteObject(
        DeleteObjectRequest.builder().bucket(bucketName).key(key).build()
      )
    }
    newCount
  }

  private def loadReferenceCounts(bucketName: String): mutable.Map[String, Int] = {
    try {
      val response = S3StorageClient.getS3Client.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build()
      )
      val bytes = response.readAllBytes()
      if (bytes.isEmpty) mutable.Map[String, Int]()
      else objectMapper.readValue(bytes, classOf[mutable.Map[String, Int]])
    } catch {
      case _: Exception => mutable.Map[String, Int]()
    }
  }

  private def saveReferenceCounts(bucketName: String, counts: mutable.Map[String, Int]): Unit = {
    val jsonBytes = objectMapper.writeValueAsBytes(counts)
    S3StorageClient.getS3Client.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build(),
      RequestBody.fromBytes(jsonBytes)
    )
  }
}
