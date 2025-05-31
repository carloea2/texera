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
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer

/**
  * A utility class for managing large binary files in S3 storage, including upload, reference counting, and cleanup.
  * This class provides a centralized way to handle large binary file operations in S3 with proper error handling
  * and type safety.
  */
object S3LargeBinaryManager {

  private val BUFFER_SIZE = 1024 * 1024 // 1MB buffer size
  private val MIN_PART_SIZE = 10 * 1024 * 1024 // 10MB minimum part size for S3 multipart upload
  private val MAX_CONCURRENT_UPLOADS = 10 // Maximum number of concurrent uploads
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val REFERENCE_COUNT_PREFIX = "reference-counts/"

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
    var uploadId: String = null

    try {
      // Initialize multipart upload
      val createMultipartUploadResponse = S3StorageClient.getS3Client.createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(bucketName).key(key).build()
      )
      uploadId = createMultipartUploadResponse.uploadId()

      val partSize = MIN_PART_SIZE
      var partNumber = 1
      val completedParts = new ConcurrentHashMap[Int, CompletedPart]()
      val buffer = new Array[Byte](BUFFER_SIZE)
      var currentPartBytes = 0L
      var totalBytes = 0L

      // Create a temporary file for the current part
      var currentTempFile = java.io.File.createTempFile("s3-part-", ".tmp")
      var partOutputStream = new java.io.FileOutputStream(currentTempFile)
      var previousTempFile: java.io.File = null
      val uploadFutures = new ArrayBuffer[Future[Unit]]()

      try {
        var bytesRead = bufferedStream.read(buffer)
        while (bytesRead != -1) {
          partOutputStream.write(buffer, 0, bytesRead)
          currentPartBytes += bytesRead
          totalBytes += bytesRead

          if (currentPartBytes >= partSize) {
            partOutputStream.close()

            // Create a new temporary file for the next part
            previousTempFile = currentTempFile
            currentTempFile = java.io.File.createTempFile("s3-part-", ".tmp")
            partOutputStream = new java.io.FileOutputStream(currentTempFile)

            // Upload the part asynchronously
            val currentPartNumber = partNumber
            val currentPartFile = previousTempFile
            val future: Future[Unit] = Future {
              val uploadPartResponse = S3StorageClient.getS3Client.uploadPart(
                UploadPartRequest
                  .builder()
                  .bucket(bucketName)
                  .key(key)
                  .uploadId(uploadId)
                  .partNumber(currentPartNumber)
                  .build(),
                RequestBody.fromFile(currentPartFile)
              )

              completedParts.put(
                currentPartNumber,
                CompletedPart
                  .builder()
                  .partNumber(currentPartNumber)
                  .eTag(uploadPartResponse.eTag())
                  .build()
              )

              // Clean up the temporary file
              if (currentPartFile.exists()) {
                currentPartFile.delete()
              }
              () // Explicit Unit return
            }
            uploadFutures += future

            // Wait if we've reached the maximum number of concurrent uploads
            if (uploadFutures.size >= MAX_CONCURRENT_UPLOADS) {
              Await.result(Future.sequence(uploadFutures), Duration.Inf)
              uploadFutures.clear()
            }

            partNumber += 1
            currentPartBytes = 0
          }
          bytesRead = bufferedStream.read(buffer)
        }

        // Upload the last part if there's any remaining data
        if (currentPartBytes > 0) {
          partOutputStream.close()
          val currentPartNumber = partNumber
          val currentPartFile = currentTempFile
          val future: Future[Unit] = Future {
            val uploadPartResponse = S3StorageClient.getS3Client.uploadPart(
              UploadPartRequest
                .builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(currentPartNumber)
                .build(),
              RequestBody.fromFile(currentPartFile)
            )

            completedParts.put(
              currentPartNumber,
              CompletedPart
                .builder()
                .partNumber(currentPartNumber)
                .eTag(uploadPartResponse.eTag())
                .build()
            )

            // Clean up the temporary file
            if (currentPartFile.exists()) {
              currentPartFile.delete()
            }
            () // Explicit Unit return
          }
          uploadFutures += future
        }

        // Wait for all remaining uploads to complete
        if (uploadFutures.nonEmpty) {
          Await.result(Future.sequence(uploadFutures), Duration.Inf)
        }

        // Complete multipart upload
        val sortedParts = new java.util.ArrayList[CompletedPart]()
        for (i <- 1 to partNumber) {
          val part = completedParts.get(i)
          if (part != null) {
            sortedParts.add(part)
          }
        }

        S3StorageClient.getS3Client.completeMultipartUpload(
          CompleteMultipartUploadRequest
            .builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(sortedParts).build())
            .build()
        )

        // Initialize reference count
        val referenceCountKey = s"$REFERENCE_COUNT_PREFIX$key"
        saveReferenceCount(bucketName, referenceCountKey, 0)

        s"s3://$bucketName/$key"
      } catch {
        case e: Exception =>
          // Abort multipart upload on failure
          if (uploadId != null) {
            S3StorageClient.getS3Client.abortMultipartUpload(
              AbortMultipartUploadRequest
                .builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .build()
            )
          }
          throw e
      } finally {
        try {
          partOutputStream.close()
        } catch {
          case _: Exception => // Ignore close errors
        }
        // Clean up all temporary files
        if (currentTempFile != null && currentTempFile.exists()) {
          currentTempFile.delete()
        }
        if (previousTempFile != null && previousTempFile.exists()) {
          previousTempFile.delete()
        }
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
    val referenceCountKey = s"$REFERENCE_COUNT_PREFIX$key"

    val currentCount = loadReferenceCount(bucketName, referenceCountKey)
    val newCount = currentCount + 1
    saveReferenceCount(bucketName, referenceCountKey, newCount)
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
    val referenceCountKey = s"$REFERENCE_COUNT_PREFIX$key"

    val currentCount = loadReferenceCount(bucketName, referenceCountKey)
    val newCount = Math.max(0, currentCount - 1)
    saveReferenceCount(bucketName, referenceCountKey, newCount)

    if (newCount == 0) {
      S3StorageClient.getS3Client.deleteObject(
        DeleteObjectRequest.builder().bucket(bucketName).key(key).build()
      )
      // Also delete the reference count file when the object is deleted
      S3StorageClient.getS3Client.deleteObject(
        DeleteObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build()
      )
    }
    newCount
  }

  private def loadReferenceCount(bucketName: String, referenceCountKey: String): Int = {
    try {
      val response = S3StorageClient.getS3Client.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build()
      )
      val bytes = response.readAllBytes()
      if (bytes.isEmpty) 0
      else objectMapper.readValue(bytes, classOf[Int])
    } catch {
      case _: Exception => 0
    }
  }

  private def saveReferenceCount(
      bucketName: String,
      referenceCountKey: String,
      count: Int
  ): Unit = {
    val jsonBytes = objectMapper.writeValueAsBytes(count)
    S3StorageClient.getS3Client.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build(),
      RequestBody.fromBytes(jsonBytes)
    )
  }
}
