package edu.uci.ics.texera.service.util

import software.amazon.awssdk.services.s3.model.{
  AbortMultipartUploadRequest,
  CompleteMultipartUploadRequest,
  CompletedMultipartUpload,
  CompletedPart,
  CreateMultipartUploadRequest,
  DeleteObjectRequest,
  UploadPartRequest
}
import edu.uci.ics.amber.core.storage.StorageConfig

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import scala.util.{Failure, Try}

/**
  * A utility class for managing large binary files in S3 storage, including upload, reference counting, and cleanup.
  * This class provides a centralized way to handle large binary file operations in S3 with proper error handling
  * and type safety.
  */
object S3LargeBinaryManager {

  private val BUFFER_SIZE = 8192
  private val MIN_PART_SIZE = 5 * 1024 * 1024 // 5MB minimum part size for S3 multipart upload

  /**
    * Uploads a file to S3 and initializes its reference count
    *
    * @param inputStream The input stream of the file to upload
    * @return The S3 URI of the uploaded file
    */
  def uploadFile(inputStream: InputStream): Try[String] = {
    val bucketName = StorageConfig.s3LargeBinaryBucketName
    val key = s"${java.util.UUID.randomUUID()}"

    Try {
      // Wrap the input stream in a BufferedInputStream for better performance
      val bufferedStream = new BufferedInputStream(inputStream, BUFFER_SIZE)
      try {
        // Initialize multipart upload
        val createMultipartUploadRequest = CreateMultipartUploadRequest
          .builder()
          .bucket(bucketName)
          .key(key)
          .build()

        val createMultipartUploadResponse =
          S3StorageClient.getS3Client.createMultipartUpload(createMultipartUploadRequest)
        val uploadId = createMultipartUploadResponse.uploadId()

        try {
          // Calculate part size (minimum 5MB per part)
          val partSize = MIN_PART_SIZE
          var partNumber = 1
          val completedParts = new java.util.ArrayList[CompletedPart]()
          val buffer = new Array[Byte](BUFFER_SIZE)
          var currentPartBytes = 0L
          var totalBytes = 0L

          // Create a streaming request body for the current part
          val partStream = new java.io.ByteArrayOutputStream()

          var bytesRead = bufferedStream.read(buffer)
          while (bytesRead != -1) {
            partStream.write(buffer, 0, bytesRead)
            currentPartBytes += bytesRead
            totalBytes += bytesRead

            // If we've reached the part size, upload this part
            if (currentPartBytes >= partSize) {
              val partData = partStream.toByteArray
              val uploadPartRequest = UploadPartRequest
                .builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build()

              val requestBody = software.amazon.awssdk.core.sync.RequestBody.fromBytes(partData)
              val uploadPartResponse =
                S3StorageClient.getS3Client.uploadPart(uploadPartRequest, requestBody)

              val completedPart = CompletedPart
                .builder()
                .partNumber(partNumber)
                .eTag(uploadPartResponse.eTag())
                .build()

              completedParts.add(completedPart)
              partNumber += 1
              currentPartBytes = 0
              partStream.reset()
            }
            bytesRead = bufferedStream.read(buffer)
          }

          // Upload the last part if there's any remaining data
          if (currentPartBytes > 0) {
            val partData = partStream.toByteArray
            val uploadPartRequest = UploadPartRequest
              .builder()
              .bucket(bucketName)
              .key(key)
              .uploadId(uploadId)
              .partNumber(partNumber)
              .build()

            val requestBody = software.amazon.awssdk.core.sync.RequestBody.fromBytes(partData)
            val uploadPartResponse =
              S3StorageClient.getS3Client.uploadPart(uploadPartRequest, requestBody)

            val completedPart = CompletedPart
              .builder()
              .partNumber(partNumber)
              .eTag(uploadPartResponse.eTag())
              .build()

            completedParts.add(completedPart)
          }

          // Complete multipart upload
          val completeMultipartUploadRequest = CompleteMultipartUploadRequest
            .builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
            .build()

          S3StorageClient.getS3Client.completeMultipartUpload(completeMultipartUploadRequest)
          S3ReferenceCounter.initializeReferenceCount(bucketName, key)
          s"s3://$bucketName/$key"
        } catch {
          case e: Exception =>
            // Abort multipart upload on failure
            val abortMultipartUploadRequest = AbortMultipartUploadRequest
              .builder()
              .bucket(bucketName)
              .key(key)
              .uploadId(uploadId)
              .build()
            S3StorageClient.getS3Client.abortMultipartUpload(abortMultipartUploadRequest)
            throw e
        }
      } finally {
        bufferedStream.close()
      }
    }
  }

  /**
    * Increments the reference count for an S3 object
    *
    * @param s3Uri The S3 URI of the object
    * @return Success if the operation was successful, Failure otherwise
    */
  def incrementReferenceCount(s3Uri: String): Try[Unit] = {
    if (!s3Uri.startsWith("s3://")) {
      return Failure(new IllegalArgumentException("Invalid S3 URI format"))
    }

    Try {
      val uri = new URI(s3Uri)
      val bucketName = uri.getHost
      val key = uri.getPath.stripPrefix("/")
      S3ReferenceCounter.incrementReferenceCount(bucketName, key)
    }
  }

  /**
    * Decrements the reference count for an S3 object and deletes it if the count reaches zero
    *
    * @param s3Uri The S3 URI of the object
    * @return Success with the new reference count if successful, Failure otherwise
    */
  def decrementReferenceCount(s3Uri: String): Try[Long] = {
    if (!s3Uri.startsWith("s3://")) {
      return Failure(new IllegalArgumentException("Invalid S3 URI format"))
    }

    Try {
      val uri = new URI(s3Uri)
      val bucketName = uri.getHost
      val key = uri.getPath.stripPrefix("/")
      val newCount = S3ReferenceCounter.decrementReferenceCount(bucketName, key)

      if (newCount == 0) {
        S3StorageClient.getS3Client.deleteObject(
          DeleteObjectRequest
            .builder()
            .bucket(bucketName)
            .key(key)
            .build()
        )
      }
      newCount
    }
  }
}
