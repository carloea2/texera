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
import scala.util.{Failure, Success, Try}

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
    * Initialize reference count for an object to 0
    * @param bucketName The S3 bucket name
    * @param objectKey The key of the object to initialize
    */
  private def initializeReferenceCount(bucketName: String, objectKey: String): Unit = {
    synchronized {
      val currentCounts = loadReferenceCounts(bucketName)
      currentCounts.put(objectKey, 0)
      saveReferenceCounts(bucketName, currentCounts)
    }
  }

  /**
    * Increment reference count for an object
    * @param bucketName The S3 bucket name
    * @param objectKey The key of the object to increment
    * @return The new reference count
    */
  private def incrementReferenceCount(bucketName: String, objectKey: String): Int = {
    synchronized {
      val currentCounts = loadReferenceCounts(bucketName)
      val newCount = currentCounts.getOrElse(objectKey, 0) + 1
      currentCounts.put(objectKey, newCount)
      saveReferenceCounts(bucketName, currentCounts)
      newCount
    }
  }

  /**
    * Decrement reference count for an object
    * @param bucketName The S3 bucket name
    * @param objectKey The key of the object to decrement
    * @return The new reference count
    */
  private def decrementReferenceCount(bucketName: String, objectKey: String): Int = {
    synchronized {
      val currentCounts = loadReferenceCounts(bucketName)
      val currentCount = currentCounts.getOrElse(objectKey, 0)
      val newCount = Math.max(0, currentCount - 1)
      currentCounts.put(objectKey, newCount)
      saveReferenceCounts(bucketName, currentCounts)
      newCount
    }
  }

  /**
    * Get current reference count for an object
    * @param bucketName The S3 bucket name
    * @param objectKey The key of the object to check
    * @return The current reference count
    */
  private def getReferenceCount(bucketName: String, objectKey: String): Int = {
    synchronized {
      val currentCounts = loadReferenceCounts(bucketName)
      currentCounts.getOrElse(objectKey, 0)
    }
  }

  private def loadReferenceCounts(bucketName: String): mutable.Map[String, Int] = {
    Try {
      val response = S3StorageClient.getS3Client.getObject(
        GetObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build()
      )
      val bytes = response.readAllBytes()
      if (bytes.isEmpty) {
        mutable.Map[String, Int]()
      } else {
        objectMapper.readValue(bytes, classOf[mutable.Map[String, Int]])
      }
    } match {
      case Success(counts) => counts
      case Failure(_)      => mutable.Map[String, Int]()
    }
  }

  private def saveReferenceCounts(
      bucketName: String,
      counts: mutable.Map[String, Int]
  ): Unit = {
    val jsonBytes = objectMapper.writeValueAsBytes(counts)
    S3StorageClient.getS3Client.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(referenceCountKey).build(),
      RequestBody.fromBytes(jsonBytes)
    )
  }

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
          initializeReferenceCount(bucketName, key)
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
      incrementReferenceCount(bucketName, key)
    }
  }

  /**
    * Decrements the reference count for an S3 object and deletes it if the count reaches zero
    *
    * @param s3Uri The S3 URI of the object
    * @return The new reference count if successful, throws exception otherwise
    */
  def decrementReferenceCount(s3Uri: String): Long = {
    if (!s3Uri.startsWith("s3://")) {
      throw new IllegalArgumentException("Invalid S3 URI format")
    }

    try {
      val uri = new URI(s3Uri)
      val bucketName = uri.getHost
      val key = uri.getPath.stripPrefix("/")
      val newCount = decrementReferenceCount(bucketName, key)

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
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to decrement reference count for $s3Uri: ${e.getMessage}",
          e
        )
    }
  }
}
