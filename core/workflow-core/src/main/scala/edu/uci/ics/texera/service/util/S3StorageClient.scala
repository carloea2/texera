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

package edu.uci.ics.texera.service.util

import edu.uci.ics.amber.core.storage.StorageConfig
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model._

import java.security.MessageDigest
import scala.jdk.CollectionConverters._
import java.io.InputStream

/**
  * S3Storage provides an abstraction for S3-compatible storage (e.g., MinIO).
  * - Uses credentials and endpoint from StorageConfig.
  * - Supports object upload, download, listing, and deletion.
  */
object S3StorageClient {
  val MINIMUM_NUM_OF_MULTIPART_S3_PART: Long = 5L * 1024 * 1024 // 5 MiB
  val MAXIMUM_NUM_OF_MULTIPART_S3_PARTS = 10_000

  // Initialize MinIO-compatible S3 Client
  private lazy val s3Client: S3Client = {
    val credentials = AwsBasicCredentials.create(StorageConfig.s3Username, StorageConfig.s3Password)
    S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .region(Region.of(StorageConfig.s3Region))
      .endpointOverride(java.net.URI.create(StorageConfig.s3Endpoint)) // MinIO URL
      .serviceConfiguration(
        S3Configuration.builder().pathStyleAccessEnabled(true).build()
      )
      .build()
  }

  /**
    * Get the S3 client instance
    * @return The S3 client
    */
  def getS3Client: S3Client = s3Client

  /**
    * Checks if a directory (prefix) exists within an S3 bucket.
    *
    * @param bucketName The bucket name.
    * @param directoryPrefix The directory (prefix) to check (must end with `/`).
    * @return True if the directory contains at least one object, False otherwise.
    */
  def directoryExists(bucketName: String, directoryPrefix: String): Boolean = {
    // Ensure the prefix ends with `/` to correctly match directories
    val normalizedPrefix =
      if (directoryPrefix.endsWith("/")) directoryPrefix else directoryPrefix + "/"

    val listRequest = ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .prefix(normalizedPrefix)
      .maxKeys(1) // Only check if at least one object exists
      .build()

    val listResponse = s3Client.listObjectsV2(listRequest)
    !listResponse.contents().isEmpty // If contents exist, directory exists
  }

  /**
    * Creates an S3 bucket if it does not already exist.
    *
    * @param bucketName The name of the bucket to create.
    */
  def createBucketIfNotExist(bucketName: String): Unit = {
    try {
      // Check if the bucket already exists
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
    } catch {
      case _: NoSuchBucketException | _: S3Exception =>
        // If the bucket does not exist, create it
        val createBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build()
        s3Client.createBucket(createBucketRequest)
        println(s"Bucket '$bucketName' created successfully.")
    }
  }

  /**
    * Deletes a directory (all objects under a given prefix) from a bucket.
    *
    * @param bucketName Target S3/MinIO bucket.
    * @param directoryPrefix The directory to delete (must end with `/`).
    */
  def deleteDirectory(bucketName: String, directoryPrefix: String): Unit = {
    // Ensure the directory prefix ends with `/` to avoid accidental deletions
    val prefix = if (directoryPrefix.endsWith("/")) directoryPrefix else directoryPrefix + "/"

    // List objects under the given prefix
    val listRequest = ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .prefix(prefix)
      .build()

    val listResponse = s3Client.listObjectsV2(listRequest)

    // Extract object keys
    val objectKeys = listResponse.contents().asScala.map(_.key())

    if (objectKeys.nonEmpty) {
      val objectsToDelete =
        objectKeys.map(key => ObjectIdentifier.builder().key(key).build()).asJava

      val deleteRequest = Delete
        .builder()
        .objects(objectsToDelete)
        .build()

      // Compute MD5 checksum for MinIO if required
      val md5Hash = MessageDigest
        .getInstance("MD5")
        .digest(deleteRequest.toString.getBytes("UTF-8"))

      // Convert object keys to S3 DeleteObjectsRequest format
      val deleteObjectsRequest = DeleteObjectsRequest
        .builder()
        .bucket(bucketName)
        .delete(deleteRequest)
        .build()

      // Perform batch deletion
      s3Client.deleteObjects(deleteObjectsRequest)
    }
  }

  /**
    * Uploads a file to S3 using multipart upload for better performance.
    *
    * @param bucketName Target S3/MinIO bucket
    * @param key The object key (path) in the bucket
    * @param inputStream The input stream containing the file data
    * @param fileSize The size of the file in bytes
    * @return The ETag of the uploaded object
    */
  def multipartUpload(
      bucketName: String,
      key: String,
      inputStream: InputStream,
      fileSize: Long
  ): String = {
    // Initialize multipart upload
    val createMultipartUploadRequest = CreateMultipartUploadRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .build()

    val createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest)
    val uploadId = createMultipartUploadResponse.uploadId()

    try {
      // Calculate part size (minimum 5MB per part)
      val partSize =
        Math.max(MINIMUM_NUM_OF_MULTIPART_S3_PART, fileSize / MAXIMUM_NUM_OF_MULTIPART_S3_PARTS)
      var partNumber = 1
      val completedParts = new java.util.ArrayList[CompletedPart]()

      // Upload parts
      var remainingBytes = fileSize
      while (remainingBytes > 0) {
        val currentPartSize = Math.min(partSize, remainingBytes)
        val partData = new Array[Byte](currentPartSize.toInt)
        val bytesRead = inputStream.read(partData, 0, currentPartSize.toInt)

        if (bytesRead > 0) {
          val uploadPartRequest = UploadPartRequest
            .builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .build()

          val requestBody = software.amazon.awssdk.core.sync.RequestBody.fromBytes(partData)
          val uploadPartResponse = s3Client.uploadPart(uploadPartRequest, requestBody)

          val completedPart = CompletedPart
            .builder()
            .partNumber(partNumber)
            .eTag(uploadPartResponse.eTag())
            .build()

          completedParts.add(completedPart)
          partNumber += 1
          remainingBytes -= bytesRead
        }
      }

      // Complete multipart upload
      val completeMultipartUploadRequest = CompleteMultipartUploadRequest
        .builder()
        .bucket(bucketName)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
        .build()

      val completeResponse = s3Client.completeMultipartUpload(completeMultipartUploadRequest)
      completeResponse.eTag()
    } catch {
      case e: Exception =>
        // Abort multipart upload on failure
        val abortMultipartUploadRequest = AbortMultipartUploadRequest
          .builder()
          .bucket(bucketName)
          .key(key)
          .uploadId(uploadId)
          .build()
        s3Client.abortMultipartUpload(abortMultipartUploadRequest)
        throw e
    }
  }
}
