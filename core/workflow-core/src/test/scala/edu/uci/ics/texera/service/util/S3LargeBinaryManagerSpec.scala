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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchers.any
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.core.sync.RequestBody
import java.io.ByteArrayInputStream
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.core.storage.StorageConfig

class S3LargeBinaryManagerSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  // Mock S3Client
  private val mockS3Client = mock[S3Client]
  private val testBucket = StorageConfig.s3LargeBinaryBucketName
  private val testKey = "test-key"
  private val testUploadId = "test-upload-id"
  private val testETag = "test-etag"
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // Setup mock responses
  private def setupMockResponses(): Unit = {
    // Mock createMultipartUpload response
    val createMultipartUploadResponse = CreateMultipartUploadResponse
      .builder()
      .uploadId(testUploadId)
      .build()
    doReturn(createMultipartUploadResponse)
      .when(mockS3Client)
      .createMultipartUpload(any[CreateMultipartUploadRequest])

    // Mock uploadPart response
    val uploadPartResponse = UploadPartResponse
      .builder()
      .eTag(testETag)
      .build()
    doReturn(uploadPartResponse)
      .when(mockS3Client)
      .uploadPart(any[UploadPartRequest], any[RequestBody])

    // Mock completeMultipartUpload response
    val completeMultipartUploadResponse = CompleteMultipartUploadResponse
      .builder()
      .build()
    doReturn(completeMultipartUploadResponse)
      .when(mockS3Client)
      .completeMultipartUpload(any[CompleteMultipartUploadRequest])

    // Mock getObject response for reference count
    val getObjectResponse = GetObjectResponse
      .builder()
      .build()
    doReturn(getObjectResponse)
      .when(mockS3Client)
      .getObject(any[GetObjectRequest])

    // Mock putObject response
    val putObjectResponse = PutObjectResponse
      .builder()
      .build()
    doReturn(putObjectResponse)
      .when(mockS3Client)
      .putObject(any[PutObjectRequest], any[RequestBody])
  }

  "S3LargeBinaryManager" should "upload a file successfully" in {
    setupMockResponses()

    // Create a test input stream with some data
    val testData = "test data".getBytes
    val inputStream = new ByteArrayInputStream(testData)

    // Save original S3Client and replace with mock
    val originalS3Client = S3StorageClient.getS3Client
    try {
      // Use reflection to set the S3Client
      val field = classOf[S3StorageClient.type].getDeclaredField("s3Client")
      field.setAccessible(true)
      field.set(S3StorageClient, mockS3Client)

      // Perform the upload
      val s3Uri = S3LargeBinaryManager.uploadFile(inputStream)

      // Verify the result
      s3Uri should startWith("s3://")
      s3Uri should include(testBucket)

      // Verify S3 client interactions
      verify(mockS3Client).createMultipartUpload(any[CreateMultipartUploadRequest])
      verify(mockS3Client).uploadPart(any[UploadPartRequest], any[RequestBody])
      verify(mockS3Client).completeMultipartUpload(any[CompleteMultipartUploadRequest])
    } finally {
      // Restore original S3Client
      val field = classOf[S3StorageClient.type].getDeclaredField("s3Client")
      field.setAccessible(true)
      field.set(S3StorageClient, originalS3Client)
    }
  }

  it should "handle reference counting correctly" in {
    setupMockResponses()

    val s3Uri = s"s3://$testBucket/$testKey"
    val referenceCountKey = s"reference-counts/$testKey"

    // Save original S3Client and replace with mock
    val originalS3Client = S3StorageClient.getS3Client
    try {
      // Use reflection to set the S3Client
      val field = classOf[S3StorageClient.type].getDeclaredField("s3Client")
      field.setAccessible(true)
      field.set(S3StorageClient, mockS3Client)

      // Mock getObject to return 0 as current count
      val getObjectResponse = GetObjectResponse
        .builder()
        .build()
      doReturn(getObjectResponse)
        .when(mockS3Client)
        .getObject(any[GetObjectRequest])

      // Test increment
      S3LargeBinaryManager.incrementReferenceCount(s3Uri)
      verify(mockS3Client).getObject(any[GetObjectRequest])
      verify(mockS3Client, times(2)).putObject(any[PutObjectRequest], any[RequestBody])

      // Test decrement
      val newCount = S3LargeBinaryManager.decrementReferenceCount(s3Uri)
      newCount shouldBe 0
      verify(mockS3Client, times(2)).getObject(any[GetObjectRequest])
      verify(mockS3Client, times(3)).putObject(any[PutObjectRequest], any[RequestBody])
    } finally {
      // Restore original S3Client
      val field = classOf[S3StorageClient.type].getDeclaredField("s3Client")
      field.setAccessible(true)
      field.set(S3StorageClient, originalS3Client)
    }
  }

  it should "delete object when reference count reaches zero" in {
    setupMockResponses()

    val s3Uri = s"s3://$testBucket/$testKey"
    val referenceCountKey = s"reference-counts/$testKey"

    // Save original S3Client and replace with mock
    val originalS3Client = S3StorageClient.getS3Client
    try {
      // Use reflection to set the S3Client
      val field = classOf[S3StorageClient.type].getDeclaredField("s3Client")
      field.setAccessible(true)
      field.set(S3StorageClient, mockS3Client)

      // Mock getObject to return 1 as current count
      val getObjectResponse = GetObjectResponse
        .builder()
        .build()
      doReturn(getObjectResponse)
        .when(mockS3Client)
        .getObject(any[GetObjectRequest])

      // Decrement count to zero
      val newCount = S3LargeBinaryManager.decrementReferenceCount(s3Uri)
      newCount shouldBe 0

      // Verify delete operations
      verify(mockS3Client, times(4)).deleteObject(any[DeleteObjectRequest])
    } finally {
      // Restore original S3Client
      val field = classOf[S3StorageClient.type].getDeclaredField("s3Client")
      field.setAccessible(true)
      field.set(S3StorageClient, originalS3Client)
    }
  }

  it should "handle invalid S3 URI format" in {
    val invalidUri = "invalid-uri"

    // Test increment
    an[IllegalArgumentException] should be thrownBy {
      S3LargeBinaryManager.incrementReferenceCount(invalidUri)
    }

    // Test decrement
    an[IllegalArgumentException] should be thrownBy {
      S3LargeBinaryManager.decrementReferenceCount(invalidUri)
    }
  }
}
