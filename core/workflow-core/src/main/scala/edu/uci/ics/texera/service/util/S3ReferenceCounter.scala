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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Manages reference counts for S3 objects.
  * Uses a separate S3 object to store reference counts in JSON format.
  */
object S3ReferenceCounter {
  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val referenceCountKey = "reference-counts.json"

  /**
    * Initialize reference count for an object to 0
    * @param bucketName The S3 bucket name
    * @param objectKey The key of the object to initialize
    */
  def initializeReferenceCount(bucketName: String, objectKey: String): Unit = {
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
  def incrementReferenceCount(bucketName: String, objectKey: String): Int = {
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
  def decrementReferenceCount(bucketName: String, objectKey: String): Int = {
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
  def getReferenceCount(bucketName: String, objectKey: String): Int = {
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
}
