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

package org.apache.texera.auth

import org.apache.texera.auth.util.CryptoService
import org.apache.texera.config.AuthConfig

import java.nio.charset.StandardCharsets
import java.util.Base64

/**
  * Upload-token codec.
  *
  * Stateless, self-contained encrypted token that encodes:
  *   - uploadId
  *   - did (dataset id)
  *   - uid (user id)
  *   - filePath
  *   - physicalAddress (e.g. s3://bucket/key)
  *
  * Wire format (before AES-GCM encryption):
  *   v1|uploadId|did|uid|filePathB64|physicalB64
  */
object UploadTokenParser {

  final case class UploadTokenPayload(
      uploadId: String,
      did: Int,
      uid: Int,
      filePath: String,
      physicalAddress: String
  )

  private val Version = "v1"
  private val Encoder = Base64.getUrlEncoder.withoutPadding()
  private val Decoder = Base64.getUrlDecoder

  // One crypto instance for this JVM, using the configured upload-token secret
  private val crypto: CryptoService =
    CryptoService(AuthConfig.uploadTokenSecretKey)

  /**
    * Build a payload (no expiration).
    */
  def buildPayload(
      did: Int,
      uid: Int,
      filePath: String,
      uploadId: String,
      physicalAddress: String
  ): UploadTokenPayload =
    UploadTokenPayload(
      uploadId = uploadId,
      did = did,
      uid = uid,
      filePath = filePath,
      physicalAddress = physicalAddress
    )

  /**
    * Encode a Payload into an encrypted, URL-safe token string.
    */
  def encode(payload: UploadTokenPayload): String = {
    val filePathB64 = Encoder.encodeToString(
      payload.filePath.getBytes(StandardCharsets.UTF_8)
    )
    val physicalB64 = Encoder.encodeToString(
      payload.physicalAddress.getBytes(StandardCharsets.UTF_8)
    )

    val raw =
      s"$Version|${payload.uploadId}|${payload.did}|${payload.uid}|$filePathB64|$physicalB64"

    crypto.encrypt(raw)
  }

  /**
    * Decode and decrypt a token string into a Payload.
    *
    * Throws IllegalArgumentException on:
    *   - invalid ciphertext
    *   - malformed structure
    *   - unsupported version
    */
  def decode(token: String): UploadTokenPayload = {
    val raw =
      try crypto.decrypt(token)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException("Invalid upload token", e)
      }

    val parts = raw.split("\\|", 7) // expect: v1 + 5 fields = 6 parts
    if (parts.length != 6 || parts(0) != Version) {
      throw new IllegalArgumentException("Unsupported or malformed upload token")
    }

    val uploadId = parts(1)
    val did = parts(2).toInt
    val uid = parts(3).toInt

    val filePath = new String(
      Decoder.decode(parts(4)),
      StandardCharsets.UTF_8
    )

    val physicalAddress = new String(
      Decoder.decode(parts(5)),
      StandardCharsets.UTF_8
    )

    UploadTokenPayload(
      uploadId = uploadId,
      did = did,
      uid = uid,
      filePath = filePath,
      physicalAddress = physicalAddress
    )
  }
}
