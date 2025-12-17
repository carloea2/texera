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

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

object UploadTokenParser {

  val Version: String = "v1"

  @JsonIgnoreProperties(ignoreUnknown = true)
  final case class UploadTokenPayload @JsonCreator(mode = JsonCreator.Mode.PROPERTIES) (
      @JsonProperty(value = "version", required = true)
      version: String,
      @JsonProperty(value = "uploadId", required = true)
      uploadId: String,
      @JsonProperty(value = "did", required = true)
      did: Int,
      @JsonProperty(value = "uid", required = true)
      uid: Int,
      @JsonProperty(value = "filePath", required = true)
      filePath: String,
      @JsonProperty(value = "physicalAddress", required = true)
      physicalAddress: String
  )

  private lazy val cryptoService: CryptoService =
    CryptoService(AuthConfig.uploadTokenSecretKey)

  private lazy val objectMapper: ObjectMapper =
    new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def encode(payload: UploadTokenPayload): String = {
    val node = objectMapper.createObjectNode()
    node.put("version", Version)
    node.put("uploadId", payload.uploadId)
    node.put("did", payload.did)
    node.put("uid", payload.uid)
    node.put("filePath", payload.filePath)
    node.put("physicalAddress", payload.physicalAddress)

    val rawJson = objectMapper.writeValueAsString(node)
    cryptoService.encrypt(rawJson)
  }

  def decode(token: String): UploadTokenPayload = {
    val decryptedJson = cryptoService.decrypt(token)
    val decodedPayload = objectMapper.readValue(decryptedJson, classOf[UploadTokenPayload])

    decodedPayload
  }
}
