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

package org.apache.texera.auth.util

import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, SecureRandom}
import java.util.Base64
import javax.crypto.{Cipher, SecretKey}
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}

/**
  * Generic AES-GCM crypto utilities.
  *
  * Usage:
  *   val crypto = CryptoService("secret")
  *   val token  = crypto.encrypt("hello")
  *   val plain  = crypto.decrypt(token)
  */
final class CryptoService private (private val key: SecretKey) {

  def encrypt(plain: String): String =
    CryptoService.encrypt(plain, key)

  def decrypt(token: String): String =
    CryptoService.decrypt(token, key)
}

object CryptoService {
  private val Algorithm = "AES/GCM/NoPadding"
  private val IvLength = 12
  private val TagLength = 128

  private val random = new SecureRandom()

  /** Build an instance from a String secret. */
  def apply(secret: String): CryptoService =
    new CryptoService(deriveKeyFromSecret(secret))

  /** Derive a 256-bit AES key from a String. */
  def deriveKeyFromSecret(secret: String): SecretKey = {
    val digest = MessageDigest.getInstance("SHA-256")
    val keyBytes = digest.digest(secret.getBytes(StandardCharsets.UTF_8))
    new SecretKeySpec(keyBytes, "AES")
  }

  /** Low-level encrypt with explicit key. */
  def encrypt(plain: String, key: SecretKey): String = {
    val iv = new Array[Byte](IvLength)
    random.nextBytes(iv)

    val cipher = Cipher.getInstance(Algorithm)
    cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TagLength, iv))

    val cipherText = cipher.doFinal(plain.getBytes(StandardCharsets.UTF_8))

    val combined = new Array[Byte](iv.length + cipherText.length)
    System.arraycopy(iv, 0, combined, 0, iv.length)
    System.arraycopy(cipherText, 0, combined, iv.length, cipherText.length)

    Base64.getUrlEncoder.withoutPadding().encodeToString(combined)
  }

  /** Low-level decrypt with explicit key. */
  def decrypt(token: String, key: SecretKey): String = {
    val combined = Base64.getUrlDecoder.decode(token)

    if (combined.length <= IvLength) {
      throw new IllegalArgumentException("Invalid encrypted token")
    }

    val iv = java.util.Arrays.copyOfRange(combined, 0, IvLength)
    val cipherText = java.util.Arrays.copyOfRange(combined, IvLength, combined.length)

    val cipher = Cipher.getInstance(Algorithm)
    cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TagLength, iv))

    val plainBytes = cipher.doFinal(cipherText)
    new String(plainBytes, StandardCharsets.UTF_8)
  }
}
