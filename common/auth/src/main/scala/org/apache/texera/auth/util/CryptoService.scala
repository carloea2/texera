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

  /** Low-level encrypt with explicit key.
   *
   * Algorithm: AES-GCM (AEAD).
   * - Provides confidentiality (encryption) and integrity/authenticity (GCM tag).
   * - Output format (before Base64): [ IV || (ciphertext || tag) ]
   *   In JCE, `doFinal()` in GCM returns ciphertext with the authentication tag appended.
   */
  def encrypt(plain: String, key: SecretKey): String = {

    // Allocate a fresh IV/nonce for this encryption.
    // In GCM the IV must be unique per message under the same key; uniqueness prevents nonce-reuse attacks (keystream reuse and possible tag forgery).
    val iv = new Array[Byte](IvLength)

    // Fill IV with cryptographically secure random bytes.
    // Random IVs make identical plaintexts encrypt to different outputs and make collisions extremely unlikely.
    random.nextBytes(iv)

    // Create a Cipher for the requested transformation (e.g., "AES/GCM/NoPadding").
    // GCM is the mode; "NoPadding" is standard for GCM in JCE.
    val cipher = Cipher.getInstance(Algorithm)

    // Initialize cipher for ENCRYPT using:
    // - `key`: the AES key material
    // - `GCMParameterSpec(TagLength, iv)`: supplies the IV and the desired authentication tag length.
    //   TagLength is in bits (commonly 128 bits = 16 bytes).
    // The tag is what later allows decryption to detect tampering.
    cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TagLength, iv))

    // Convert the plaintext string to bytes in a deterministic encoding (UTF-8).
    // Crypto APIs operate on bytes; UTF-8 avoids platform-dependent encodings.
    val plainBytes = plain.getBytes(StandardCharsets.UTF_8)

    // Encrypt and compute the authentication tag.
    // For AES-GCM in JCE, `doFinal()` returns: ciphertext || tag (tag appended at the end).
    // Any modification of the ciphertext/tag will be detected during decrypt via tag verification.
    val cipherText = cipher.doFinal(plainBytes)

    // Build the final payload to return.
    // We must include the IV with the output because decryption needs the same IV to recompute the keystream
    // and verify the authentication tag. The IV is not secret, only required to be unique.
    val combined = new Array[Byte](iv.length + cipherText.length)

    // Prefix the payload with the IV so the decrypt() routine can read it back.
    System.arraycopy(iv, 0, combined, 0, iv.length)

    // Append ciphertext+tag after the IV.
    System.arraycopy(cipherText, 0, combined, iv.length, cipherText.length)

    // Encode binary payload as URL-safe Base64 text (no '+', '/', or '=' padding).
    // This makes it safe to store/transport in URLs, cookies, headers, etc.
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
