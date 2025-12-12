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
  *   val crypto = CryptoService("super-long-random-secret")
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
  private val IvLength = 12 // 96-bit IV
  private val TagLength = 128 // bits

  private val random = new SecureRandom()

  /** Build an instance from a String secret. */
  def apply(secret: String): CryptoService =
    new CryptoService(deriveKeyFromSecret(secret))

  /** Build an instance from an existing SecretKey. */
  def fromKey(key: SecretKey): CryptoService =
    new CryptoService(key)

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

  /** Convenience helpers if you *really* want one-liners. */
  def encryptWithSecret(plain: String, secret: String): String = {
    val key = deriveKeyFromSecret(secret)
    encrypt(plain, key)
  }

  def decryptWithSecret(token: String, secret: String): String = {
    val key = deriveKeyFromSecret(secret)
    decrypt(token, key)
  }
}
