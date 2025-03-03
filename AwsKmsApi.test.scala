package org.encalmo.aws

import java.nio.charset.StandardCharsets
import java.util.Base64

class AwsKmsApiSpec extends TestSuite {

  val testSigningKeyArn =
    "arn:aws:kms:eu-central-1:047719648492:alias/test-signing"

  val testEncryptionKeyArn =
    "arn:aws:kms:eu-central-1:047719648492:alias/test-encryption"

  test("sign and verify array of bytes") {

    given awsClient: AwsClient =
      AwsClient.initializeWithProperties(
        localAwsSecurityCredentials ++ debugModeProperties
      )

    val signature: Array[Byte] = AwsKmsApi
      .signRawMessage(
        testSigningKeyArn,
        "RSASSA_PKCS1_V1_5_SHA_256",
        "Hello World!".getBytes()
      )

    assert(
      AwsKmsApi
        .verifyRawMessage(
          testSigningKeyArn,
          "RSASSA_PKCS1_V1_5_SHA_256",
          "Hello World!".getBytes(),
          signature
        )
    )
  }

  test("sign and verify string message") {

    given awsClient: AwsClient =
      AwsClient.initializeWithProperties(
        localAwsSecurityCredentials ++ debugModeProperties
      )

    val signature: String = AwsKmsApi
      .signRawMessage(
        testSigningKeyArn,
        "RSASSA_PKCS1_V1_5_SHA_256",
        "Hello World!"
      )

    assert(
      AwsKmsApi
        .verifyRawMessage(
          testSigningKeyArn,
          "RSASSA_PKCS1_V1_5_SHA_256",
          "Hello World!",
          signature
        )
    )
  }

  test("encrypt and decrypt array of bytes") {

    given awsClient: AwsClient =
      AwsClient.initializeWithProperties(
        localAwsSecurityCredentials ++ debugModeProperties
      )

    val message: Array[Byte] = "Hello World!".getBytes()

    val ciphertextBlob: Array[Byte] = AwsKmsApi
      .encryptMessage(
        testEncryptionKeyArn,
        "RSAES_OAEP_SHA_256",
        message
      )

    val decryptedMessage: Array[Byte] = AwsKmsApi
      .decryptMessage(
        testEncryptionKeyArn,
        "RSAES_OAEP_SHA_256",
        ciphertextBlob
      )

    assert(message.sameElements(decryptedMessage))

  }
}
