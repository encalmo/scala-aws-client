package org.encalmo.aws

import software.amazon.awssdk.services.secretsmanager.model.*

import scala.jdk.CollectionConverters.*

object AwsSecretsManagerApi {

  /** Retrieves the contents of the encrypted fields SecretBinary from the specified version of a secret.
    *
    * To retrieve the values for a group of secrets, call BatchGetSecretValue.
    *
    * **Required permissions: ** secretsmanager:GetSecretValue. If the secret is encrypted using a customer-managed key
    * instead of the Amazon Web Services managed key aws/secretsmanager, then you also need kms:Decrypt permissions for
    * that key. For more information, see IAM policy actions for Secrets Manager and Authentication and access control
    * in Secrets Manager.
    *
    * @param secretId
    *   The ARN or name of the secret to retrieve. For an ARN, we recommend that you specify a complete ARN rather than
    *   a partial ARN.
    */
  def getSecretValueString(secretId: String)(using AwsClient): String =
    summon[AwsClient].secrets
      .getSecretValue(
        GetSecretValueRequest.builder().secretId(secretId).build()
      )
      .secretString()

  /** Creates a new version with a new encrypted secret value and attaches it to the secret. The version can contain a
    * new SecretString value or a new SecretBinary value.
    *
    * We recommend you avoid calling PutSecretValue at a sustained rate of more than once every 10 minutes. When you
    * update the secret value, Secrets Manager creates a new version of the secret. Secrets Manager removes outdated
    * versions when there are more than 100, but it does not remove versions created less than 24 hours ago. If you call
    * PutSecretValue more than once every 10 minutes, you create more versions than Secrets Manager removes, and you
    * will reach the quota for secret versions.
    *
    * @param secretId
    *   The ARN or name of the secret to retrieve. For an ARN, we recommend that you specify a complete ARN rather than
    *   a partial ARN.
    *
    * @param secret
    *   The text to encrypt and store in the new version of the secret.
    */
  def putSecretValue(secretId: String, secret: String)(using
      AwsClient
  ): String =
    summon[AwsClient].secrets
      .putSecretValue(
        PutSecretValueRequest
          .builder()
          .secretId(secretId)
          .secretString(secret)
          .build()
      )
      .versionId()

  /** Retrieves the contents of the encrypted fields SecretString from the specified version of a secret.
    *
    * To retrieve the values for a group of secrets, call BatchGetSecretValue.
    *
    * **Required permissions: ** secretsmanager:GetSecretValue. If the secret is encrypted using a customer-managed key
    * instead of the Amazon Web Services managed key aws/secretsmanager, then you also need kms:Decrypt permissions for
    * that key. For more information, see IAM policy actions for Secrets Manager and Authentication and access control
    * in Secrets Manager.
    *
    * @param secretId
    *   The ARN or name of the secret to retrieve. For an ARN, we recommend that you specify a complete ARN rather than
    *   a partial ARN.
    */
  def getSecretValueBinary(secretId: String)(using AwsClient): Array[Byte] =
    summon[AwsClient].secrets
      .getSecretValue(
        GetSecretValueRequest.builder().secretId(secretId).build()
      )
      .secretBinary()
      .asByteArray()

  /** Retrieves the contents of the encrypted fields SecretString or SecretBinary for up to 20 secrets. To retrieve a
    * single secret, call GetSecretValue.
    *
    * To choose which secrets to retrieve, you can specify a list of secrets by name or ARN, or you can use filters. If
    * Secrets Manager encounters errors such as AccessDeniedException while attempting to retrieve any of the secrets,
    * you can see the errors in Errors in the response.
    *
    * @param secretIds
    *   The ARN or names of the secrets to retrieve
    */
  def getSecretValueBinary(
      secretsIds: String*
  )(using AwsClient): Seq[SecretValueEntry] =
    summon[AwsClient].secrets
      .batchGetSecretValue(
        BatchGetSecretValueRequest
          .builder()
          .secretIdList(secretsIds.asJava)
          .build()
      )
      .secretValues()
      .asScala
      .toSeq

}
