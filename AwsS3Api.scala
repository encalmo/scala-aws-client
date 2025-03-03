package org.encalmo.aws

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.{GetObjectPresignRequest, PutObjectPresignRequest}

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Duration
import java.util.Base64
import scala.jdk.CollectionConverters.*

object AwsS3Api {

  /** Returns a list of all buckets owned by the authenticated sender of the request. To use this operation, you must
    * have the aws.s3:ListAllMyBuckets permission.
    */
  inline def listBuckets()(using aws: AwsClient): Seq[Bucket] =
    AwsClient.invoke(s"listBuckets") {
      aws.s3
        .listBuckets(ListBucketsRequest.builder().build())
        .buckets()
        .asScala
        .toSeq
    }

  /** Creates an Amazon S3 bucket. */
  inline def createBucket(bucketName: String)(using aws: AwsClient): Boolean =
    AwsClient.invoke(s"createBucket") {
      aws.s3.createBucket(
        CreateBucketRequest
          .builder()
          .bucket(bucketName)
          .build()
      )

      val bucketRequestWait = HeadBucketRequest
        .builder()
        .bucket(bucketName)
        .build()
      // Wait until the bucket is created.
      val waiterResponse =
        aws.s3.waiter().waitUntilBucketExists(bucketRequestWait)
      waiterResponse.matched().response().isPresent()
    }

  /** Returns some or all (up to 1,000) of the objects in a bucket. */
  inline def listBucketObjects(
      bucketName: String
  )(using aws: AwsClient): Seq[S3Object] =
    AwsClient.invoke(s"listBucketObjects") {
      aws.s3
        .listObjects(
          ListObjectsRequest
            .builder()
            .bucket(bucketName)
            .build()
        )
        .contents()
        .asScala
        .toSeq
    }

  /** Returns some or all (up to 1,000) of the objects in a bucket. Limits the response to keys that begin with the
    * specified prefix.
    */
  inline def listBucketObjects(
      bucketName: String,
      objectKeyPrefix: String
  )(using aws: AwsClient): Seq[S3Object] =
    AwsClient.invoke(s"listBucketObjects") {
      aws.s3
        .listObjects(
          ListObjectsRequest
            .builder()
            .bucket(bucketName)
            .prefix(objectKeyPrefix)
            .build()
        )
        .contents()
        .asScala
        .toSeq
    }

  /** Retrieves all the metadata from an object without returning the object itself. This operation is useful if you're
    * interested only in an object's metadata.
    */
  inline def getObjectAttributes(
      bucketName: String,
      objectKey: String
  )(using aws: AwsClient): GetObjectAttributesResponse =
    AwsClient.invoke(s"getObjectAttributes") {
      aws.s3.getObjectAttributes(
        GetObjectAttributesRequest
          .builder()
          .bucket(bucketName)
          .key(objectKey)
          .build()
      )
    }

  /** Returns a map of metadata of the object in S3. */
  def getObjectMetadata(
      bucketName: String,
      objectKey: String
  )(using aws: AwsClient): Map[String, String] =
    AwsClient.invoke(s"getObjectMetadata") {
      aws.s3
        .getObject(
          GetObjectRequest
            .builder()
            .bucket(bucketName)
            .key(objectKey)
            .build()
        )
        .response()
        .metadata()
        .asScala
        .toMap
    }

  /** The HEAD operation retrieves metadata from an object without returning the object itself. This operation is useful
    * if you're interested only in an object's metadata.
    *
    * A HEAD request has the same options as a GET operation on an object. The response is identical to the GET response
    * except that there is no response body. Because of this, if the HEAD request generates an error, it returns a
    * generic code, such as 400 Bad Request, 403 Forbidden, 404 Not Found, 405 Method Not Allowed, 412 Precondition
    * Failed, or 304 Not Modified. It's not possible to retrieve the exact exception of these error codes.
    */
  def headObject(
      bucketName: String,
      objectKey: String
  )(using aws: AwsClient): HeadObjectResponse =
    AwsClient.invoke(s"headObject") {
      aws.s3.headObject(
        HeadObjectRequest.builder().bucket(bucketName).key(objectKey).build()
      )
    }

  def checkObjectExists(
      bucketName: String,
      objectKey: String
  )(using aws: AwsClient) =
    AwsClient.invoke(s"checkObjectExists") {
      try {
        headObject(bucketName, objectKey)
        true
      } catch {
        case _ => false
      }
    }

  def checkObjectNotExists(
      bucketName: String,
      objectKey: String
  )(using aws: AwsClient) =
    AwsClient.invoke(s"checkObjectNotExists") {
      try {
        headObject(bucketName, objectKey)
        false
      } catch {
        case _ => true
      }
    }

  /** Retrieves an object from Amazon S3. */
  def getObjectInputStream(
      bucketName: String,
      objectKey: String
  )(using aws: AwsClient): InputStream =
    AwsClient.invoke(s"getObjectInputStream") {
      aws.s3
        .getObjectAsBytes(
          GetObjectRequest
            .builder()
            .bucket(bucketName)
            .key(objectKey)
            .checksumMode(ChecksumMode.ENABLED)
            .build()
        )
        .asInputStream()
    }

  /** Adds an object to a bucket. */
  def putObjectUsingByteArray(
      bucketName: String,
      objectKey: String,
      bytes: Array[Byte],
      metadata: Map[String, String] = Map.empty
  )(using
      aws: AwsClient
  ): PutObjectResponse =
    AwsClient.invoke(s"putObjectUsingByteArray") {
      aws.s3.putObject(
        PutObjectRequest
          .builder()
          .bucket(bucketName)
          .key(objectKey)
          .metadata(metadata.asJava)
          .checksumAlgorithm(ChecksumAlgorithm.SHA256)
          .checksumSHA256(
            new String(
              Base64
                .getEncoder()
                .encode(
                  MessageDigest
                    .getInstance("SHA-256")
                    .digest(bytes)
                )
            )
          )
          .build(),
        RequestBody.fromBytes(bytes)
      )
    }

  /** Adds an object to a bucket. */
  def putObjectUsingByteBuffer(
      bucketName: String,
      objectKey: String,
      buffer: ByteBuffer,
      metadata: Map[String, String] = Map.empty
  )(using
      aws: AwsClient
  ): PutObjectResponse =
    AwsClient.invoke(s"putObjectUsingByteBuffer") {
      aws.s3.putObject(
        PutObjectRequest
          .builder()
          .bucket(bucketName)
          .key(objectKey)
          .metadata(metadata.asJava)
          .build(),
        RequestBody.fromByteBuffer(buffer)
      )
    }

  /** Adds an object to a bucket. */
  def putObjectUsingPath(
      bucketName: String,
      objectKey: String,
      path: Path,
      metadata: Map[String, String] = Map.empty
  )(using
      aws: AwsClient
  ): PutObjectResponse =
    AwsClient.invoke(s"putObjectUsingPath") {
      aws.s3.putObject(
        PutObjectRequest
          .builder()
          .bucket(bucketName)
          .key(objectKey)
          .metadata(metadata.asJava)
          .build(),
        RequestBody.fromFile(path)
      )
    }

  def createPresignedGetUrl(
      bucketName: String,
      objectKey: String,
      durationMinutes: Int
  )(using
      aws: AwsClient
  ): String =
    AwsClient.invoke(s"createPresignedGetUrl") {
      val presigner = S3Presigner.create()
      val objectRequest = GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(objectKey)
        .build()
      val presignRequest = GetObjectPresignRequest
        .builder()
        .signatureDuration(
          Duration.ofMinutes(durationMinutes)
        )
        .getObjectRequest(objectRequest)
        .build()
      val presignedRequest = presigner.presignGetObject(presignRequest)
      presignedRequest.url().toExternalForm()
    }

  def createPresignedPutUrl(
      bucketName: String,
      objectKey: String,
      durationMinutes: Int,
      metadata: Map[String, String] = Map.empty
  )(using
      aws: AwsClient
  ): String =
    AwsClient.invoke(s"createPresignedPutUrl") {
      val presigner = S3Presigner.create()
      val objectRequest = PutObjectRequest
        .builder()
        .bucket(bucketName)
        .key(objectKey)
        .metadata(metadata.asJava)
        .build()
      val presignRequest = PutObjectPresignRequest
        .builder()
        .signatureDuration(
          Duration.ofMinutes(durationMinutes)
        )
        .putObjectRequest(objectRequest)
        .build()
      val presignedRequest = presigner.presignPutObject(presignRequest)
      presignedRequest.url().toExternalForm()
    }

}
