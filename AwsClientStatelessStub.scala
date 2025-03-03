package org.encalmo.aws

import org.encalmo.aws.AwsClientStatelessStub.*
import org.encalmo.aws.AwsDynamoDbApi.{DynamoDbItem, DynamoDbItemUpdate}

import software.amazon.awssdk.awscore.{AwsRequest, AwsResponse}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.apigatewayv2.ApiGatewayV2Client
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.iam.IamClient
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.lambda.LambdaClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.{
  BatchGetSecretValueRequest,
  BatchGetSecretValueResponse,
  GetSecretValueRequest,
  GetSecretValueResponse
}
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sts.StsClient

import scala.jdk.CollectionConverters.*
import scala.reflect.TypeTest
import software.amazon.awssdk.services.apigateway.ApiGatewayClient

/** Support for stateless testing of AWS Client interfaces. Work-in-progres. Currently suported are:
  *   - DynamoDB: Get, Put, Delete and Update Item.
  */
class AwsClientStatelessStub extends AwsClient, StubsBuffer {

  final override def isDebugMode: Boolean =
    Option(System.getenv("AWS_CLIENT_DEBUG_MODE")).getOrElse("ON").contains("ON")

  override lazy val dynamoDb: DynamoDbClientStatelessStub = new DynamoDbClientStatelessStub(this)
  override lazy val iam: IamClient = ???
  override lazy val sqs: SqsClient = ???
  override lazy val sts: StsClient = ???
  override lazy val secrets: SecretsManagerClient = new SecretsManagerClientStatelessStub(this)
  override lazy val kms: KmsClient = ???
  override lazy val s3: S3Client = ???
  override lazy val lambda: LambdaClient = ???
  override lazy val apigateway: ApiGatewayClient = ???
  override lazy val apigatewayv2: ApiGatewayV2Client = ???

  override lazy val currentRegion: Region =
    Region.of(Option(System.getenv("AWS_DEFAULT_REGION")).getOrElse("us-east-1"))

  final def expectPutItem(
      tableName: String,
      item: DynamoDbItem,
      mode: Mode = Mode.ONCE
  ): Unit = {
    val refinedItem = item.filterNot(_._2.nul())

    val request: PutItemRequest = PutItemRequest
      .builder()
      .tableName(tableName)
      .item(refinedItem.asJava)
      .build()
    val response = PutItemResponse.builder().build()
    expect(request, response, mode)
  }

  final def expectGetItem(
      tableName: String,
      key: (String, AttributeValue),
      expectedItem: DynamoDbItem,
      projection: Seq[String] = Seq.empty,
      mode: Mode = Mode.ONCE
  ): Unit = {
    val projectionExpressionOption: Option[String] =
      if (projection.isEmpty) None
      else Some((key._1 +: projection.filterNot(_ == key._1)).mkString(","))
    val request: GetItemRequest =
      if (
        projectionExpressionOption.isDefined
        && projection.exists(n => AwsDynamoDbApi.dynamoDbReservedWords.contains(n.toUpperCase()))
      )
      then
        GetItemRequest
          .builder()
          .key(Map(key).asJava)
          .tableName(tableName)
          .projectionExpression(
            (key._1 +: projection.filterNot(_ == key._1).map(n => s"#$n")).mkString(",")
          )
          .expressionAttributeNames(
            projection.filterNot(_ == key._1).map(n => (s"#$n", n)).toMap.asJava
          )
          .build()
      else
        GetItemRequest
          .builder()
          .key(Map(key).asJava)
          .tableName(tableName)
          .optionally(projectionExpressionOption, _.projectionExpression)
          .build()
    val response = GetItemResponse.builder().item(expectedItem.asJava).build()
    expect(request, response, mode)
  }

  final def expectDeleteItem(
      tableName: String,
      key: (String, AttributeValue),
      mode: Mode = Mode.ONCE
  ): Unit = {
    val request: DeleteItemRequest = DeleteItemRequest
      .builder()
      .tableName(tableName)
      .key(Map(key).asJava)
      .build()
    val response = DeleteItemResponse.builder().build()
    expect(request, response, mode)
  }

  final def expectUpdateItem(
      tableName: String,
      key: (String, AttributeValue),
      update: DynamoDbItemUpdate,
      mode: Mode = Mode.ONCE,
      returnUpdatedItem: Boolean = false
  ): Unit = {
    val refinedUpdate: collection.Map[String, AttributeValueUpdate] =
      update.filterNot(x =>
        x._2 == null ||
          (x._2.actionAsString() != "DELETE"
            && (x._2.value().nul() || x._2.value() == null))
      )

    val request: UpdateItemRequest = UpdateItemRequest
      .builder()
      .tableName(tableName)
      .key(Map(key).asJava)
      .attributeUpdates(refinedUpdate.asJava)
      .returnValues(
        if returnUpdatedItem then ReturnValue.ALL_NEW
        else ReturnValue.NONE
      )
      .build()
    val response =
      if (request.returnValues() == ReturnValue.ALL_NEW)
      then
        throw new UnsupportedOperationException(
          s"returning an updated item is not supported in stateless stub"
        )
      else UpdateItemResponse.builder().build()
    expect(request, response, mode)
  }

  final def expectGetSecretValueString(
      secretId: String,
      expectedSecret: String,
      mode: Mode = Mode.ONCE
  ): Unit = {
    val request = GetSecretValueRequest.builder().secretId(secretId).build()
    val response =
      GetSecretValueResponse.builder().secretString(expectedSecret).build()
    expect(request, response, mode)
  }

  final def expectGetSecretValueBinary(
      secretId: String,
      expectedSecret: Array[Byte],
      mode: Mode = Mode.ONCE
  ): Unit = {
    val request = GetSecretValueRequest.builder().secretId(secretId).build()
    val response =
      GetSecretValueResponse
        .builder()
        .secretBinary(SdkBytes.fromByteArray(expectedSecret))
        .build()
    expect(request, response, mode)
  }

}

object AwsClientStatelessStub {

  enum Mode:
    case ONCE
    case ALWAYS

  final case class Stub(
      request: AwsRequest,
      response: AwsResponse,
      mode: Mode,
      var consumed: Boolean = false
  )

  trait StubsBuffer {
    private val stubs = collection.mutable.Buffer[Stub]()

    final def find[R <: AwsResponse](
        request: AwsRequest
    )(
        exception: String => Exception
    )(using typeTest: TypeTest[AwsResponse, R]): R = {
      stubs
        .find(stub => !stub.consumed && stub.request == request)
        .map { stub =>
          if (stub.mode == Mode.ONCE) then stub.consumed = true
          typeTest
            .unapply(stub.response)
            .getOrElse(
              throw ClassCastException(
                s"Expected response type does not match the actual type."
              )
            )
        }
        .getOrElse(throw exception(request.toString()))
    }

    final def expect(
        request: AwsRequest,
        response: AwsResponse,
        mode: Mode
    ): Unit =
      stubs.append(Stub(request, response, mode))
  }

  abstract class AwsClientStubException(m: String) extends Exception(m)

  final class DynamoDbPutItemNotExpected(m: String) extends AwsClientStubException(m)
  final class DynamoDbGetItemNotExpected(m: String) extends AwsClientStubException(m)
  final class DynamoDbUpdateItemNotExpected(m: String) extends AwsClientStubException(m)
  final class DynamoDbDeleteItemNotExpected(m: String) extends AwsClientStubException(m)

  final class DynamoDbClientStatelessStub(stubs: StubsBuffer) extends DynamoDbClient {

    override def close(): Unit = ()
    override def serviceName(): String = "dynamodb"

    final override def putItem(
        putItemRequest: PutItemRequest
    ): PutItemResponse =
      stubs.find[PutItemResponse](putItemRequest)(
        DynamoDbPutItemNotExpected.apply
      )

    final override def getItem(
        getItemRequest: GetItemRequest
    ): GetItemResponse =
      stubs.find[GetItemResponse](getItemRequest)(
        DynamoDbGetItemNotExpected.apply
      )

    final override def updateItem(
        updateItemRequest: UpdateItemRequest
    ): UpdateItemResponse =
      stubs.find[UpdateItemResponse](updateItemRequest)(
        DynamoDbUpdateItemNotExpected.apply
      )

    final override def deleteItem(
        deleteItemRequest: DeleteItemRequest
    ): DeleteItemResponse =
      stubs.find[DeleteItemResponse](deleteItemRequest)(
        DynamoDbDeleteItemNotExpected.apply
      )
  }

  final class GetSecretValueNotExpected(m: String) extends AwsClientStubException(m)
  final class BatchGetSecretValueNotExpected(m: String) extends AwsClientStubException(m)

  final class SecretsManagerClientStatelessStub(stubs: StubsBuffer) extends SecretsManagerClient {

    final override def close(): Unit = ()
    final override def serviceName(): String = "secretsmanager"

    final override def getSecretValue(
        getSecretValueRequest: GetSecretValueRequest
    ): GetSecretValueResponse =
      stubs.find[GetSecretValueResponse](getSecretValueRequest)(
        GetSecretValueNotExpected.apply
      )

    final override def batchGetSecretValue(
        batchGetSecretValueRequest: BatchGetSecretValueRequest
    ): BatchGetSecretValueResponse =
      stubs.find[BatchGetSecretValueResponse](batchGetSecretValueRequest)(
        BatchGetSecretValueNotExpected.apply
      )

  }

}
