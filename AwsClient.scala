package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.{DynamoDbItem, show}

import software.amazon.awssdk.auth.credentials.{AwsCredentials, AwsCredentialsProvider, AwsSessionCredentials}
import software.amazon.awssdk.awscore.AwsRequest
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.interceptor.Context.ModifyHttpRequest
import software.amazon.awssdk.core.interceptor.{ExecutionAttributes, ExecutionInterceptor}
import software.amazon.awssdk.http.SdkHttpRequest
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbResponse, UpdateItemResponse}
import software.amazon.awssdk.services.iam.IamClient
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.apigateway.ApiGatewayClient
import software.amazon.awssdk.services.apigatewayv2.ApiGatewayV2Client

import scala.io.AnsiColor
import scala.io.AnsiColor.*
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal
import software.amazon.awssdk.services.lambda.LambdaClient

trait AwsClient {

  def isDebugMode: Boolean = false

  def currentRegion: Region

  def sts: StsClient
  def iam: IamClient
  def sqs: SqsClient
  def dynamoDb: DynamoDbClient
  def secrets: SecretsManagerClient
  def kms: KmsClient
  def s3: S3Client
  def lambda: LambdaClient
  def apigateway: ApiGatewayClient
  def apigatewayv2: ApiGatewayV2Client

  lazy val callerAccountId: String =
    sts.getCallerIdentity().account()

  extension [B <: AwsRequest.Builder](builder: B)
    inline def optionally[A](
        value: Option[A],
        function: B => (A => B)
    ): B = {
      value
        .map(value => function(builder)(value))
        .getOrElse(builder)
    }

    inline def conditionally(
        condition: Boolean,
        function: B => B
    ): B = {
      if (condition)
      then function(builder)
      else builder
    }

}

object AwsClient {

  inline def invoke[R](
      inline description: => String
  )(body: => R)(using awsClient: AwsClient): R = {
    val result = body
    if (awsClient.isDebugMode)
    then
      print(AnsiColor.YELLOW)
      print("[AwsClient] ")
      print(AnsiColor.CYAN)
      print(description)
      print(AnsiColor.RESET)
      print(AnsiColor.YELLOW)
      print(" returned ")
      print(AnsiColor.CYAN)
      inline result match {
        case _: Seq[DynamoDbItem] =>
          println(result.asInstanceOf[List[DynamoDbItem]].map(_.show).mkString("[", ", ", "]"))
        case _: Option[DynamoDbItem] =>
          println(result.asInstanceOf[Option[DynamoDbItem]].map(_.show).getOrElse("none"))
        case _: DynamoDbItem =>
          println(result.asInstanceOf[DynamoDbItem].show)
        case _: UpdateItemResponse =>
          val r = result.asInstanceOf[UpdateItemResponse]
          if (r.hasAttributes()) then println(r.attributes().asScala.asInstanceOf[DynamoDbItem].show)
          else println("success")
        case _: DynamoDbResponse =>
          println("success")
        case _ =>
          println(result)
      }
      print(AnsiColor.RESET)

    result
  }

  extension [B <: AwsClientBuilder[B, ?]](builder: B)
    inline def optionalConfiguration[A](
        value: Option[A],
        function: B => (A => B)
    ): B = {
      value
        .map(value => function(builder)(value))
        .getOrElse(builder)
    }

  inline def currentRegion(using awsClient: AwsClient): Region =
    awsClient.currentRegion

  inline def callerAccountId(using awsClient: AwsClient): String =
    awsClient.callerAccountId

  /** Creates new instance of the AwsClient for the given region */
  def initialize(region: Region): AwsClient =
    new AwsClient {
      val httpClientBuilder: UrlConnectionHttpClient.Builder =
        UrlConnectionHttpClient.builder()

      final override val currentRegion: Region = region

      final lazy val iam: IamClient = createIamClient(region, httpClientBuilder)
      final lazy val sts: StsClient = createStsClient(region, httpClientBuilder)
      final lazy val sqs: SqsClient = createSqsClient(region, httpClientBuilder)
      final lazy val dynamoDb: DynamoDbClient = createDynamoDbClient(region, httpClientBuilder)
      final lazy val secrets: SecretsManagerClient = createSecretsManagerClient(region, httpClientBuilder)
      final lazy val kms: KmsClient = createKmsClient(region, httpClientBuilder)
      final lazy val s3: S3Client = createS3Client(region, httpClientBuilder)
      final lazy val lambda: LambdaClient = createLambdaClient(region, httpClientBuilder)
      final lazy val apigateway: ApiGatewayClient = createApiGatewayClient(region, httpClientBuilder)
      final lazy val apigatewayv2: ApiGatewayV2Client = createApiGatewayV2Client(region, httpClientBuilder)
    }

  inline def initializeWithProperties(
      map: Map[String, String]
  ): AwsClient =
    initializeWithProperties(map.get)

  /** Creates new instance of the AwsClient for the given region using a function to retrieve system properties
    */
  def initializeWithProperties(
      maybeProperty: String => Option[String]
  ): AwsClient =
    new AwsClient {
      override def isDebugMode: Boolean =
        maybeProperty("AWS_CLIENT_DEBUG_MODE").contains("ON")

      if (isDebugMode)
      then
        println(
          s"${AnsiColor.YELLOW}[AwsClient]${AnsiColor.RESET}${AnsiColor.CYAN} Starting ...${AnsiColor.RESET}"
        )

      val httpClientBuilder: UrlConnectionHttpClient.Builder =
        UrlConnectionHttpClient.builder()

      val maybeCredentialsProvider: Option[AwsCredentialsProvider] =
        for {
          accessKeyId <- maybeProperty("AWS_ACCESS_KEY_ID")
          secretAccessKey <- maybeProperty("AWS_SECRET_ACCESS_KEY")
          sessionToken <- maybeProperty("AWS_SESSION_TOKEN")
        } yield {
          if (isDebugMode)
          then
            println(
              s"${AnsiColor.YELLOW}[AwsClient]${AnsiColor.RESET}${AnsiColor.CYAN} Credentials has been initialized${AnsiColor.RESET}"
            )
          new AwsCredentialsProvider {
            override def resolveCredentials(): AwsCredentials =
              AwsSessionCredentials.create(
                accessKeyId,
                secretAccessKey,
                sessionToken
              )
          }
        }

      final override val currentRegion: Region =
        maybeProperty("AWS_DEFAULT_REGION").map(Region.of).getOrElse(Region.US_EAST_1)

      if (isDebugMode)
      then
        println(
          s"${AnsiColor.YELLOW}[AwsClient]${AnsiColor.RESET}${AnsiColor.CYAN} Running in region $currentRegion${AnsiColor.RESET}"
        )

      final lazy val iam: IamClient =
        createIamClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val sts: StsClient =
        createStsClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val sqs: SqsClient =
        createSqsClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val dynamoDb: DynamoDbClient =
        createDynamoDbClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val secrets: SecretsManagerClient =
        createSecretsManagerClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val kms: KmsClient =
        createKmsClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val s3: S3Client =
        createS3Client(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val lambda: LambdaClient =
        createLambdaClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val apigateway: ApiGatewayClient =
        createApiGatewayClient(currentRegion, httpClientBuilder, maybeCredentialsProvider)
      final lazy val apigatewayv2: ApiGatewayV2Client =
        createApiGatewayV2Client(currentRegion, httpClientBuilder, maybeCredentialsProvider)
    }

  inline def maybe[R](using
      AwsClient
  )(block: AwsClient ?=> R): Either[Throwable, R] =
    try {
      Right(block)
    } catch {
      case e: AwsServiceException =>
        System.err.println(
          s"${RED}[ERROR] ${e.awsErrorDetails().errorMessage()}${RESET}"
        );
        Left(e)

      case NonFatal(e) =>
        System.err.println(
          s"${RED}[ERROR] ${e.getClass().getName()}: ${WHITE}${RED_B}${e
              .getMessage()}${RESET}"
        )
        Left(e)
    }

  inline def optionally[R](using
      AwsClient
  )(block: AwsClient ?=> Option[R]): Option[R] =
    try {
      block
    } catch {
      case e: AwsServiceException =>
        System.err.println(
          s"${RED}[ERROR] ${e.awsErrorDetails().errorMessage()}${RESET}"
        );
        None

      case NonFatal(e) =>
        System.err.println(
          s"${RED}[ERROR] ${e.getClass().getName()}: ${WHITE}${RED_B}${e
              .getMessage()}${RESET}"
        )
        None
    }

  inline def createStsClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): StsClient =
    StsClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .build()

  val tracingInterceptor = new ExecutionInterceptor() {
    override def modifyHttpRequest(
        context: ModifyHttpRequest,
        executionAttributes: ExecutionAttributes
    ): SdkHttpRequest =
      context
        .httpRequest()
        .toBuilder()
        .removeHeader("X-Amzn-Trace-Id")
        .build()
  }

  inline def createSqsClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): SqsClient =
    SqsClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def createDynamoDbClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): DynamoDbClient =
    DynamoDbClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .defaultsMode(DefaultsMode.IN_REGION)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def createIamClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): IamClient =
    IamClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .build()

  inline def createSecretsManagerClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): SecretsManagerClient =
    SecretsManagerClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def createKmsClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): KmsClient =
    KmsClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def createS3Client(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): S3Client =
    S3Client
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def createLambdaClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): LambdaClient =
    LambdaClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def createApiGatewayClient(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): ApiGatewayClient =
    ApiGatewayClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def createApiGatewayV2Client(
      region: Region,
      httpClientBuilder: UrlConnectionHttpClient.Builder,
      credentialsProvider: Option[AwsCredentialsProvider] = None
  ): ApiGatewayV2Client =
    ApiGatewayV2Client
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .optionalConfiguration(credentialsProvider, _.credentialsProvider)
      .region(region)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .addExecutionInterceptor(tracingInterceptor)
          .build()
      )
      .build()

  inline def newStatelessTestingStub(): AwsClientStatelessStub =
    AwsClientStatelessStub()

  inline def newStatefulTestingStub(): AwsClientStatefulStub =
    AwsClientStatefulStub()

}
