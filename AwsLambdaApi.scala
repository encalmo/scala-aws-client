package org.encalmo.aws

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.lambda.model.*

import scala.jdk.CollectionConverters.*

object AwsLambdaApi {

  /** Creates a Lambda function */
  inline def createFunction(
      name: String,
      architecture: Architecture,
      awsRuntime: Runtime,
      handler: String,
      role: String,
      code: FunctionCode
  )(using
      aws: AwsClient
  ): CreateFunctionResponse =
    AwsClient.invoke(s"createFunction") {
      aws.lambda
        .createFunction(
          CreateFunctionRequest
            .builder()
            .functionName(name)
            .architectures(architecture)
            .runtime(awsRuntime)
            .role(role)
            .code(code)
            .handler(handler)
            .build()
        )
    }

  /** Invokes a Lambda function. You can invoke a function synchronously (and wait for the response), or asynchronously.
    * By default, Lambda invokes your function synchronously (i.e. theInvocationType is RequestResponse). To invoke a
    * function asynchronously, set InvocationType to Event. Lambda passes the ClientContext object to your function for
    * synchronous invocations only.
    */
  inline def invokeLambda(name: String, invocationType: InvocationType, payload: String)(using
      aws: AwsClient
  ): InvokeResponse =
    AwsClient.invoke(
      s"Invoke '$name' lambda${invocationType match { case InvocationType.EVENT => " async"; case _ => "" }} with payload: $payload"
    ) {
      aws.lambda
        .invoke(
          InvokeRequest
            .builder()
            .functionName(name)
            .payload(SdkBytes.fromUtf8String(payload))
            .invocationType(invocationType)
            .build()
        )
    }

  /** Returns a list of Lambda functions, with the version-specific configuration of each. Lambda returns up to 50
    * functions per call.
    */
  inline def listFunctions()(using aws: AwsClient): Seq[FunctionConfiguration] =
    AwsClient.invoke(s"listFunctions") {
      aws.lambda
        .listFunctions()
        .functions()
        .asScala
        .toSeq
    }

  /** Returns information about the function or function version, with a link to download the deployment package that's
    * valid for 10 minutes. If you specify a function version, only details that are specific to that version are
    * returned.
    *
    * @param name
    *   The name of the Lambda function, version, or alias. Name formats:
    *   - Function name – my-function (name-only), my-function:v1 (with alias).
    *   - Function ARN – arn:aws:lambda:us-west-2:123456789012:function:my-function.
    *   - Partial ARN – 123456789012:function:my-function. You can append a version number or alias to any of the
    *     formats.
    * @return
    *   The configuration of the function or version.
    */
  inline def getFunctionConfiguration(
      name: String
  )(using aws: AwsClient): FunctionConfiguration =
    AwsClient.invoke(s"getFunctionConfiguration") {
      aws.lambda
        .getFunction(GetFunctionRequest.builder().functionName(name).build())
        .configuration()
    }

  /** Returns the deployment package of the function or version. */
  inline def getFunctionCodeLocation(
      name: String
  )(using aws: AwsClient): FunctionCodeLocation =
    AwsClient.invoke(s"getFunctionCodeLocation") {
      aws.lambda
        .getFunction(GetFunctionRequest.builder().functionName(name).build())
        .code()
    }

  /** Returns a list of aliases for a Lambda function. */
  inline def getFunctionAliases(
      lambdaArn: String
  )(using aws: AwsClient): Seq[AliasConfiguration] =
    AwsClient.invoke(s"getFunctionAliases") {
      aws.lambda
        .listAliases(ListAliasesRequest.builder().functionName(lambdaArn).build())
        .aliases()
        .asScala
        .toSeq
    }

  /** Returns a list of versions, with the version-specific configuration of each. Lambda returns up to 50 versions per
    * call.
    */
  inline def getFunctionVersions(
      lambdaArn: String
  )(using aws: AwsClient): Seq[FunctionConfiguration] =
    AwsClient.invoke(s"getFunctionVersions") {
      aws.lambda
        .listVersionsByFunction(
          ListVersionsByFunctionRequest.builder().functionName(lambdaArn).build()
        )
        .versions()
        .asScala
        .toSeq
    }

  /** Returns a function's tags. */
  inline def listLambdaTags(
      lambdaArn: String
  )(using aws: AwsClient): Seq[(String, String)] =
    AwsClient.invoke(s"listLambdaTags") {
      aws.lambda
        .listTags(ListTagsRequest.builder().resource(lambdaArn).build())
        .tags()
        .asScala
        .toSeq
    }

  /** Add tags to lambda. */
  inline def tagLambda(
      lambdaArn: String,
      tags: Map[String, String]
  )(using aws: AwsClient): Unit = {
    AwsClient.invoke(s"tagLambda") {
      aws.lambda
        .tagResource(
          TagResourceRequest
            .builder()
            .resource(lambdaArn)
            .tags(tags.asJava)
            .build()
        )
    }
  }

  /** Modify the version-specific settings of a Lambda function.
    *
    * When you update a function, Lambda provisions an instance of the function and its supporting resources. If your
    * function connects to a VPC, this process can take a minute. During this time, you can't modify the function, but
    * you can still invoke it.
    *
    * @param runtime
    *   The identifier of the function's runtime. Runtime is required if the deployment package is a .zip file archive.
    *   Specifying a runtime results in an error if you're deploying a function using a container image.
    * @param handler
    *   The name of the method within your code that Lambda calls to run your function. Handler is required if the
    *   deployment package is a .zip file archive. The format includes the file name. It can also include namespaces and
    *   other qualifiers, depending on the runtime
    * @param timeout
    *   The amount of time (in seconds) that Lambda allows a function to run before stopping it. The default is 3
    *   seconds. The maximum allowed value is 900 seconds.
    * @param memorySize
    *   The amount of memory available to the function at runtime. Increasing the function memory also increases its CPU
    *   allocation. The default value is 128 MB. The value can be any multiple of 1 MB.
    * @param snapStart
    *   The function's SnapStart setting.
    */
  inline def updateFunctionConfiguration(
      lambdaArn: String,
      runtime: Option[Runtime] = None,
      handler: Option[String] = None,
      memorySize: Option[Int] = None,
      timeout: Option[Int] = None,
      snapStart: Option[SnapStart] = None
  )(using aws: AwsClient) =
    AwsClient.invoke(s"updateFunctionConfiguration") {
      val (revisionId, codeSha256) = {
        val response = aws.lambda
          .updateFunctionConfiguration(
            UpdateFunctionConfigurationRequest
              .builder()
              .functionName(lambdaArn)
              .optionally(runtime, _.runtime)
              .optionally(handler, _.handler)
              .optionally(memorySize, b => i => b.memorySize(Integer.valueOf(i)))
              .optionally(timeout, b => i => b.timeout(Integer.valueOf(i)))
              .optionally(snapStart, b => s => b.snapStart(s))
              .build()
          )
        (response.revisionId(), response.codeSha256())
      }
      val status = aws.lambda
        .waiter()
        .waitUntilFunctionUpdatedV2(
          GetFunctionRequest.builder().functionName(lambdaArn).build()
        )
        .matched()
      if (status.response().isPresent()) {
        (revisionId, codeSha256)
      } else {
        throw status.exception().get()
      }
    }

  /** Updates a Lambda function's code. If code signing is enabled for the function, the code package must be signed by
    * a trusted publisher.
    *
    * @param architecture
    *   The instruction set architecture that the function supports, arm64 or x86_64
    * @param publish
    *   Set to true to publish a new version of the function after updating the code. This has the same effect as
    *   calling PublishVersion separately.
    */
  inline def updateFunctionCode(
      lambdaArn: String,
      architecture: String,
      zipFile: SdkBytes,
      publish: Boolean
  )(using aws: AwsClient): (String, String) =
    AwsClient.invoke(s"updateFunctionCode") {
      val (revisionId, codeSha256) = {
        val response = aws.lambda
          .updateFunctionCode(
            UpdateFunctionCodeRequest
              .builder()
              .functionName(lambdaArn)
              .architecturesWithStrings(architecture)
              .publish(publish)
              .zipFile(zipFile)
              .build()
          )
        (response.revisionId(), response.codeSha256())
      }
      val status = aws.lambda
        .waiter()
        .waitUntilFunctionUpdatedV2(
          GetFunctionRequest.builder().functionName(lambdaArn).build()
        )
        .matched()
      if (status.response().isPresent()) {
        (revisionId, codeSha256)
      } else {
        throw status.exception().get()
      }
    }

  inline def updateFunctionCodeUsingS3Object(
      lambdaArn: String,
      architecture: String,
      bucketName: String,
      objectKey: String,
      publish: Boolean
  )(using aws: AwsClient): (String, String) =
    AwsClient.invoke(s"updateFunctionCodeUsingS3Object") {
      val (revisionId, codeSha256) = {
        val response = aws.lambda
          .updateFunctionCode(
            UpdateFunctionCodeRequest
              .builder()
              .functionName(lambdaArn)
              .architecturesWithStrings(architecture)
              .publish(publish)
              .s3Bucket(bucketName)
              .s3Key(objectKey)
              .build()
          )
        (response.revisionId(), response.codeSha256())
      }
      val status = aws.lambda
        .waiter()
        .waitUntilFunctionUpdatedV2(
          GetFunctionRequest.builder().functionName(lambdaArn).build()
        )
        .matched()
      if (status.response().isPresent()) {
        (revisionId, codeSha256)
      } else {
        throw status.exception().get()
      }
    }

  /** Returns a function's environment variables. */
  inline def getFunctionEnvironmentVariables(
      lambdaArn: String
  )(using aws: AwsClient): Map[String, String] =
    AwsClient.invoke(s"getFunctionEnvironmentVariables") {
      Option(
        aws.lambda
          .getFunctionConfiguration(
            GetFunctionConfigurationRequest
              .builder()
              .functionName(lambdaArn)
              .build()
          )
          .environment()
      )
        .map(_.variables().asScala.toMap)
        .getOrElse(Map.empty)
    }

  /** Modify the version-specific environment of a Lambda function.
    */
  inline def updateFunctionEnvironmentVariables(
      lambdaArn: String,
      variables: Map[String, String]
  )(using aws: AwsClient): String =
    AwsClient.invoke(s"updateFunctionEnvironmentVariables") {
      val revisionId = aws.lambda
        .updateFunctionConfiguration(
          UpdateFunctionConfigurationRequest
            .builder()
            .functionName(lambdaArn)
            .environment(
              Environment.builder().variables(variables.asJava).build()
            )
            .build()
        )
        .revisionId()
      val status = aws.lambda
        .waiter()
        .waitUntilFunctionUpdatedV2(
          GetFunctionRequest.builder().functionName(lambdaArn).build()
        )
        .matched()
      if (status.response().isPresent()) {
        revisionId
      } else {
        throw status.exception().get()
      }
    }

  /** Creates a version from the current code and configuration of a function.
    */
  inline def publishNewVersion(
      lambdaArn: String,
      description: String
  )(using aws: AwsClient): String =
    AwsClient.invoke(s"publishNewVersion") {
      val version = aws.lambda
        .publishVersion(
          PublishVersionRequest
            .builder()
            .functionName(lambdaArn)
            .description(description)
            .build()
        )
        .version()
      val status = aws.lambda
        .waiter()
        .waitUntilPublishedVersionActive(
          GetFunctionConfigurationRequest
            .builder()
            .functionName(lambdaArn)
            .build()
        )
        .matched()
      if (status.response().isPresent()) {
        version
      } else {
        throw status.exception().get()
      }
    }

  /** Updates the configuration of a Lambda function alias.
    */
  inline def updateAlias(
      lambdaArn: String,
      aliasName: String,
      version: String
  )(using aws: AwsClient): String =
    AwsClient.invoke(s"updateAlias") {
      aws.lambda
        .updateAlias(
          UpdateAliasRequest
            .builder()
            .functionName(lambdaArn)
            .name(aliasName)
            .functionVersion(version: String)
            .build()
        )
        .aliasArn()
    }

}
