package org.encalmo.aws

import software.amazon.awssdk.services.apigateway.model.*

import scala.jdk.CollectionConverters.*

object AwsApiGatewayApi {

  /** Lists the RestApis resources for your collection. */
  def listRestApis()(using aws: AwsClient): Seq[RestApi] = {
    aws.apigateway.getRestApis().items().asScala.toSeq
  }

  /** Lists resources. */
  def listResources(restApiId: String)(using aws: AwsClient): Seq[Resource] = {
    aws.apigateway
      .getResourcesPaginator(
        GetResourcesRequest
          .builder()
          .restApiId(restApiId)
          .embed("methods")
          .build()
      )
      .iterator()
      .asScala
      .flatMap(_.items().asScala)
      .toSeq
  }

  /** Get resource. */
  def getResource(restApiId: String, resourceId: String)(using aws: AwsClient): GetResourceResponse = {
    aws.apigateway.getResource(
      GetResourceRequest
        .builder()
        .restApiId(restApiId)
        .resourceId(resourceId)
        .embed("methods")
        .build()
    )
  }

  /** Get integration. */
  def getIntegration(restApiId: String, resourceId: String, method: String)(using
      aws: AwsClient
  ): GetIntegrationResponse = {
    aws.apigateway.getIntegration(
      GetIntegrationRequest
        .builder()
        .restApiId(restApiId)
        .httpMethod(method)
        .build()
    )
  }

  /** Creates new resource. */
  def createResource(restApiId: String, parentResourceId: String, pathPart: String)(using
      aws: AwsClient
  ): String = {
    aws.apigateway
      .createResource(
        CreateResourceRequest
          .builder()
          .restApiId(restApiId)
          .parentId(parentResourceId: String)
          .pathPart(pathPart)
          .build()
      )
      .id()
  }

  /* Adds a new Model resource to an existing RestApi. */
  def createModel(
      restApiId: String,
      name: String,
      description: String,
      schema: String,
      contentType: String = "application/json"
  )(using
      aws: AwsClient
  ): String = {
    aws.apigateway
      .createModel(
        CreateModelRequest
          .builder()
          .restApiId(restApiId)
          .name(name)
          .contentType(contentType)
          .description(description)
          .schema(schema)
          .build()
      )
      .name()
  }

  /** Add a method to an existing Resource resource. */
  def addMethod(
      restApiId: String,
      resourceId: String,
      httpMethod: String,
      apiKeyRequired: Boolean,
      operationName: String,
      requestModels: Map[String, String] = Map.empty
  )(using
      aws: AwsClient
  ) = {
    aws.apigateway.putMethod(
      PutMethodRequest
        .builder()
        .restApiId(restApiId)
        .resourceId(resourceId)
        .httpMethod(httpMethod)
        .authorizationType("NONE")
        .apiKeyRequired(apiKeyRequired: Boolean)
        .operationName(operationName)
        .requestModels(requestModels.asJava)
        .build()
    )
  }

  private val integrationRequestDataMappingExpresssionPrefixes =
    Set(
      "stageVariables.",
      "method.request.path.",
      "method.request.querystring.",
      "method.request.multivaluequerystring.",
      "method.request.header.",
      "method.request.multivalueheader.",
      "method.request.body",
      "context."
    )

  private def maybeQuote(value: String): String =
    if (
      integrationRequestDataMappingExpresssionPrefixes
        .exists(p => value.startsWith(p))
    ) then value
    else s"'$value'"

  /** Adds a new method with HTTP integration */
  def addHttpIntegration(
      restApiId: String,
      resourceId: String,
      httpMethod: String,
      integrationHttpMethod: String,
      uri: String,
      pathParameters: Map[String, String] = Map.empty,
      headers: Map[String, String] = Map.empty,
      queryParameters: Map[String, String] = Map.empty,
      requestTemplates: Map[String, String] = Map.empty,
      credentials: Option[String] = None
  )(using
      aws: AwsClient
  ): PutIntegrationResponse = {
    val requestParameters =
      pathParameters.map((name, value) => (s"integration.request.path.$name", maybeQuote(value)))
        ++ headers.map((name, value) => (s"integration.request.header.$name", maybeQuote(value)))
        ++ queryParameters.map((name, value) => (s"integration.request.querystring.$name", maybeQuote(value)))

    aws.apigateway.putIntegration(
      PutIntegrationRequest
        .builder()
        .restApiId(restApiId)
        .resourceId(resourceId)
        .`type`(IntegrationType.HTTP)
        .httpMethod(httpMethod)
        .integrationHttpMethod(integrationHttpMethod)
        .connectionType(ConnectionType.INTERNET)
        .uri(uri)
        .passthroughBehavior("WHEN_NO_TEMPLATES")
        .requestParameters(requestParameters.toMap.asJava)
        .requestTemplates(requestTemplates.asJava)
        .optionally(credentials, _.credentials)
        .build()
    )
  }

  /** Adds a new method with AWS_PROXY integration */
  def addAwsIntegration(
      restApiId: String,
      resourceId: String,
      httpMethod: String,
      integrationHttpMethod: String,
      uri: String,
      pathParameters: Map[String, String] = Map.empty,
      headers: Map[String, String] = Map.empty,
      queryParameters: Map[String, String] = Map.empty,
      requestTemplates: Map[String, String] = Map.empty,
      credentials: Option[String] = None
  )(using
      aws: AwsClient
  ): PutIntegrationResponse = {
    val requestParameters =
      pathParameters.map((name, value) => (s"integration.request.path.$name", maybeQuote(value)))
        ++ headers.map((name, value) => (s"integration.request.header.$name", maybeQuote(value)))
        ++ queryParameters.map((name, value) => (s"integration.request.querystring.$name", maybeQuote(value)))
    aws.apigateway.putIntegration(
      PutIntegrationRequest
        .builder()
        .restApiId(restApiId)
        .resourceId(resourceId)
        .`type`(IntegrationType.AWS)
        .httpMethod(httpMethod)
        .integrationHttpMethod(integrationHttpMethod)
        .connectionType(ConnectionType.INTERNET)
        .uri(uri)
        .passthroughBehavior("WHEN_NO_TEMPLATES")
        .requestParameters(requestParameters.asJava)
        .requestTemplates(requestTemplates.asJava)
        .optionally(credentials, _.credentials)
        .build()
    )
  }

  /** Delete existing method integration */
  def deleteIntegration(
      restApiId: String,
      resourceId: String,
      httpMethod: String
  )(using
      aws: AwsClient
  ): DeleteIntegrationResponse = {
    aws.apigateway.deleteIntegration(
      DeleteIntegrationRequest
        .builder()
        .restApiId(restApiId)
        .resourceId(resourceId)
        .httpMethod(httpMethod)
        .build()
    )
  }

  /** Generates a sample mapping template that can be used to transform a payload into the structure of a model. */
  def generateTemplateFromModel(
      restApiId: String,
      modelName: String
  )(using
      aws: AwsClient
  ) = {
    aws.apigateway.getModelTemplate(
      GetModelTemplateRequest
        .builder()
        .restApiId(restApiId)
        .modelName(modelName)
        .build()
    )
  }

  /** Add new integration response mapping */
  def addIntegrationResponse(
      restApiId: String,
      resourceId: String,
      httpMethod: String,
      statusCode: String,
      responseParameters: Map[String, String] = Map.empty,
      responseTemplates: Map[String, String] = Map.empty,
      selectionPattern: Option[String] = None
  )(using
      aws: AwsClient
  ) = {
    aws.apigateway.putIntegrationResponse(
      PutIntegrationResponseRequest
        .builder()
        .restApiId(restApiId)
        .resourceId(resourceId)
        .httpMethod(httpMethod)
        .statusCode(statusCode)
        .responseParameters(responseParameters.asJava)
        .responseTemplates(responseTemplates.asJava)
        .optionally(selectionPattern, _.selectionPattern)
        .build()
    )
  }

  def addMethodResponses(
      restApiId: String,
      resourceId: String,
      httpMethod: String,
      statusCodes: Seq[String]
  )(using
      aws: AwsClient
  ) =
    statusCodes
      .foreach(statusCode => addMethodResponse(restApiId, resourceId, httpMethod, statusCode))

  /** Add a method reponse type to an existing method. */
  def addMethodResponse(
      restApiId: String,
      resourceId: String,
      httpMethod: String,
      statusCode: String,
      responseModels: Map[String, String] = Map.empty,
      responseParameters: Map[String, Boolean] = Map.empty
  )(using
      aws: AwsClient
  ): PutMethodResponseResponse = {
    aws.apigateway.putMethodResponse(
      PutMethodResponseRequest
        .builder()
        .restApiId(restApiId)
        .resourceId(resourceId)
        .httpMethod(httpMethod)
        .statusCode(statusCode)
        .responseModels(responseModels.asJava)
        .responseParameters(responseParameters.view.view.mapValues(_.asInstanceOf[java.lang.Boolean]).toMap.asJava)
        .build()
    )
  }

  /** Creates a RequestValidator of a given RestApi. */
  def createRequestValidator(
      restApiId: String,
      name: String,
      validateRequestBody: Boolean,
      validateRequestParameters: Boolean
  )(using
      aws: AwsClient
  ): String = {
    aws.apigateway
      .createRequestValidator(
        CreateRequestValidatorRequest
          .builder()
          .restApiId(restApiId)
          .name(name)
          .validateRequestBody(validateRequestBody)
          .validateRequestParameters(validateRequestParameters)
          .build()
      )
      .id()
  }

  /** Gets information about one or more Stage resources. */
  def listStages(restApiId: String)(using aws: AwsClient): Seq[Stage] = {
    aws.apigateway
      .getStages(GetStagesRequest.builder().restApiId(restApiId).build())
      .item()
      .asScala
      .toSeq
  }

  /** Lists domain name resources. */
  def listDomainNames()(using aws: AwsClient): Seq[DomainName] = {
    aws.apigateway
      .getDomainNames(GetDomainNamesRequest.builder().build())
      .items()
      .asScala
      .toSeq
  }

  /** Lists BasePathMapping resources. */
  def listApiMappings(
      domainName: String
  )(using aws: AwsClient): Seq[BasePathMapping] = {
    aws.apigateway
      .getBasePathMappings(
        GetBasePathMappingsRequest
          .builder()
          .domainName(domainName)
          .build()
      )
      .items()
      .asScala
      .toSeq
  }

  /** Gets all the usage plans of the caller's account. */
  def listUsagePlans()(using aws: AwsClient): Seq[UsagePlan] = {
    aws.apigateway
      .getUsagePlans(GetUsagePlansRequest.builder().build())
      .items()
      .asScala
      .toSeq
  }

  /** Gets information about a Stage resource. */
  def getStage(restApiId: String, stageName: String)(using
      aws: AwsClient
  ): GetStageResponse = {
    aws.apigateway.getStage(
      GetStageRequest
        .builder()
        .restApiId(restApiId)
        .stageName(stageName)
        .build()
    )
  }

  /** Gets a usage plan of a given plan identifier. */
  def getUsagePlan(usagePlanId: String)(using
      aws: AwsClient
  ): GetUsagePlanResponse = {
    aws.apigateway.getUsagePlan(
      GetUsagePlanRequest
        .builder()
        .usagePlanId(usagePlanId: String)
        .build()
    )
  }

  /** Gets all the usage plan keys representing the API keys added to a specified usage plan.
    */
  def getUsagePlanKeys(usagePlanId: String)(using
      aws: AwsClient
  ): Seq[UsagePlanKey] = {
    aws.apigateway
      .getUsagePlanKeys(
        GetUsagePlanKeysRequest
          .builder()
          .usagePlanId(usagePlanId: String)
          .build()
      )
      .items()
      .asScala
      .toSeq
  }

  /** Gets a usage plan key of a given key identifier. */
  def getUsagePlanKey(usagePlanId: String, keyId: String)(using
      aws: AwsClient
  ): GetUsagePlanKeyResponse = {
    aws.apigateway.getUsagePlanKey(
      GetUsagePlanKeyRequest
        .builder()
        .usagePlanId(usagePlanId: String)
        .keyId(keyId)
        .build()
    )
  }

  /** Gets information about the current ApiKeys resource. */
  def getApiKeys()(using
      aws: AwsClient
  ): Seq[ApiKey] = {
    aws.apigateway
      .getApiKeys(
        GetApiKeysRequest.builder().includeValues(true).build()
      )
      .items()
      .asScala
      .toSeq
  }

  /** Gets information about the current ApiKey resource. */
  def getApiKey(apiKey: String)(using
      aws: AwsClient
  ): String = {
    aws.apigateway
      .getApiKey(
        GetApiKeyRequest
          .builder()
          .apiKey(apiKey: String)
          .includeValue(true)
          .build()
      )
      .value()
  }

  /** Exports a deployed version of a RestApi in a specified format. */
  def exportOpenApi3SpecWithAWSExtensions(
      restApiId: String,
      stageName: String
  )(using aws: AwsClient): String = {
    aws.apigateway
      .getExport(
        GetExportRequest
          .builder()
          .restApiId(restApiId)
          .stageName(stageName)
          .exportType("oas30")
          .parameters(Map("extensions" -> "apigateway").asJava)
          .build()
      )
      .body()
      .asUtf8String()
  }

}
