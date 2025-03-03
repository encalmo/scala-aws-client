package org.encalmo.aws

import software.amazon.awssdk.services.apigatewayv2.model.*
import scala.jdk.CollectionConverters.*

object AwsApiGatewayV2Api {

  inline def getApis()(using aws: AwsClient): Seq[Api] =
    aws.apigatewayv2
      .getApis(GetApisRequest.builder().build())
      .items()
      .asScala
      .toSeq

}
