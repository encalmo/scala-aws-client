package org.encalmo.aws

import org.encalmo.aws.AwsLambdaApi.*

import software.amazon.awssdk.services.lambda.model.InvocationType

class AwsLambdaApiSpec extends TestSuite {

  test("invoka lambda sync") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    val event = """{"foo":"bar"}"""
    assertEquals(
      invokeLambda("remittances", InvocationType.REQUEST_RESPONSE, event)
        .payload()
        .asUtf8String,
      event
    )
  }

  test("invoka lambda async") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    val event = """{"foo":"bar"}"""
    assertEquals(
      invokeLambda("remittances", InvocationType.REQUEST_RESPONSE, event)
        .payload()
        .asUtf8String,
      event
    )
  }

  test("set custom handler stub") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    awsClientStub.lambda.setHandler((name, input) => "Hello World!")
    val event = """{"foo":"bar"}"""
    assertEquals(
      invokeLambda("remittances", InvocationType.REQUEST_RESPONSE, event)
        .payload()
        .asUtf8String,
      "Hello World!"
    )
  }

}
