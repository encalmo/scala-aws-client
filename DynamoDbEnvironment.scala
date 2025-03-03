package org.encalmo.aws

trait DynamoDbEnvironment {
  def dynamodbTableArnPrefix: String
  def dynamodbTableArnSuffix: String
}

object DefaultDynamoDbEnvironment extends DynamoDbEnvironment {
  inline val dynamodbTableArnPrefix = ""
  inline val dynamodbTableArnSuffix = ""
}
