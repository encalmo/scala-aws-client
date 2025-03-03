package org.encalmo.aws

object AwsStsApi {

  inline def getCallerAccountId()(using AwsClient): String =
    summon[AwsClient].sts.getCallerIdentity().account()

}
