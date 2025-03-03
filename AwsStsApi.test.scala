package org.encalmo.aws

import org.encalmo.aws.AwsStsApi.*

class AwsStsApiSpec extends TestSuite {

  test("invoka lambda sync") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    awsClientStub.sts.setAwsAccountId("foo")
    assertEquals(
      getCallerAccountId(),
      "foo"
    )
  }

}
