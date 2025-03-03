package org.encalmo.aws

import org.encalmo.aws.AwsClientStatelessStub.*
import org.encalmo.aws.AwsDynamoDbApi.*
import org.encalmo.aws.AwsDynamoDbApi.given
import org.encalmo.aws.AwsSecretsManagerApi.getSecretValueString

import software.amazon.awssdk.services.dynamodb.model.{AttributeValue}

import scala.jdk.CollectionConverters.*

class AwsClientStatelessStubSpec extends TestSuite {

  val item = DynamoDbItem("bar" -> "baz", "faz" -> "zoo")
  val item2 = DynamoDbItem("bar" -> "baz2", "faz" -> "zoo2")

  test("stub DynamoDb PutItem operation once") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub
      .expectPutItem(tableName = "foo", item = item)
    awsClientStub
      .expectPutItem(tableName = "foo", item = item2)

    putItemInTable("foo", item)
    intercept[DynamoDbPutItemNotExpected] {
      putItemInTable("foo", item)
    }
    putItemInTable("foo", item2)
    intercept[DynamoDbPutItemNotExpected] {
      putItemInTable("foo", item2)
    }
  }

  test("stub DynamoDb PutItem operation always") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub
      .expectPutItem(tableName = "foo", item = item, mode = Mode.ALWAYS)

    assertMultipleTimes { putItemInTable("foo", item) }
  }

  test("fail if DynamoDb PutItem operation not expected") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    intercept[DynamoDbPutItemNotExpected] {
      putItemInTable("foo", item)
    }
  }

  test("stub DynamoDb GetItem operation once") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub
      .expectGetItem(
        tableName = "foo",
        key = "bar" -> "baz",
        expectedItem = item,
        projection = Seq("bar")
      )
    awsClientStub
      .expectGetItem(
        tableName = "foo",
        key = "bar" -> "baz2",
        expectedItem = item2
      )

    assertEquals(
      obtained = getItemFromTable("foo", "bar" -> "baz", Seq("bar")).get,
      expected = item
    )
    intercept[DynamoDbGetItemNotExpected] {
      getItemFromTable("foo", "bar" -> "baz")
    }
    assertNotEquals(
      obtained = getItemFromTable("foo", "bar" -> "baz2").get,
      expected = item
    )
    intercept[DynamoDbGetItemNotExpected] {
      getItemFromTable("foo", "bar" -> "baz2")
    }
  }

  test("stub DynamoDb GetItem operation with projection and reserved words") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    val item = DynamoDbItem("bar" -> "baz", "status" -> "active")
    awsClientStub
      .expectGetItem(
        tableName = "foo",
        key = "bar" -> "baz",
        expectedItem = item,
        projection = Seq("status")
      )

    assertEquals(
      obtained = getItemFromTable("foo", "bar" -> "baz", Seq("status")).get,
      expected = item
    )
  }

  test("fail if DynamoDb GetItem operation not expected") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    intercept[DynamoDbGetItemNotExpected] {
      getItemFromTable("foo", "bar" -> "baz")
    }
  }

  test("stub DynamoDb DeleteItem operation once") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub
      .expectDeleteItem(
        tableName = "foo",
        key = "bar" -> "baz"
      )
    awsClientStub
      .expectDeleteItem(
        tableName = "foo",
        key = "bar" -> "baz2"
      )

    deleteItemFromTable("foo", "bar" -> "baz")
    intercept[DynamoDbDeleteItemNotExpected] {
      deleteItemFromTable("foo", "bar" -> "baz")
    }
    deleteItemFromTable("foo", "bar" -> "baz2")
    intercept[DynamoDbDeleteItemNotExpected] {
      deleteItemFromTable("foo", "bar" -> "baz2")
    }
  }

  test("fail if DynamoDb DeleteItem operation not expected") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    intercept[DynamoDbDeleteItemNotExpected] {
      deleteItemFromTable("foo", "bar" -> "baz")
    }
  }

  test("stub DynamoDb UpdateItem operation once") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub
      .expectUpdateItem(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
      )
    awsClientStub
      .expectUpdateItem(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zab", "zoo" -> "zoz")
      )

    updateItemInTable(
      tableName = "foo",
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
    )
    intercept[DynamoDbUpdateItemNotExpected] {
      updateItemInTable(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
      )
    }
    updateItemInTable(
      tableName = "foo",
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate("faz" -> "zab", "zoo" -> "zoz")
    )
    intercept[DynamoDbUpdateItemNotExpected] {
      updateItemInTable(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zab", "zoo" -> "zoz")
      )
    }
    intercept[DynamoDbUpdateItemNotExpected] {
      updateItemInTable(
        tableName = "foo",
        key = "bar" -> "baz2",
        update = DynamoDbItemUpdate("faz" -> "zaz")
      )
    }
  }

  test("fail if DynamoDb UpdateItem operation not expected") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    intercept[DynamoDbUpdateItemNotExpected] {
      updateItemInTable(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
      )
    }
  }

  test("stub multiple DynamoDb operations once") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub
      .expectPutItem(tableName = "foo", item = item)
    awsClientStub
      .expectGetItem(
        tableName = "foo",
        key = "bar" -> "baz",
        expectedItem = item
      )
    awsClientStub
      .expectDeleteItem(
        tableName = "foo",
        key = "bar" -> "baz"
      )
    awsClientStub
      .expectUpdateItem(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
      )

    putItemInTable("foo", item)
    assertEquals(
      obtained = getItemFromTable("foo", "bar" -> "baz").get,
      expected = item
    )
    deleteItemFromTable("foo", "bar" -> "baz")
    updateItemInTable(
      tableName = "foo",
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
    )
    assertMultipleTimes {
      intercept[DynamoDbPutItemNotExpected] {
        putItemInTable("foo", item)
      }
      intercept[DynamoDbGetItemNotExpected] {
        getItemFromTable("foo", "bar" -> "baz")
      }
      intercept[DynamoDbDeleteItemNotExpected] {
        deleteItemFromTable("foo", "bar" -> "baz")
      }
      intercept[DynamoDbUpdateItemNotExpected] {
        updateItemInTable(
          tableName = "foo",
          key = "bar" -> "baz",
          update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
        )
      }
    }
  }

  test("stub multiple DynamoDb operations in always mode") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub
      .expectPutItem(tableName = "foo", item = item, mode = Mode.ALWAYS)
    awsClientStub
      .expectGetItem(
        tableName = "foo",
        key = "bar" -> "baz",
        expectedItem = item,
        mode = Mode.ALWAYS
      )
    awsClientStub
      .expectDeleteItem(
        tableName = "foo",
        key = "bar" -> "baz",
        mode = Mode.ALWAYS
      )
    awsClientStub
      .expectUpdateItem(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz"),
        mode = Mode.ALWAYS
      )
    assertMultipleTimes {
      putItemInTable("foo", item)
      assertEquals(
        obtained = getItemFromTable("foo", "bar" -> "baz").get,
        expected = item
      )
      deleteItemFromTable("foo", "bar" -> "baz")
      updateItemInTable(
        tableName = "foo",
        key = "bar" -> "baz",
        update = DynamoDbItemUpdate("faz" -> "zaa", "zoo" -> "zaz")
      )
    }
  }

  test("parse UpdateExpression into attribute value updates") {
    val attributeUpdates =
      AwsDynamoDbApi.AttributeUpdate.parseUpdateExpression(
        updateExpression =
          "SET #status = :newStatus, carrier = :carrier, lastUpdated = :lastUpdated ADD count :one REMOVE total",
        attributeNames = Map("#status" -> "status"),
        attributeValues = Map(
          ":newStatus" -> "sent",
          ":carrier" -> mapOf("name" -> "Twilio"),
          ":lastUpdated" -> 818281928,
          ":one" -> 1
        )
      )
    assert(attributeUpdates.exists(_ == "count" -> add(1)))
    assert(
      attributeUpdates.exists(_ == "status" -> toUpdate(fromString("sent")))
    )
    assert(
      attributeUpdates.exists(
        _ == "carrier" -> toUpdate(DynamoDbItem("name" -> "Twilio"))
      )
    )
    assert(
      attributeUpdates.exists(
        _ == "lastUpdated" -> toUpdate(fromLong(818281928))
      )
    )
    assert(
      attributeUpdates.exists(_ == "total" -> toUpdate(REMOVE))
    )
  }

  test("stub secret value") {
    given awsClientStub: AwsClientStatelessStub =
      AwsClient.newStatelessTestingStub()
    awsClientStub.expectGetSecretValueString("foo", "bar")
    assertEquals(getSecretValueString("foo"), "bar")
    intercept[GetSecretValueNotExpected] {
      getSecretValueString("bar")
    }
  }

}
