package org.encalmo.aws

import org.encalmo.aws.AwsClient.{initializeWithProperties, maybe, optionally}
import org.encalmo.aws.AwsDynamoDbApi.*
import org.encalmo.aws.AwsDynamoDbApi.given
import org.encalmo.aws.AwsSqsApi.listQueues
import org.encalmo.aws.AwsStsApi.getCallerAccountId

import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.concurrent.duration.Duration
import scala.io.AnsiColor.*
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

class AwsClientSpec extends TestSuite {

  override val munitTimeout = Duration(300, "s")

  final case class XYZ(foo: String, bar: Int)

  test("initialize aws client with custom properties") {

    given awsClient: AwsClient =
      initializeWithProperties(
        localAwsSecurityCredentials ++ debugModeProperties
      )

    assertEquals(AwsClient.currentRegion, Region.EU_CENTRAL_1)

    assert(maybe(getCallerAccountId()).isRight)

    assert(optionally(Some(getCallerAccountId())).isDefined)

    assert(AwsClient.callerAccountId.nonEmpty)

    assert(maybe(listTables()).isRight)

    assert(maybe(listQueues()).isRight)

  }

  test("test DynamoDb operations with simple key") {
    given awsClient: AwsClient =
      initializeWithProperties(
        localAwsSecurityCredentials ++ debugModeProperties
      )
    val tableName = "test-123-simple"
    val keyName = "testKey"
    val keyValue = "foo123"
    createTable(
      tableName = tableName,
      partitionKey = keyName,
      ignoreTableAlreadyExists = true,
      indexes = Seq(
        DynamoDbIndex(
          indexName = "index-foo",
          indexPartitionKey = "foo" -> "S",
          indexNonKeyAttributes = Seq("bar", "status")
        )
      )
    )
    listTables()
    putItemInTable(
      tableName,
      Map(
        keyName -> keyValue,
        "foo" -> "bar",
        "zoo" -> 456,
        "nested" -> DynamoDbItem("foo" -> 1),
        "list" -> listOf("a", "b", "c")
      )
    )
    putItemInTable(
      tableName,
      Map(keyName -> "foo1234", "foo" -> "baz", "zoo" -> 789)
    )
    val item = getItemFromTable(tableName, keyName -> keyValue).get
    assertEquals(item.maybeString("foo"), Some("bar"))
    assertEquals(item.maybeInt("zoo"), Some(456))
    assertEquals(
      item.maybeListOf[String]("list"),
      Some(Seq("a", "b", "c"))
    )
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "xyz" -> listOf(DynamoDbItem("foo" -> "a", "bar" -> 1), DynamoDbItem("foo" -> "b", "bar" -> 2))
      ),
      onlyIfRecordExists = true
    )
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "status" -> "a"
      ),
      onlyIfRecordExists = true
    )
    AwsClient.maybe(
      updateItemInTable(
        tableName,
        (keyName -> "foo12345"),
        Map(
          "status" -> "a"
        ),
        onlyIfRecordExists = true
      )
    )
    assertEquals(getItemFromTable(tableName, (keyName -> "foo12345")), None)
    AwsClient.maybe(
      updateItemInTable(
        tableName,
        (keyName -> "foo123456"),
        Map(
          "a" -> "status"
        ),
        onlyIfRecordExists = true
      )
    )
    assertEquals(getItemFromTable(tableName, (keyName -> "foo123456")), None)
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "xyz" -> listOf(DynamoDbItem("foo" -> "a", "bar" -> 1), DynamoDbItem("foo" -> "b", "bar" -> 2))
      )
    )
    val itemXYZ = getItemFromTable(tableName, keyName -> keyValue).get
    assertEquals(
      itemXYZ.maybeListOfClass[XYZ]("xyz"),
      Some(Seq(XYZ("a", 1), XYZ("b", 2)))
    )
    assertEquals(
      itemXYZ.maybeClass[XYZ]("xyz"),
      None
    )
    val updatedItem1 = updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "bar" -> DynamoDbItem("f1" -> 1, "f2" -> "2"),
        "nested.foo" -> fromString("here"),
        "list" -> appendToList("d")
      ),
      returnUpdatedItem = true
    ).updatedItem
    assert(updatedItem1.isDefined)
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "list" -> prependToList("_"),
        "bar" -> ifNotExists("bar")
      ),
      returnUpdatedItem = true
    )
    val item2 = getItemFromTable(
      tableName,
      keyName -> keyValue
    ).get
    assertEquals(
      item2.maybeNestedItem("bar"),
      Some(DynamoDbItem("f1" -> 1, "f2" -> "2"))
    )
    assertEquals(
      item2.maybeListOf[String]("list"),
      Some(Seq("_", "a", "b", "c", "d"))
    )
    assertEquals(item2.maybeInt("zoo"), Some(456))
    queryTable(tableName, (keyName -> keyValue))
    assertEquals(
      queryTable(
        tableName,
        ("foo" -> "bar"),
        indexName = Some("index-foo")
      ).size,
      1
    )
    assertEquals(
      queryTableWithScrolling(
        tableName,
        ("foo" -> "bar"),
        indexName = Some("index-foo")
      ).items.size,
      1
    )

    assertEquals(
      queryTableAllPages(
        tableName,
        ("foo" -> "bar"),
        indexName = Some("index-foo")
      ).size,
      1
    )
    assertEquals(
      queryTableAllPages(
        tableName,
        ("foo" -> "bar"),
        indexName = Some("index-foo"),
        projection = Some(Seq("bar"))
      ).size,
      1
    )
    assertEquals(
      queryTable(
        tableName,
        ("foo" -> "baz"),
        indexName = Some("index-foo")
      ).size,
      1
    )
    assertEquals(
      queryTable(
        tableName,
        ("foo" -> "bas"),
        indexName = Some("index-foo")
      ).size,
      0
    )
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "list" -> removeFromList(0)
      )
    )
    assertEquals(
      getItemFromTable(tableName, keyName -> keyValue)
        .flatMap(_.maybe[String]("list", "0")),
      Some("a")
    )
    deleteItemFromTable(tableName, (keyName -> keyValue))
    assertEquals(getItemFromTable(tableName, (keyName -> keyValue)), None)
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "stringset" -> addToSet("foo")
      )
    )
    assertEquals(
      getItemFromTable(tableName, keyName -> keyValue)
        .flatMap(_.maybe[Set[String]]("stringset")),
      Some(Set("foo"))
    )
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "stringset" -> addToSet("bar")
      )
    )
    assertEquals(
      getItemFromTable(tableName, keyName -> keyValue)
        .flatMap(_.maybe[Set[String]]("stringset")),
      Some(Set("foo", "bar"))
    )
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "stringset" -> removeFromSet("foo")
      )
    )
    assertEquals(
      getItemFromTable(tableName, keyName -> keyValue)
        .flatMap(_.maybe[Set[String]]("stringset")),
      Some(Set("bar"))
    )
    updateItemInTable(
      tableName,
      (keyName -> keyValue),
      Map(
        "stringset" -> removeFromSet(Set("bar"))
      )
    )
    assertEquals(
      getItemFromTable(tableName, keyName -> keyValue)
        .flatMap(_.maybe[Set[String]]("stringset")),
      None
    )
    scanTable(tableName)
    scanTable(tableName, filters = Seq(FilterCondition.attributeNotExists("zoo")))
    assertEquals(
      scanTable(
        tableName,
        indexName = Some("index-foo")
      ).size,
      1
    )
    assertEquals(
      scanTableWithScrolling(
        tableName,
        indexName = Some("index-foo")
      ).items.size,
      1
    )
    assertEquals(
      scanTableAllPages(
        tableName,
        indexName = Some("index-foo")
      ).size,
      1
    )
    assertEquals(
      scanTableAllPages(
        tableName,
        indexName = Some("index-foo"),
        projection = Some(Seq("bar"))
      ).size,
      1
    )
    assertEquals(
      scanTable(
        tableName,
        indexName = Some("index-foo"),
        filters = Seq(FilterCondition.attributeNotExists("zoo"))
      ).size,
      1
    )
    assertEquals(
      scanTable(
        tableName,
        indexName = Some("index-foo")
      ).size,
      1
    )
    deleteTable(tableName)
  }

  test("test DynamoDb operations with composite key") {
    given awsClient: AwsClient =
      initializeWithProperties(
        localAwsSecurityCredentials ++ debugModeProperties
      )
    val tableName = "test-123-composite"
    val keyName = "testKey"
    val keyValue = "foo123"
    createTable(
      tableName = tableName,
      partitionKey = keyName,
      sortKey = Some("zoo" -> "N"),
      ignoreTableAlreadyExists = true,
      indexes = Seq(
        DynamoDbIndex(
          indexName = "index-foo",
          indexPartitionKey = "foo" -> "S",
          indexSortKey = Some("zoo" -> "N"),
          indexNonKeyAttributes = Seq("bar")
        )
      )
    )
    putItemInTable(
      tableName,
      Map(keyName -> keyValue, "foo" -> "bar", "zoo" -> 456)
    )
    putItemInTable(
      tableName,
      Map(keyName -> "foo1234", "foo" -> "baz", "zoo" -> 789)
    )
    val item =
      getItemFromTableUsingCompositeKey(
        tableName,
        Seq(keyName -> keyValue, "zoo" -> 456)
      ).get
    assertEquals(item.maybeString("foo"), Some("bar"))
    assertEquals(item.maybeInt("zoo"), Some(456))
    updateItemInTableUsingCompositeKey(
      tableName,
      Seq(keyName -> keyValue, "zoo" -> 456),
      Map("bar" -> DynamoDbItem("f1" -> 1, "f2" -> "2")),
      onlyIfRecordExists = true
    )
    AwsClient.maybe(
      updateItemInTableUsingCompositeKey(
        tableName,
        Seq(keyName -> "foo-12345", "zoo" -> 456),
        Map("bar" -> DynamoDbItem("f1" -> 1, "f2" -> "2")),
        onlyIfRecordExists = true
      )
    )
    assertEquals(getItemFromTableUsingCompositeKey(tableName, Seq(keyName -> "foo-12345", "zoo" -> 456)), None)
    AwsClient.maybe(
      updateItemInTableUsingCompositeKey(
        tableName,
        Seq(keyName -> "foo-123456", "zoo" -> 456),
        Map("status" -> "a"),
        onlyIfRecordExists = true
      )
    )
    assertEquals(getItemFromTableUsingCompositeKey(tableName, Seq(keyName -> "foo-123456", "zoo" -> 456)), None)
    updateItemInTableUsingCompositeKey(
      tableName,
      Seq(keyName -> keyValue, "zoo" -> 456),
      Map("status" -> "a"),
      onlyIfRecordExists = true
    )
    updateItemInTableUsingCompositeKey(
      tableName,
      Seq(keyName -> keyValue, "zoo" -> 456),
      Map("bar" -> DynamoDbItem("f1" -> 1, "f2" -> "2"))
    )
    val item2 = getItemFromTableUsingCompositeKey(
      tableName,
      Seq(keyName -> keyValue, "zoo" -> 456)
    ).get
    assertEquals(
      item2.maybeNestedItem("bar"),
      Some(DynamoDbItem("f1" -> 1, "f2" -> "2"))
    )
    assertEquals(item2.maybeInt("zoo"), Some(456))
    queryTable(tableName, (keyName -> keyValue))
    assertEquals(
      queryTable(
        tableName = tableName,
        partitionKey = keyName -> keyValue,
        sortKey = Some(KeyCondition.equals("zoo", 456))
      ).size,
      1
    )
    assertEquals(
      queryTable(
        tableName = tableName,
        partitionKey = "foo" -> "bar",
        sortKey = Some(KeyCondition.equals("zoo", 456)),
        indexName = Some("index-foo")
      ).size,
      1
    )
    assertEquals(
      queryTable(
        tableName,
        ("foo" -> "baz"),
        sortKey = Some(KeyCondition.equals("zoo", 456)),
        indexName = Some("index-foo")
      ).size,
      0
    )
    assertEquals(
      queryTable(
        tableName,
        ("foo" -> "bar"),
        sortKey = Some(KeyCondition.equals("zoo", 789)),
        indexName = Some("index-foo")
      ).size,
      0
    )
    deleteItemFromTableUsingCompositeKey(
      tableName,
      Seq(keyName -> keyValue, "zoo" -> 456)
    )
    assertEquals(
      getItemFromTableUsingCompositeKey(
        tableName,
        Seq(keyName -> keyValue, "zoo" -> 456)
      ),
      None
    )
    deleteTable(tableName)
  }

}
