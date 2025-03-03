package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.*
import org.encalmo.aws.AwsDynamoDbApi.given
import org.encalmo.aws.AwsSecretsManagerApi.*
import org.encalmo.aws.InMemoryDynamoDb.*

import software.amazon.awssdk.services.dynamodb.model.{AttributeValue}

import scala.jdk.CollectionConverters.*

class AwsClientStatefulStubSpec extends TestSuite {

  val foo = "foo"
  val undefined = "undefined"
  val item1 = DynamoDbItem(
    "bar" -> "baz",
    "faz" -> 5,
    "zoo" -> false,
    "map" -> mapOf("a" -> 1, "b" -> "2", "c" -> listOf(7, true, "c"))
  )
  val item1a = DynamoDbItem("bar" -> "baz", "faz" -> 1)
  val item2 = DynamoDbItem("bar" -> "boz", "fas" -> 0)
  val itemWrongKey = DynamoDbItem("bas" -> "baz", "faz" -> 5)

  test("create a table, then put, get, delete items") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    putItemInTable(tableName = foo, item = item1)
    putItemInTable(tableName = foo, item = item2)
    intercept[DynamoDbInvalidItemKey] {
      putItemInTable(tableName = foo, item = itemWrongKey)
    }
    intercept[DynamoDbUndefinedTable] {
      putItemInTable(tableName = undefined, item = item1)
    }
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(item1)
    )
    assertEquals(
      obtained = getItemFromTable(
        tableName = foo,
        key = "bar" -> "baz",
        projection = Seq("bar", "map.a", "map.c", "zoo", "status")
      ),
      expected = Some(
        DynamoDbItem(
          "bar" -> "baz",
          "zoo" -> false,
          "map" -> mapOf("a" -> 1, "c" -> listOf(7, true, "c"))
        )
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "boz"),
      expected = Some(item2)
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "bas"),
      expected = None
    )
    intercept[DynamoDbInvalidItemKey] {
      getItemFromTable(tableName = foo, key = "bas" -> "baz")
    }
    intercept[DynamoDbUndefinedTable] {
      getItemFromTable(tableName = undefined, key = "bar" -> "baz")
    }
    deleteItemFromTable(tableName = foo, key = "bar" -> "baz")
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = None
    )
    deleteItemFromTable(tableName = foo, key = "bar" -> "baz")
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = None
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "boz"),
      expected = Some(item2)
    )
    intercept[DynamoDbInvalidItemKey] {
      getItemFromTable(tableName = foo, key = "bas" -> "bas")
    }
    intercept[DynamoDbUndefinedTable] {
      deleteItemFromTable(tableName = undefined, key = "bar" -> "baz")
    }
    putItemInTable(tableName = foo, item = item1)
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(item1)
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "boz"),
      expected = Some(item2)
    )
    putItemInTable(tableName = foo, item = item1a)
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(item1a)
    )
  }

  test("put and update items in the table") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    putItemInTable(tableName = foo, item = item1)
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(item1)
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "faz" -> 567,
        "zoo" -> true
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(
        DynamoDbItem(
          "bar" -> "baz",
          "faz" -> 567,
          "zoo" -> true,
          "map" -> mapOf("a" -> 1, "b" -> "2", "c" -> listOf(7, true, "c"))
        )
      )
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "faz" -> REMOVE,
        "zoo" -> 0
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(
        DynamoDbItem(
          "bar" -> "baz",
          "zoo" -> 0,
          "map" -> mapOf("a" -> 1, "b" -> "2", "c" -> listOf(7, true, "c"))
        )
      )
    )
  }

  test("update items in the empty table") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "faz" -> 567,
        "zoo" -> true
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "faz" -> 567, "zoo" -> true))
    )
  }

  test("append to the list in the update") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    putItemInTable(
      tableName = foo,
      item = DynamoDbItem(
        "bar" -> "baz",
        "list" -> listOf("a")
      )
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> appendToList("b")
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> listOf("a", "b")))
    )
  }

  test("prepend to the list in the update") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    putItemInTable(
      tableName = foo,
      item = DynamoDbItem(
        "bar" -> "baz",
        "list" -> listOf("a")
      )
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> prependToList("b")
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> listOf("b", "a")))
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> removeFromList(0)
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> listOf("a")))
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> removeFromList(0)
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> listOf()))
    )
  }

  test("set if not exists in the update") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    putItemInTable(
      tableName = foo,
      item = DynamoDbItem(
        "bar" -> "baz",
        "list" -> listOf("a")
      )
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> ifNotExists("b")
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> listOf("a")))
    )
  }

  test("set if not exists in the update (2)") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    putItemInTable(
      tableName = foo,
      item = DynamoDbItem(
        "bar" -> "baz"
      )
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> ifNotExists(listOf("b"))
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> listOf("b")))
    )
  }

  test("append to the set in the update") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    createTable(tableName = foo, partitionKey = "bar")
    putItemInTable(
      tableName = foo,
      item = DynamoDbItem(
        "bar" -> "baz"
      )
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> addToSet("b")
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> setOfStrings("b")))
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> addToSet("a")
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> setOfStrings("b", "a")))
    )
    updateItemInTable(
      tableName = foo,
      key = "bar" -> "baz",
      update = DynamoDbItemUpdate(
        "list" -> removeFromSet("b")
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName = foo, key = "bar" -> "baz"),
      expected = Some(DynamoDbItem("bar" -> "baz", "list" -> setOfStrings("a")))
    )
  }

  test("get and put a secret value") {
    given awsClientStub: AwsClientStatefulStub =
      AwsClient.newStatefulTestingStub()
    awsClientStub.setupSecrets(Map("zoo" -> "zaz"))
    assertEquals(getSecretValueString("zoo"), "zaz")
    putSecretValue("zoo", "zaa")
    assertEquals(getSecretValueString("zoo"), "zaa")
  }

  test("beginsWithSyntax") {
    assert(
      Condition.beginsWithSyntax.matches(
        "begins_with( #ref , :value )"
      )
    )
    assert(
      Condition.beginsWithSyntax.matches("begins_with(#ref,:value)")
    )
    assert(
      Condition.beginsWithSyntax.matches(
        "begins_with( #ref,:value )"
      )
    )
    assert(
      !Condition.beginsWithSyntax.matches(
        "contains( #ref,:value )"
      )
    )
  }

  test("containsSyntax") {
    assert(
      Condition.containsSyntax.matches(
        "contains( #ref , :value )"
      )
    )
    assert(
      Condition.containsSyntax.matches("contains(#ref,:value)")
    )
    assert(
      Condition.containsSyntax.matches(
        "contains( #ref,:value )"
      )
    )
    assert(
      !Condition.containsSyntax.matches(
        "begins_with( #ref,:value )"
      )
    )
  }

  test("sizeSyntax") {
    assert(
      Condition.sizeSyntax.matches(
        "size( #ref)"
      )
    )
    assert(
      Condition.sizeSyntax.matches("size(#ref)")
    )
    assert(
      Condition.sizeSyntax.matches(
        "size( #ref )"
      )
    )
    assert(
      !Condition.sizeSyntax.matches(
        "begins_with( #ref,:value )"
      )
    )
  }

  test("attributeExistsSyntax") {
    assert(
      Condition.attributeExistsSyntax.matches(
        "attribute_exists( #ref )"
      )
    )
    assert(
      Condition.attributeExistsSyntax.matches("attribute_exists(#ref)")
    )
    assert(
      Condition.attributeExistsSyntax.matches(
        "attribute_exists(name)"
      )
    )
    assert(
      !Condition.attributeExistsSyntax.matches(
        "attribute_not_exists(name)"
      )
    )
  }

  test("attributeNotExistsSyntax") {
    assert(
      Condition.attributeNotExistsSyntax.matches(
        "attribute_not_exists( #ref )"
      )
    )
    assert(
      Condition.attributeNotExistsSyntax.matches("attribute_not_exists(#ref)")
    )
    assert(
      Condition.attributeNotExistsSyntax.matches(
        "attribute_not_exists(name)"
      )
    )
    assert(
      !Condition.attributeNotExistsSyntax.matches(
        "attribute_exists(name)"
      )
    )
  }

  test("attributeTypeSyntax") {
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,S)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,SS)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,N)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,SS)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,B)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,BS)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,BOOL)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,M)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,L)"))
    assert(Condition.attributeTypeSyntax.matches("attribute_type(#ref,NULL)"))
    assert(
      !Condition.attributeTypeSyntax.matches(
        "attribute_exists(name)"
      )
    )
  }

  test("parse key condition expression") {
    assertEquals(
      InMemoryDynamoDb.parseKeyConditionExpression(
        "  #foo =  :foo ",
        Map("#foo" -> "fooName"),
        Map(":foo" -> fromString("Hello!"))
      ),
      KeyConditionExpression(
        ("fooName", CompareFunction.EQUAL, Seq(fromString("Hello!")))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseKeyConditionExpression(
        "  #foo =  :foo and  #bar  = :bar ",
        Map("#foo" -> "fooName", "#bar" -> "barName"),
        Map(":foo" -> fromString("Hello!"), ":bar" -> fromInt(123))
      ),
      KeyConditionExpression(
        ("fooName", CompareFunction.EQUAL, Seq(fromString("Hello!"))),
        ("barName", CompareFunction.EQUAL, Seq(fromInt(123)))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseKeyConditionExpression(
        " #foo =  :foo and  #bar  < :bar ",
        Map("#foo" -> "fooName", "#bar" -> "barName"),
        Map(":foo" -> fromString("Hello!"), ":bar" -> fromInt(123))
      ),
      KeyConditionExpression(
        ("fooName", CompareFunction.EQUAL, Seq(fromString("Hello!"))),
        ("barName", CompareFunction.LOWER_THAN, Seq(fromInt(123)))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseKeyConditionExpression(
        " #foo =  :foo and  #bar  BETWEEN :bar1   AND  :bar2 ",
        Map("#foo" -> "fooName", "#bar" -> "barName"),
        Map(
          ":foo" -> fromString("Hello!"),
          ":bar1" -> fromInt(123),
          ":bar2" -> fromInt(456)
        )
      ),
      KeyConditionExpression(
        ("fooName", CompareFunction.EQUAL, Seq(fromString("Hello!"))),
        ("barName", CompareFunction.BETWEEN, Seq(fromInt(123), fromInt(456)))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseKeyConditionExpression(
        " #foo =  :foo and  begins_with( #bar , :bar ) ",
        Map("#foo" -> "fooName", "#bar" -> "barName"),
        Map(
          ":foo" -> fromString("Hello!"),
          ":bar" -> fromInt(123)
        )
      ),
      KeyConditionExpression(
        ("fooName", CompareFunction.EQUAL, Seq(fromString("Hello!"))),
        ("barName", CompareFunction.BEGINS_WITH, Seq(fromInt(123)))
      )
    )
  }

  test("parse filter expression") {
    assertEquals(
      InMemoryDynamoDb.parseFilterExpression(
        "  #foo =  :foo ",
        Map("#foo" -> "fooName"),
        Map(":foo" -> fromString("Hello!"))
      ),
      Seq(
        ("fooName", CompareFunction.EQUAL, Seq(fromString("Hello!")))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseFilterExpression(
        "  #foo =  :foo and bar < :bar and #c > :cc",
        Map("#foo" -> "fooName", "#c" -> "ccc"),
        Map(
          ":foo" -> fromString("Hello!"),
          ":bar" -> fromInt(123),
          ":cc" -> fromInt(0)
        )
      ),
      Seq(
        ("fooName", CompareFunction.EQUAL, Seq(fromString("Hello!"))),
        ("bar", CompareFunction.LOWER_THAN, Seq(fromInt(123))),
        ("ccc", CompareFunction.GREATER_THAN, Seq(fromInt(0)))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseFilterExpression(
        "  #foo IN (:foo1, :foo2, :foo3) ",
        Map("#foo" -> "fooName"),
        Map(
          ":foo1" -> fromString("Hello"),
          ":foo2" -> fromString("World!"),
          ":foo3" -> fromString("It's me.")
        )
      ),
      Seq(
        (
          "fooName",
          CompareFunction.IN,
          Seq(fromString("Hello"), fromString("World!"), fromString("It's me."))
        )
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseFilterExpression(
        " begins_with(#foo,:foo) ",
        Map("#foo" -> "fooName"),
        Map(":foo" -> fromString("Hel"))
      ),
      Seq(
        ("fooName", CompareFunction.BEGINS_WITH, Seq(fromString("Hel")))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseFilterExpression(
        " contains(#foo,:foo) ",
        Map("#foo" -> "fooName"),
        Map(":foo" -> fromString("Hel"))
      ),
      Seq(
        ("fooName", CompareFunction.CONTAINS, Seq(fromString("Hel")))
      )
    )
    assertEquals(
      InMemoryDynamoDb.parseFilterExpression(
        " attribute_exists(#foo) ",
        Map("#foo" -> "fooName"),
        Map(":foo" -> fromString("Hel"))
      ),
      Seq(
        ("fooName", CompareFunction.ATTRIBUTE_EXISTS, Seq.empty)
      )
    )
  }

  val itemA = DynamoDbItemKey("foo" -> "bar", "zoo" -> 123)
  val itemB = DynamoDbItemKey("foo" -> "bar", "zoo" -> 456)
  val itemC = DynamoDbItemKey("foo" -> "baz", "zoo" -> 123)
  val itemD = DynamoDbItemKey("foo" -> "bar", "zoo" -> fromString("Hello!"))
  val itemE = DynamoDbItemKey("foo" -> "bat", "zoo" -> fromString("World"))
  val itemF = DynamoDbItemKey("foo" -> "bak", "zoo" -> fromString("It's me."))

  test("Simple KeyConditionExpression matches EQUALS") {

    val simpleKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar")))
    )
    assertEquals(simpleKce.matches(itemA), true)
    assertEquals(simpleKce.matches(itemB), true)
    assertEquals(simpleKce.matches(itemC), false)
    assertEquals(simpleKce.matches(itemD), true)

  }
  test("Simple KeyConditionExpression not matches EQUAL") {
    val simpleKce = KeyConditionExpression(
      ("bar", CompareFunction.EQUAL, Seq(fromString("bar")))
    )
    assertEquals(simpleKce.matches(itemA), false)
    assertEquals(simpleKce.matches(itemB), false)
    assertEquals(simpleKce.matches(itemC), false)
    assertEquals(simpleKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches EQUAL") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.EQUAL, Seq(fromInt(123)))
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches EQUAL") {
    val complexKce = KeyConditionExpression(
      KeyCondition.equals("foo", "bar"),
      KeyCondition.equals("zoo", 123)
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches LOWER_THAN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.LOWER_THAN, Seq(fromInt(456)))
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches LOWER_THAN") {
    val complexKce = KeyConditionExpression(
      KeyCondition.equals("foo", "bar"),
      KeyCondition.lowerThan("zoo", 456)
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches LOWER_THAN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.LOWER_THAN, Seq(fromInt(123)))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches LOWER_THAN_OR_EQUAL") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.LOWER_THAN_OR_EQUAL, Seq(fromInt(456)))
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), true)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches LOWER_THAN_OR_EQUAL") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.LOWER_THAN_OR_EQUAL, Seq(fromInt(123)))
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches GREATER_THAN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.GREATER_THAN, Seq(fromInt(456)))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches GREATER_THAN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.GREATER_THAN, Seq(fromInt(123)))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), true)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches GREATER_THAN_OR_EQUAL") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.GREATER_THAN_OR_EQUAL, Seq(fromInt(456)))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), true)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches GREATER_THAN_OR_EQUAL") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.GREATER_THAN_OR_EQUAL, Seq(fromInt(123)))
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), true)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches BETWEEN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.BETWEEN, Seq(fromInt(123), fromInt(456)))
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches BETWEEN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.BETWEEN, Seq(fromInt(123), fromInt(457)))
    )
    assertEquals(complexKce.matches(itemA), true)
    assertEquals(complexKce.matches(itemB), true)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches BETWEEN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.BETWEEN, Seq(fromInt(124), fromInt(456)))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches BETWEEN") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("baz"))),
      ("zoo", CompareFunction.BETWEEN, Seq(fromInt(0), fromInt(1000)))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), true)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression not matches BEGINS_WITH") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("baz"))),
      ("zoo", CompareFunction.BEGINS_WITH, Seq(fromString("1")))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), false)
  }
  test("Complex KeyConditionExpression matches BEGINS_WITH") {
    val complexKce = KeyConditionExpression(
      ("foo", CompareFunction.EQUAL, Seq(fromString("bar"))),
      ("zoo", CompareFunction.BEGINS_WITH, Seq(fromString("Hel")))
    )
    assertEquals(complexKce.matches(itemA), false)
    assertEquals(complexKce.matches(itemB), false)
    assertEquals(complexKce.matches(itemC), false)
    assertEquals(complexKce.matches(itemD), true)
  }

  test("query the table using partition key") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionKey"
    createTable(tableName, partitionKey = "foo")
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar3", "zoo" -> 125))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar4", "zoo" -> 126))

    assertEquals(
      queryTable(tableName, partitionKey = "foo" -> fromString("bar1")),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    )
    assertEquals(
      queryTable(tableName, partitionKey = "foo" -> fromString("bar2")),
      Seq(DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    )
  }
  test("ConditionExpression matches NOT_EQUAL") {
    val condition =
      ("foo", CompareFunction.NOT_EQUAL, Seq(fromString("bar")))
    assertEquals(condition.matches(itemA), false)
    assertEquals(condition.matches(itemB), false)
    assertEquals(condition.matches(itemC), true)
    assertEquals(condition.matches(itemD), false)
  }
  test("ConditionExpression not matches NOT_EQUAL") {
    val condition =
      ("foo", CompareFunction.NOT_EQUAL, Seq(fromString("baz")))
    assertEquals(condition.matches(itemA), true)
    assertEquals(condition.matches(itemB), true)
    assertEquals(condition.matches(itemC), false)
    assertEquals(condition.matches(itemD), true)
  }
  test("ConditionExpression matches IN") {
    val condition =
      (
        "foo",
        CompareFunction.IN,
        Seq(fromString("baz"), fromString("bas"), fromString("bar"))
      )
    assertEquals(condition.matches(itemA), true)
    assertEquals(condition.matches(itemB), true)
    assertEquals(condition.matches(itemC), true)
    assertEquals(condition.matches(itemD), true)
    assertEquals(condition.matches(itemE), false)
    assertEquals(condition.matches(itemF), false)
  }
  test("ConditionExpression matches string CONTAINS string") {
    val condition =
      (
        "zoo",
        CompareFunction.CONTAINS,
        Seq(fromString("l"))
      )
    assertEquals(condition.matches(DynamoDbItem("zoo" -> "l")), false)
    assertEquals(condition.matches(DynamoDbItem("zoo" -> "al")), true)
    assertEquals(condition.matches(DynamoDbItem("zoo" -> "la")), true)
    assertEquals(condition.matches(DynamoDbItem("zoo" -> "ala")), true)
    assertEquals(condition.matches(DynamoDbItem("zoo" -> "k")), false)
  }
  test("ConditionExpression matches strings list CONTAINS string") {
    val condition =
      (
        "zoo",
        CompareFunction.CONTAINS,
        Seq(fromString("l"))
      )
    assertEquals(
      condition.matches(DynamoDbItem("zoo" -> Seq("l", "m", "n"))),
      true
    )
    assertEquals(
      condition.matches(DynamoDbItem("zoo" -> Seq("la", "ma", "na"))),
      false
    )
    assertEquals(
      condition.matches(DynamoDbItem("zoo" -> fromInt(1))),
      false
    )
    assertEquals(
      condition.matches(DynamoDbItem("zoo" -> fromBoolean(true))),
      false
    )
  }
  test("ConditionExpression matches numbers list CONTAINS number") {
    val condition =
      (
        "zoo",
        CompareFunction.CONTAINS,
        Seq(fromDouble(12.34d))
      )
    assertEquals(
      condition.matches(DynamoDbItem("zoo" -> Seq(12d, 12.3d, 12.34d))),
      true
    )
    assertEquals(
      condition.matches(
        DynamoDbItem("zoo" -> Seq(12d, 12.3d, 12.33d, 12.35d, 13d))
      ),
      false
    )
  }
  test("ConditionExpression matches numbers list CONTAINS list") {
    val condition =
      (
        "zoo",
        CompareFunction.CONTAINS,
        Seq(fromIterable[String](Seq("a", "b")))
      )
    assertEquals(
      condition.matches(
        DynamoDbItem(
          "zoo" -> Seq(Seq("a"), Seq("a", "b"), Seq("a", "b", "c"))
            .map(fromIterableOfString)
        )
      ),
      true
    )
  }
  test("ConditionExpression matches ATTRIBUTE_EXISTS") {
    val condition =
      (
        "zoo",
        CompareFunction.ATTRIBUTE_EXISTS,
        Seq.empty
      )
    assertEquals(condition.matches(DynamoDbItem("zoo" -> 1)), true)
    assertEquals(condition.matches(DynamoDbItem("zo" -> 1)), false)
    assertEquals(condition.matches(DynamoDbItem("zuu" -> 1)), false)
    assertEquals(condition.matches(DynamoDbItem("zuu" -> 1, "zoo" -> 1)), true)
    assertEquals(condition.matches(DynamoDbItem("zuu" -> 1, "zo" -> 1)), false)
  }
  test("ConditionExpression matches ATTRIBUTE_NOT_EXISTS") {
    val condition =
      (
        "zoo",
        CompareFunction.ATTRIBUTE_NOT_EXISTS,
        Seq.empty
      )
    assertEquals(condition.matches(DynamoDbItem("zoo" -> 1)), false)
    assertEquals(condition.matches(DynamoDbItem("zo" -> 1)), true)
    assertEquals(condition.matches(DynamoDbItem("zuu" -> 1)), true)
    assertEquals(condition.matches(DynamoDbItem("zuu" -> 1, "zoo" -> 1)), false)
    assertEquals(condition.matches(DynamoDbItem("zuu" -> 1, "zo" -> 1)), true)
  }
  test("ConditionExpression matches ATTRIBUTE_TYPE") {
    val conditionS = FilterCondition.attributeType("zoo", "S")
    assertEquals(conditionS.matches(DynamoDbItem("zoo" -> 1)), false)
    assertEquals(conditionS.matches(DynamoDbItem("zoo" -> "1")), true)
    val conditionN = FilterCondition.attributeType("zoo", "N")
    assertEquals(conditionN.matches(DynamoDbItem("zoo" -> 1)), true)
    assertEquals(conditionN.matches(DynamoDbItem("zoo" -> "1")), false)
    val conditionL = FilterCondition.attributeType("zoo", "L")
    assertEquals(conditionL.matches(DynamoDbItem("zoo" -> 1)), false)
    assertEquals(conditionL.matches(DynamoDbItem("zoo" -> "1")), false)
    assertEquals(
      conditionL.matches(DynamoDbItem("zoo" -> setOfStrings("a"))),
      false
    )
    assertEquals(
      conditionL.matches(DynamoDbItem("zoo" -> setOfNumbers(1.1d, 2, 2.1d))),
      false
    )
    assertEquals(conditionL.matches(DynamoDbItem("zoo" -> listOf("S"))), true)
    val conditionM = FilterCondition.attributeType("zoo", "M")
    assertEquals(conditionM.matches(DynamoDbItem("zoo" -> 1)), false)
    assertEquals(conditionM.matches(DynamoDbItem("zoo" -> "1")), false)
    assertEquals(
      conditionM.matches(DynamoDbItem("zoo" -> setOfStrings("a"))),
      false
    )
    assertEquals(
      conditionM.matches(DynamoDbItem("zoo" -> setOfNumbers(1.1d, 2, 2.1d))),
      false
    )
    assertEquals(conditionM.matches(DynamoDbItem("zoo" -> listOf("S"))), false)
    assertEquals(
      conditionM.matches(DynamoDbItem("zoo" -> mapOf("S" -> "S"))),
      true
    )
    val conditionSS = FilterCondition.attributeType("zoo", "SS")
    assertEquals(conditionSS.matches(DynamoDbItem("zoo" -> 1)), false)
    assertEquals(conditionSS.matches(DynamoDbItem("zoo" -> "1")), false)
    assertEquals(
      conditionSS.matches(DynamoDbItem("zoo" -> setOfStrings("a"))),
      true
    )
    assertEquals(
      conditionSS.matches(DynamoDbItem("zoo" -> setOfNumbers(1.1d, 2, 2.1d))),
      false
    )
    val conditionNS = FilterCondition.attributeType("zoo", "NS")
    assertEquals(conditionNS.matches(DynamoDbItem("zoo" -> 1)), false)
    assertEquals(conditionNS.matches(DynamoDbItem("zoo" -> "1")), false)
    assertEquals(
      conditionNS.matches(DynamoDbItem("zoo" -> setOfStrings("a"))),
      false
    )
    assertEquals(
      conditionNS.matches(DynamoDbItem("zoo" -> setOfNumbers(1.1d, 2, 2.1d))),
      true
    )
  }

  test("query the table using partition and sort key equality") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndSortKey"
    createTable(tableName, partitionKey = "foo", sortKey = Some("zoo" -> "N"))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar3", "zoo" -> 125))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar4", "zoo" -> 126))

    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar1"),
        sortKey = Some(KeyCondition.equals("zoo", 123))
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    )
    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar1"),
        sortKey = Some(KeyCondition.equals("zoo", 124))
      ),
      Seq.empty
    )
    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar2"),
        sortKey = Some(KeyCondition.equals("zoo", 123))
      ),
      Seq.empty
    )
    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar2"),
        sortKey = Some(KeyCondition.equals("zoo", 124))
      ),
      Seq(DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    )
  }

  test("query the table using partition and sort key comparison LOWER_THAN") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndSortKey"
    createTable(tableName, partitionKey = "foo", sortKey = Some("zoo" -> "N"))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar3", "zoo" -> 125))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar4", "zoo" -> 126))

    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar1"),
        sortKey = Some(KeyCondition.lowerThan("zoo", 124))
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    )
    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar1"),
        sortKey = Some(KeyCondition.lowerThan("zoo", 123))
      ),
      Seq.empty
    )
  }

  test("query the index using partition key") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndIndex"
    createTable(
      tableName,
      partitionKey = "foo",
      indexes = Seq(
        DynamoDbIndex(indexName = "index-zoo", indexPartitionKey = "zoo" -> "S")
      )
    )
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar3", "zoo" -> 125))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar4", "zoo" -> 126))

    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar1")
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    )
    assertEquals(
      queryTable(
        tableName,
        indexName = Some("index-zoo"),
        partitionKey = "zoo" -> fromInt(123)
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    )
  }

  test("query the index using partition key with projection") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndIndex"
    createTable(
      tableName,
      partitionKey = "foo",
      indexes = Seq(
        DynamoDbIndex(
          indexName = "index-zoo-a",
          indexPartitionKey = "zoo" -> "N",
          indexNonKeyAttributes = Seq("a")
        ),
        DynamoDbIndex(
          indexName = "index-zoo-b",
          indexPartitionKey = "zoo" -> "N",
          indexNonKeyAttributes = Seq("b")
        )
      )
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa", "b" -> 11)
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar2", "zoo" -> 124, "a" -> "bb")
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar3", "zoo" -> 125, "b" -> 33)
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar4", "zoo" -> 126)
    )

    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar1")
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa", "b" -> 11))
    )
    assertEquals(
      queryTable(
        tableName,
        indexName = Some("index-zoo-a"),
        partitionKey = "zoo" -> fromInt(123)
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa"))
    )
    assertEquals(
      queryTable(
        tableName,
        indexName = Some("index-zoo-b"),
        partitionKey = "zoo" -> fromInt(123)
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "b" -> 11))
    )
  }

  test(
    "query the index using partition and sort key with projection and filter"
  ) {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndSortKeyAndIndex"
    createTable(
      tableName,
      partitionKey = "foo",
      indexes = Seq(
        DynamoDbIndex(
          indexName = "index-zoo",
          indexPartitionKey = "zoo" -> "N",
          indexSortKey = Some("b" -> "N"),
          indexNonKeyAttributes = Seq("a")
        )
      )
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa", "b" -> 11)
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar2", "zoo" -> 123, "a" -> "bb")
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar3", "zoo" -> 123, "b" -> 33)
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar4", "zoo" -> 123)
    )
    putItemInTable(
      tableName,
      DynamoDbItem("foo" -> "bar5", "zoo" -> 123, "a" -> "ba", "b" -> 11)
    )

    assertEquals(
      queryTable(
        tableName,
        partitionKey = "foo" -> fromString("bar1")
      ),
      Seq(DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa", "b" -> 11))
    )
    assertEquals(
      queryTable(
        tableName,
        indexName = Some("index-zoo"),
        partitionKey = "zoo" -> fromInt(123),
        sortKey = Some(KeyCondition.equals("b", 11))
      ),
      Seq(
        DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa", "b" -> 11),
        DynamoDbItem("foo" -> "bar5", "zoo" -> 123, "a" -> "ba", "b" -> 11)
      )
    )
    assertEquals(
      queryTable(
        tableName,
        indexName = Some("index-zoo"),
        partitionKey = "zoo" -> fromInt(123),
        sortKey = Some(KeyCondition.lowerThan("b", 100))
      ),
      Seq(
        DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa", "b" -> 11),
        DynamoDbItem("foo" -> "bar5", "zoo" -> 123, "a" -> "ba", "b" -> 11),
        DynamoDbItem("foo" -> "bar3", "zoo" -> 123, "b" -> 33)
      )
    )
    assertEquals(
      queryTable(
        tableName,
        indexName = Some("index-zoo"),
        partitionKey = "zoo" -> fromInt(123),
        sortKey = Some(KeyCondition.lowerThan("b", 100)),
        filters = Seq(FilterCondition.beginsWith("a", "a"))
      ),
      Seq(
        DynamoDbItem("foo" -> "bar1", "zoo" -> 123, "a" -> "aa", "b" -> 11)
      )
    )
  }

  test("scan the table") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndSortKey"
    createTable(tableName, partitionKey = "foo", sortKey = Some("zoo" -> "N"))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar3", "zoo" -> 125))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar4", "zoo" -> 126))

    assertEquals(
      scanTable(
        tableName
      ),
      Seq(
        DynamoDbItem("foo" -> "bar1", "zoo" -> 123),
        DynamoDbItem("foo" -> "bar2", "zoo" -> 124),
        DynamoDbItem("foo" -> "bar3", "zoo" -> 125),
        DynamoDbItem("foo" -> "bar4", "zoo" -> 126)
      )
    )
  }

  test("scan the table with projection") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndSortKey"
    createTable(tableName, partitionKey = "foo", sortKey = Some("zoo" -> "N"))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar3", "zoo" -> 125))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar4", "zoo" -> 126))

    assertEquals(
      scanTable(
        tableName,
        projection = Some(Seq("foo"))
      ),
      Seq(
        DynamoDbItem("foo" -> "bar1"),
        DynamoDbItem("foo" -> "bar2"),
        DynamoDbItem("foo" -> "bar3"),
        DynamoDbItem("foo" -> "bar4")
      )
    )
  }

  test("scan the index") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "tableWithPartitionAndIndex"
    createTable(
      tableName,
      partitionKey = "foo",
      indexes = Seq(
        DynamoDbIndex(indexName = "index-zoo", indexPartitionKey = "zoo" -> "S")
      )
    )
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar1", "zoo" -> 123))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar2", "zoo" -> 124))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar3"))
    putItemInTable(tableName, DynamoDbItem("foo" -> "bar4", "zoo" -> 126))

    assertEquals(
      scanTable(
        tableName,
        indexName = Some("index-zoo")
      ).toSet,
      Set(
        DynamoDbItem("foo" -> "bar1", "zoo" -> 123),
        DynamoDbItem("foo" -> "bar4", "zoo" -> 126),
        DynamoDbItem("foo" -> "bar2", "zoo" -> 124)
      )
    )
  }

  test("update items in the table using an expression") {
    given awsClientStub: AwsClient = AwsClient.newStatefulTestingStub()
    val tableName = "foo"
    createTable(tableName, partitionKey = "bar")
    val item = DynamoDbItem(
      "bar" -> "baz",
      "faz" -> 5,
      "zoo" -> false,
      "map" -> mapOf("a" -> 1, "b" -> "2", "c" -> listOf(7, true, "c"))
    )
    putItemInTable(tableName, item)
    assertEquals(
      obtained = getItemFromTable(tableName, key = "bar" -> "baz"),
      expected = Some(item)
    )
    updateItemInTable(
      tableName,
      key = "bar" -> "baz",
      update = Map(
        "faz" -> 567,
        "map.c[0]" -> 8,
        "map.d" -> "new"
      )
    )
    assertEquals(
      obtained = getItemFromTable(tableName, key = "bar" -> "baz").prettyPrint,
      expected = Some(
        DynamoDbItem(
          "bar" -> "baz",
          "faz" -> 567,
          "zoo" -> false,
          "map" -> mapOf(
            "a" -> 1,
            "b" -> "2",
            "c" -> listOf(8, true, "c"),
            "d" -> "new"
          )
        )
      ).prettyPrint
    )
  }

  test("pickling and unpickling") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    awsClientStub.dynamoDb.setup(
      Map(
        "foo" -> InMemoryDynamoDbTableDump(
          keySchema = Set("bar"),
          items = Map(
            DynamoDbItemKey("bar" -> "a") -> DynamoDbItem("aa" -> 1),
            DynamoDbItemKey("bar" -> "b") -> DynamoDbItem("bb" -> 2)
          )
        )
      )
    )
    val pickles = awsClientStub.dynamoDb.pickle()
    println(pickles)
    awsClientStub.dynamoDb.unpickle(pickles)
  }

  test("snapshot and diff") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    awsClientStub.dynamoDb.makeSnapshot("s1")
    awsClientStub.dynamoDb.setup(
      Map(
        "foo" -> InMemoryDynamoDbTableDump(
          keySchema = Set("bar"),
          items = Map(
            DynamoDbItemKey("bar" -> "a") -> DynamoDbItem("aa" -> 1),
            DynamoDbItemKey("bar" -> "b") -> DynamoDbItem("bb" -> 2)
          )
        )
      )
    )
    awsClientStub.dynamoDb.makeSnapshot("s2")
    updateItemInTable("foo", ("bar" -> "a"), Map("aa" -> 3, "ab" -> 0))
    awsClientStub.dynamoDb.makeSnapshot("s3")
    dynamoDbDiff("s2", "s3").foreach(println)
  }

  test("store and retrieve a message") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()

    val response = AwsSqsApi.sendMessage("foo.fifo", "Hello World!")

    val (message, messageId) = awsClientStub.sqs.peek("foo.fifo").get
    assertEquals(response.messageId(), messageId)
    assertEquals(message, "Hello World!")
  }

  test("unpickle tables") {
    given awsClientStub: AwsClientStatefulStub = AwsClient.newStatefulTestingStub()
    awsClientStub.dynamoDb.unpickle(pickledTables)
    assert(
      getItemFromTable(
        "arn:aws:dynamodb:us-east-1:694389950650:table/accounts-alpha",
        "accountId" -> "8bc0fb64-e0e7-40E1-998a-eadb561809f2"
      ).isDefined
    )
  }

  def pickledTables = """|{
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/accounts-alpha": {
                          |    "indexes": [
                          |      {
                          |        "indexName": "zeroHashParticipantCode-index",
                          |        "keySchema": [
                          |          "zeroHashParticipantCode"
                          |        ],
                          |        "nonKeyAttributes": [],
                          |        "projectionType": "ALL"
                          |      },
                          |      {
                          |        "indexName": "astraUserId-index",
                          |        "keySchema": [
                          |          "astraUserId"
                          |        ],
                          |        "nonKeyAttributes": [
                          |          "astraUserIntentId",
                          |          "phoneNumber"
                          |        ],
                          |        "projectionType": "INCLUDE"
                          |      },
                          |      {
                          |        "indexName": "astraUserIntentId-index",
                          |        "keySchema": [
                          |          "astraUserIntentId"
                          |        ],
                          |        "nonKeyAttributes": [
                          |          "astraUserId",
                          |          "phoneNumber"
                          |        ],
                          |        "projectionType": "INCLUDE"
                          |      }
                          |    ],
                          |    "items": [
                          |      [
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2"
                          |        },
                          |        {
                          |          "address": {
                          |            "city": "Pawnee",
                          |            "country": "US",
                          |            "line1": "123 Main St.",
                          |            "line2": null,
                          |            "postalCode": "46001",
                          |            "stateOrProvince": "IN"
                          |          },
                          |          "bankAccounts": [
                          |            {
                          |              "accessToken": "access-sandbox-f5cbdf3e-9796-4e13-84a6-064abb001531",
                          |              "accountName": "Plaid Checking",
                          |              "bankAccountId": "5qDQjGdNvNIw4pz3zKqEirj3NgBd7gc5XmoQG",
                          |              "institutionId": "ins_109508",
                          |              "institutionName": "First Platypus Bank",
                          |              "internalBankId": "cd5c45a2-2A4A-4228-a8c6-5c718163f5a6",
                          |              "mask": "0000",
                          |              "sardineProcessorToken": "processor-sandbox-9a1a733f-331b-41fc-a3ee-d8247c80eaf1",
                          |              "subtype": "checking",
                          |              "type": "depository"
                          |            }
                          |          ],
                          |          "createdAt": 1722954663,
                          |          "dailyLimitAmount": 60200,
                          |          "dateOfBirth": "1975-01-18",
                          |          "emailAddress": "caroline.bagby+1481635@zerohash.com",
                          |          "firstName": "Leslie",
                          |          "idNumber": "123456789",
                          |          "idType": "GOVERNMENT_ID",
                          |          "last4": "6789",
                          |          "lastName": "Knope",
                          |          "minimumTransactionIntervalSeconds": 60,
                          |          "monthlyLimitAmount": 110300,
                          |          "phoneNumber": "+15056447011",
                          |          "transactionLimitAmount": 50100,
                          |          "verificationIpAddress": "192.0.2.0",
                          |          "verificationProviderRefId": "5334a560-d5cb-47bf-bc72-882ed5d0bc95",
                          |          "weeklyLimitAmount": 60200
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "accountId"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/authorizations-alpha": {
                          |    "items": [
                          |      [
                          |        {
                          |          "phoneNumber": "+15056447011"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "createdAt": 1722954304,
                          |          "lastUpdated": 1722954661,
                          |          "status": "authorized"
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "phoneNumber"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/interbankExchangeRates-alpha": {
                          |    "items": [
                          |      [
                          |        {
                          |          "currency": "MXN"
                          |        },
                          |        {
                          |          "rate": 16.8827
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "currency": "PHP"
                          |        },
                          |        {
                          |          "rate": 55.9949366431
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "currency"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/messages-alpha": {
                          |    "items": [],
                          |    "keySchema": [
                          |      "messageId"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/parameters-alpha": {
                          |    "items": [
                          |      [
                          |        {
                          |          "parameter": "maximumNumberOfAccounts"
                          |        },
                          |        {
                          |          "value": "0"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "parameter": "meridianExchangeRateFeeBps"
                          |        },
                          |        {
                          |          "value": "250"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "parameter": "meridianTransactionFeeUSD"
                          |        },
                          |        {
                          |          "value": "100"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "parameter": "ssnDuplicationCheck"
                          |        },
                          |        {
                          |          "value": "OFF"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "parameter": "twoTxnFeeFree"
                          |        },
                          |        {
                          |          "value": "ON"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "parameter": "unsupportedStates"
                          |        },
                          |        {
                          |          "value": "{\"ZZ\"}"
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "parameter"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/payments-alpha": {
                          |    "indexes": [
                          |      {
                          |        "indexName": "providerTransactionId-index",
                          |        "keySchema": [
                          |          "providerTransactionId"
                          |        ],
                          |        "nonKeyAttributes": [],
                          |        "projectionType": "ALL"
                          |      }
                          |    ],
                          |    "items": [
                          |      [
                          |        {
                          |          "paymentId": "Ra1oau5heyj17lopl3rkszh0z2"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "amount": 100,
                          |          "applicationBaseUrl": "http://localhost:3000",
                          |          "brand": "gcash",
                          |          "createdAt": 1722954681,
                          |          "currency": "USD",
                          |          "currencyBaseExchangeRate": 55.9949366431,
                          |          "currencyExchangeRateFeeBps": 75,
                          |          "currencyInterbankExchangeRate": 55.9949366431,
                          |          "currencyLiveExchangeRate": 57.7954677989,
                          |          "environment": "LocalTestAlpha",
                          |          "events": [
                          |            {
                          |              "actor": "remittances:InitializePaymentTransactionHandler",
                          |              "event": "start",
                          |              "status": "TransactionRequested",
                          |              "timestamp": 1722954681
                          |            }
                          |          ],
                          |          "exchangeRate": 57.36200179040825,
                          |          "externalTransactionId": "d7f463f6-ca8e-4f36-ad3d-9b33e578e407",
                          |          "fee": 0,
                          |          "feeDiscount": {
                          |            "discountAmount": 100,
                          |            "discountPercentage": 100,
                          |            "reason": "GCASH_INITIAL_PROMOTION"
                          |          },
                          |          "internalBankId": "cd5c45a2-2A4A-4228-a8c6-5c718163f5a6",
                          |          "lastUpdated": 1722954681,
                          |          "originalFee": 100,
                          |          "paymentDatePrefix": "2024-08",
                          |          "paymentMethod": "ach",
                          |          "phoneNumber": "+15056447011",
                          |          "receiverFirstName": "Ludwick",
                          |          "receiverLastName": "Zemenhoff",
                          |          "receiverPhoneNumber": "+639291610998",
                          |          "sardineValidateResult": "\"Unexpected error response\"",
                          |          "status": "SardineValidationError",
                          |          "total": 100,
                          |          "transactionType": "remittance",
                          |          "validUntil": 1723559481,
                          |          "wallet": "gcash",
                          |          "walletAmount": 5737,
                          |          "walletCurrency": "PHP"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "paymentId": "Ra23hfnslcqy07prt8f2szq87y"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "amount": 100,
                          |          "applicationBaseUrl": "http://localhost:3000",
                          |          "brand": "gcash",
                          |          "createdAt": 1722954705,
                          |          "currency": "USD",
                          |          "currencyBaseExchangeRate": 55.9949366431,
                          |          "currencyExchangeRateFeeBps": 75,
                          |          "currencyInterbankExchangeRate": 55.9949366431,
                          |          "currencyLiveExchangeRate": 57.7954677989,
                          |          "environment": "LocalTestAlpha",
                          |          "events": [
                          |            {
                          |              "actor": "remittances:InitializePaymentTransactionHandler",
                          |              "event": "start",
                          |              "status": "TransactionRequested",
                          |              "timestamp": 1722954705
                          |            }
                          |          ],
                          |          "exchangeRate": 57.36200179040825,
                          |          "externalTransactionId": "8b37e1dc-8b99-48b6-b88a-f32dc4a4c449",
                          |          "fee": 0,
                          |          "feeDiscount": {
                          |            "discountAmount": 100,
                          |            "discountPercentage": 100,
                          |            "reason": "GCASH_INITIAL_PROMOTION"
                          |          },
                          |          "internalBankId": "cd5c45a2-2A4A-4228-a8c6-5c718163f5a6",
                          |          "lastUpdated": 1722954705,
                          |          "originalFee": 100,
                          |          "paymentDatePrefix": "2024-08",
                          |          "paymentMethod": "ach",
                          |          "phoneNumber": "+15056447011",
                          |          "receiverFirstName": "Ludwick",
                          |          "receiverLastName": "Zemenhoff",
                          |          "receiverPhoneNumber": "+639291610998",
                          |          "sardineValidateResult": "{\"sessionKey\":\"827cd335-7ad8-4715-94be-a82eb6e32dcb\",\"level\":\"high\",\"status\":\"Success\",\"customer\":{\"score\":0,\"level\":\"high\",\"reasonCodes\":[\"TAG\"],\"signals\":[{\"key\":\"addressRiskLevel\",\"value\":\"low\",\"reasonCodes\":[\"AL4\",\"AL5\",\"AL9\"]},{\"key\":\"adverseMediaLevel\",\"value\":\"low\"},{\"key\":\"bankLevel\",\"value\":\"low\"},{\"key\":\"emailDomainLevel\",\"value\":\"low\"},{\"key\":\"emailLevel\",\"value\":\"low\"},{\"key\":\"nsfLevel\",\"value\":\"low\"},{\"key\":\"pepLevel\",\"value\":\"low\"},{\"key\":\"phoneCarrier\",\"value\":\"Commio, LLC\"},{\"key\":\"phoneLevel\",\"value\":\"high\",\"reasonCodes\":[\"TAG\"]},{\"key\":\"phoneLineType\",\"value\":\"NonFixedVoIP\"},{\"key\":\"sanctionLevel\",\"value\":\"low\"}],\"phone\":{\"nameScore\":-1,\"addressScore\":-1},\"address\":{\"validity\":\"unknown\"},\"bank\":{\"isNachaDebitCompatible\":\"true\",\"nsf\":{\"score\":0,\"normalizedScore\":0}}},\"transaction\":{\"level\":\"high\",\"amlLevel\":\"low\",\"indemnification\":{\"decision\":\"approved\",\"instantLimit\":6000,\"holdAmount\":0}},\"checkpoints\":{\"aml\":{\"riskLevel\":{\"value\":\"low\",\"ruleIds\":null}},\"ach\":{\"riskLevel\":{\"value\":\"low\",\"ruleIds\":null}},\"customer\":{\"customerPurchaseLevel\":{\"value\":\"low\",\"ruleIds\":[\"120\",\"124\"]},\"historicalLevel\":{\"value\":\"low\",\"ruleIds\":[\"118\",\"124\"]},\"phoneLevel\":{\"value\":\"high\",\"ruleIds\":[\"22\"]},\"riskLevel\":{\"value\":\"high\",\"ruleIds\":[\"22\"]}}},\"rules\":[{\"id\":137,\"isLive\":false,\"isAllowlisted\":false,\"name\":\"IRS CTR Structuring from SSNs suspected to be stolen or synthetic fraud\"},{\"id\":124,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"always true\"},{\"id\":125,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Default: low phone risk level \"},{\"id\":27,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Phone number in medium trust\"},{\"id\":22,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Address doesn't match\"},{\"id\":118,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Long time user (>45 days) & purchase history \"},{\"id\":120,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"customer score low\"},{\"id\":143,\"isLive\":false,\"isAllowlisted\":false,\"name\":\"us phone\"},{\"id\":2438,\"isLive\":false,\"isAllowlisted\":false,\"name\":\"us phone 222\"},{\"id\":126508,\"isLive\":false,\"isAllowlisted\":false,\"name\":\"plaid identity name match\"}],\"checkpointData\":[{\"name\":\"ach\",\"type\":\"weighted_max\"},{\"name\":\"aml\",\"type\":\"weighted_max\"},{\"name\":\"customer\",\"type\":\"weighted_max\"}],\"sardineCustomerId\":\"75ce715b-2e84-4315-ba56-12e5d1c5275d\",\"sardineFlowId\":\"b91197bf-93d3-4235-ae91-123a12244516\",\"sardineSessionToken\":\"827cd335-7ad8-4715-94be-a82eb6e32dcb\"}",
                          |          "status": "SardineRiskPassed",
                          |          "total": 100,
                          |          "transactionType": "remittance",
                          |          "validUntil": 1723559505,
                          |          "wallet": "gcash",
                          |          "walletAmount": 5737,
                          |          "walletCurrency": "PHP"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "paymentId": "Rcigjmed9kd1zirjcd7jjv6nae"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "amount": 100,
                          |          "applicationBaseUrl": "http://localhost:3000",
                          |          "brand": "gcash",
                          |          "createdAt": 1722954698,
                          |          "currency": "USD",
                          |          "currencyBaseExchangeRate": 55.9949366431,
                          |          "currencyExchangeRateFeeBps": 75,
                          |          "currencyInterbankExchangeRate": 55.9949366431,
                          |          "currencyLiveExchangeRate": 57.7954677989,
                          |          "environment": "LocalTestAlpha",
                          |          "events": [
                          |            {
                          |              "actor": "remittances:InitializePaymentTransactionHandler",
                          |              "event": "start",
                          |              "status": "TransactionRequested",
                          |              "timestamp": 1722954698
                          |            }
                          |          ],
                          |          "exchangeRate": 57.36200179040825,
                          |          "externalTransactionId": "995bd0da-efff-4702-ad39-b9ae26d29a10",
                          |          "fee": 0,
                          |          "feeDiscount": {
                          |            "discountAmount": 100,
                          |            "discountPercentage": 100,
                          |            "reason": "GCASH_INITIAL_PROMOTION"
                          |          },
                          |          "internalBankId": "cd5c45a2-2A4A-4228-a8c6-5c718163f5a6",
                          |          "lastUpdated": 1722954698,
                          |          "originalFee": 100,
                          |          "paymentDatePrefix": "2024-08",
                          |          "paymentMethod": "ach",
                          |          "phoneNumber": "+15056447011",
                          |          "receiverFirstName": "Ludwick",
                          |          "receiverLastName": "Zemenhoff",
                          |          "receiverPhoneNumber": "+639291610998",
                          |          "sardineValidateResult": "{\"status\":\"Plaid Error\",\"plaidError\":{\"error_code\":\"INVALID_ACCOUNT_ID\"}}",
                          |          "status": "SardineValidationError",
                          |          "total": 100,
                          |          "transactionType": "remittance",
                          |          "validUntil": 1723559498,
                          |          "wallet": "gcash",
                          |          "walletAmount": 5737,
                          |          "walletCurrency": "PHP"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "paymentId": "Rpzgbwqp5c9upd8nqjct3x0vh1"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "amount": 100,
                          |          "applicationBaseUrl": "http://localhost:3000",
                          |          "brand": "gcash",
                          |          "createdAt": 1722954701,
                          |          "currency": "USD",
                          |          "currencyBaseExchangeRate": 55.9949366431,
                          |          "currencyExchangeRateFeeBps": 75,
                          |          "currencyInterbankExchangeRate": 55.9949366431,
                          |          "currencyLiveExchangeRate": 57.7954677989,
                          |          "environment": "LocalTestAlpha",
                          |          "events": [
                          |            {
                          |              "actor": "remittances:InitializePaymentTransactionHandler",
                          |              "event": "start",
                          |              "status": "TransactionRequested",
                          |              "timestamp": 1722954701
                          |            }
                          |          ],
                          |          "exchangeRate": 57.36200179040825,
                          |          "externalTransactionId": "6ef54c4a-a497-497a-8be1-cbf26a206727",
                          |          "fee": 0,
                          |          "feeDiscount": {
                          |            "discountAmount": 100,
                          |            "discountPercentage": 100,
                          |            "reason": "GCASH_INITIAL_PROMOTION"
                          |          },
                          |          "internalBankId": "cd5c45a2-2A4A-4228-a8c6-5c718163f5a6",
                          |          "lastUpdated": 1722954701,
                          |          "originalFee": 100,
                          |          "paymentDatePrefix": "2024-08",
                          |          "paymentMethod": "ach",
                          |          "phoneNumber": "+15056447011",
                          |          "receiverFirstName": "Ludwick",
                          |          "receiverLastName": "Zemenhoff",
                          |          "receiverPhoneNumber": "+639291610998",
                          |          "sardineValidateResult": "{\"status\":\"Plaid Error\",\"plaidError\":{\"error_code\":\"ITEM_LOGIN_REQUIRED\"}}",
                          |          "status": "SardineValidationError",
                          |          "total": 100,
                          |          "transactionType": "remittance",
                          |          "validUntil": 1723559501,
                          |          "wallet": "gcash",
                          |          "walletAmount": 5737,
                          |          "walletCurrency": "PHP"
                          |        }
                          |      ],
                          |      [
                          |        {
                          |          "paymentId": "Ryiatteb8un41bttmy1bu2w4jj"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "amount": 100,
                          |          "applicationBaseUrl": "http://localhost:3000",
                          |          "brand": "gcash",
                          |          "createdAt": 1722954690,
                          |          "currency": "USD",
                          |          "currencyBaseExchangeRate": 55.9949366431,
                          |          "currencyExchangeRateFeeBps": 75,
                          |          "currencyInterbankExchangeRate": 55.9949366431,
                          |          "currencyLiveExchangeRate": 57.7954677989,
                          |          "environment": "LocalTestAlpha",
                          |          "events": [
                          |            {
                          |              "actor": "remittances:InitializePaymentTransactionHandler",
                          |              "event": "start",
                          |              "status": "TransactionRequested",
                          |              "timestamp": 1722954690
                          |            }
                          |          ],
                          |          "exchangeRate": 57.36200179040825,
                          |          "externalTransactionId": "5e188f18-8cf7-42e4-aca9-c5ffd4f491dd",
                          |          "fee": 0,
                          |          "feeDiscount": {
                          |            "discountAmount": 100,
                          |            "discountPercentage": 100,
                          |            "reason": "GCASH_INITIAL_PROMOTION"
                          |          },
                          |          "internalBankId": "cd5c45a2-2A4A-4228-a8c6-5c718163f5a6",
                          |          "lastUpdated": 1722954690,
                          |          "originalFee": 100,
                          |          "paymentDatePrefix": "2024-08",
                          |          "paymentMethod": "ach",
                          |          "phoneNumber": "+15056447011",
                          |          "receiverFirstName": "Ludwick",
                          |          "receiverLastName": "Zemenhoff",
                          |          "receiverPhoneNumber": "+639291610998",
                          |          "sardineValidateResult": "{\"sessionKey\":\"f4894c03-81b5-4ac6-9094-e7ea82343f00\",\"level\":\"low\",\"status\":\"Foo\",\"customer\":{\"score\":0,\"level\":\"low\",\"reasonCodes\":[\"TAA\"],\"signals\":[{\"key\":\"addressRiskLevel\",\"value\":\"low\",\"reasonCodes\":[\"AL4\",\"AL5\",\"AL9\"]},{\"key\":\"adverseMediaLevel\",\"value\":\"low\"},{\"key\":\"bankLevel\",\"value\":\"low\"},{\"key\":\"emailDomainLevel\",\"value\":\"low\"},{\"key\":\"emailLevel\",\"value\":\"low\"},{\"key\":\"nsfLevel\",\"value\":\"low\"},{\"key\":\"pepLevel\",\"value\":\"low\"},{\"key\":\"phoneCarrier\",\"value\":\"MCImetro Access Transmission Services LLC\"},{\"key\":\"phoneLevel\",\"value\":\"high\",\"reasonCodes\":[\"TAA\"]},{\"key\":\"phoneLineType\",\"value\":\"Landline\"},{\"key\":\"sanctionLevel\",\"value\":\"low\"}],\"phone\":{\"addressScore\":-1,\"cityMatch\":false,\"regionMatch\":false,\"postalCodeMatch\":false},\"address\":{\"validity\":\"unknown\"},\"bank\":{\"isNachaDebitCompatible\":\"true\",\"nsf\":{\"score\":0,\"normalizedScore\":0}}},\"transaction\":{\"level\":\"high\",\"amlLevel\":\"low\",\"indemnification\":{\"decision\":\"approved\",\"instantLimit\":2147483647,\"holdAmount\":0}},\"checkpoints\":{\"ach\":{\"riskLevel\":{\"value\":\"low\",\"ruleIds\":null}},\"aml\":{\"riskLevel\":{\"value\":\"low\",\"ruleIds\":null}},\"customer\":{\"customerPurchaseLevel\":{\"value\":\"low\",\"ruleIds\":[120,124]},\"historicalLevel\":{\"value\":\"low\",\"ruleIds\":[118,124]},\"phoneLevel\":{\"value\":\"high\",\"ruleIds\":[22,176]},\"riskLevel\":{\"value\":\"high\",\"ruleIds\":[22,176]}}},\"rules\":[{\"id\":124,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"always true\"},{\"id\":125,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Default: low phone risk level \"},{\"id\":176,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Name doesn't match\"},{\"id\":22,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Address doesn't match\"},{\"id\":118,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"Long time user (>45 days) & purchase history \"},{\"id\":120,\"isLive\":true,\"isAllowlisted\":false,\"name\":\"customer score low\"},{\"id\":143,\"isLive\":false,\"isAllowlisted\":false,\"name\":\"us phone\"},{\"id\":2438,\"isLive\":false,\"isAllowlisted\":false,\"name\":\"us phone 222\"},{\"id\":137,\"isLive\":false,\"isAllowlisted\":false,\"name\":\"IRS CTR Structuring from SSNs suspected to be stolen or synthetic fraud\"}],\"checkpointData\":[{\"name\":\"customer\",\"type\":\"weighted_max\"},{\"name\":\"ach\",\"type\":\"weighted_max\"},{\"name\":\"aml\",\"type\":\"weighted_max\"}]}",
                          |          "status": "SardineValidationError",
                          |          "total": 100,
                          |          "transactionType": "remittance",
                          |          "validUntil": 1723559490,
                          |          "wallet": "gcash",
                          |          "walletAmount": 5737,
                          |          "walletCurrency": "PHP"
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "paymentId"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/receiverBlacklist-alpha": {
                          |    "items": [],
                          |    "keySchema": [
                          |      "fullName",
                          |      "lastName"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/receivers-alpha": {
                          |    "items": [
                          |      [
                          |        {
                          |          "phoneNumber": "+639291610998"
                          |        },
                          |        {
                          |          "carrier": {
                          |            "mobileCountryCode": null,
                          |            "mobileNetworkCode": null,
                          |            "name": "Twilio Stubs",
                          |            "type": "MOBILE"
                          |          },
                          |          "country": "PH",
                          |          "createdAt": 1722954682,
                          |          "lastUpdated": 1722954682,
                          |          "verificationProviderRefId": "1db32c69-9f1c-4fc6-a2ec-c9da95d73222",
                          |          "wallet": "gcash"
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "phoneNumber"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/sessions-alpha": {
                          |    "items": [
                          |      [
                          |        {
                          |          "tokenHash": "699d0ba52d2a72f91372e97402699cdfe0180d324300b2b1c9d9dc7ee28e1f40"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "authorizationSource": "last4",
                          |          "baseUrl": "http://localhost:3000",
                          |          "currencyExchangeRates": {
                          |            "PHP": {
                          |              "baseExchangeRate": 55.9949366431,
                          |              "feeBps": 75,
                          |              "interbankExchangeRate": 55.9949366431,
                          |              "liveExchangeRate": 57.7954677989,
                          |              "transactionExchangeRate": 57.36200179040825
                          |            }
                          |          },
                          |          "currentPaymentId": "Ra23hfnslcqy07prt8f2szq87y",
                          |          "ipAddress": "192.0.2.0",
                          |          "isStrongAuth": true,
                          |          "phoneNumber": "+15056447011",
                          |          "sardineTransactionLimit": 6000,
                          |          "status": "active"
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "tokenHash"
                          |    ]
                          |  },
                          |  "arn:aws:dynamodb:us-east-1:694389950650:table/verifications-alpha": {
                          |    "items": [
                          |      [
                          |        {
                          |          "phoneNumber": "+15056447011"
                          |        },
                          |        {
                          |          "accountId": "8bc0fb64-e0e7-40E1-998a-eadb561809f2",
                          |          "lastSuccessfulVerification": 1722954304,
                          |          "status": "verified"
                          |        }
                          |      ]
                          |    ],
                          |    "keySchema": [
                          |      "phoneNumber"
                          |    ]
                          |  }
                          |}""".stripMargin

}
