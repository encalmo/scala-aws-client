# scala-aws-client

This Scala library wraps selected parts of the [AWS SDK for Java 2.x](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html) to offer simpler, scala-idiomatic API.

## Services

- IAM
- DynamoDB
- ApiGateway
- SQS
- Lambda
- S3
- SecretsManager
- STS
- KMS

## Usage

Use with SBT

    libraryDependencies += "org.encalmo" %% "scala-aws-client" % "0.9.0"

or with SCALA-CLI

    //> using dep org.encalmo::scala-aws-client:0.9.0

## Working with DynamoDB

### Using AwsDynamoDbApi

```scala
  import org.encalmo.aws.AwsClient
  import org.encalmo.aws.AwsDynamoDbApi.*
  import org.encalmo.aws.AwsDynamoDbApi.given
  import org.encalmo.models.{CUID2, Amount}

  val id = CUID2.randomCUID2()

  AwsClient.maybe {
    putItemInTable(
      tableName = "items",
      item = DynamoDbItem(
        "item_id" -> id,
        "status" -> "fine",
        "price" -> 12345,
        "is_for_sale" -> true
      )
    )
  }

  val item3 = AwsClient.optionally {
    getItemFromTable("items", ("item_id" -> id))
  }
```

### Using DynamoDbTable trait

```scala
    import org.encalmo.aws.DynamoDbTable
    import org.encalmo.aws.AwsDynamoDbApi.{*,given}
    import org.encalmo.models.{CUID2, Amount}

    given DynamoDbEnvironment = DefaultDynamoDbEnvironment
    given ErrorContext = DefaultErrorContext

    case class Item(
      item_id: CUID2,
      price: Amount,
      `is-for-sale`: Boolean,
      description: Option[String] = None
  )

    object Items extends DynamoDbTable[CUID2]("item_id") {
      override inline def baseTableName: String = "items"
    }

    val id = CUID2.randomCUID2()

    val item = Item(
      item_id = id,
      price = Amount(1234),
      is_for_sale = false
    )

    Items.setItem(item)
    val item2 = Items.getItemAsClass[Item](id)
    assert(item2.isDefined)

    Items.getItem(id)
    Items.setItemProperties(id, "is_for_sale" -> true, "price" -> Amount(1234))
    Items.setItemProperties(id, "description" -> "some old crap")
    Items.getItem(id)
    Items.removeItem(id)
```

### Using DynamoDbTableWithSortKey trait

```scala
    import org.encalmo.aws.DynamoDbTable
    import org.encalmo.aws.AwsDynamoDbApi.{*,given}
    import org.encalmo.models.{CUID2, Amount}

    object OrdersTable extends DynamoDbTableWithSortKey[String, Long]("order_id", "createdAt") {
      override inline def baseTableName: String = "orders"
    }

    given DynamoDbEnvironment = DefaultDynamoDbEnvironment
    given ErrorContext = DefaultErrorContext

    val id = CUID2.randomCUID2()
    val createdAt = Instant.now().getEpochSecond()

    assert(OrdersTable.getItemOrError(id, createdAt).isLeft)
    OrdersTable.setItemProperties(id, createdAt, "status" -> "submitted")
    assert(OrdersTable.getItemOrError(id, createdAt).isRight)
    OrdersTable.removeItemProperty(id, createdAt, "status")
    assert(OrdersTable.getItemOrError(id, createdAt).isRight)
    OrdersTable.removeItem(id, createdAt)
    assert(OrdersTable.getItemOrError(id, createdAt).isLeft)
```