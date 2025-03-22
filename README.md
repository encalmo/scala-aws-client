<a href="https://github.com/encalmo/scala-aws-client">![GitHub](https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white)</a> <a href="https://central.sonatype.com/artifact/org.encalmo/scala-aws-client_3" target="_blank">![Maven Central Version](https://img.shields.io/maven-central/v/org.encalmo/scala-aws-client_3?style=for-the-badge)</a> <a href="https://encalmo.github.io/scala-aws-client/scaladoc/org/encalmo/aws.html" target="_blank"><img alt="Scaladoc" src="https://img.shields.io/badge/docs-scaladoc-red?style=for-the-badge"></a>

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

    libraryDependencies += "org.encalmo" %% "scala-aws-client" % "0.9.8"

or with SCALA-CLI

    //> using dep org.encalmo::scala-aws-client:0.9.8

## Dependencies

   - [Scala](https://www.scala-lang.org) >= 3.6.3
   - [Scala **toolkit** 0.7.0](https://github.com/scala/toolkit)
   - org.slf4j [**slf4j-nop** 2.0.17](https://central.sonatype.com/artifact/org.slf4j/slf4j-nop)
   - software.amazon.awssdk [**bom** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/bom) | [**iam** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/iam) | [**sts** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/sts) | [**sso** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/sso) | [**ssooidc** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/ssooidc) | [**dynamodb** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/dynamodb) | [**sqs** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/sqs) | [**secretsmanager** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/secretsmanager) | [**kms** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/kms) | [**s3** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/s3) | [**lambda** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/lambda) | [**apigateway** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/apigateway) | [**apigatewayv2** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/apigatewayv2) | [**url-connection-client** 2.31.6](https://central.sonatype.com/artifact/software.amazon.awssdk/url-connection-client)

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
