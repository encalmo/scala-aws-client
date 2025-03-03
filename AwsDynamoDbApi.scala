package org.encalmo.aws

import org.encalmo.aws.Utils.*

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.waiters.WaiterResponse
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter

import ujson.*
import upickle.default.{ReadWriter, Reader, Writer, readwriter}

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.regex.Pattern
import scala.annotation.targetName
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.Buffer
import scala.compiletime.*
import scala.deriving.Mirror
import scala.io.AnsiColor
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

object AwsDynamoDbApi {

  type DynamoDbItem = collection.Map[String, AttributeValue]
  type DynamoDbItemKey = collection.Map[String, AttributeValue]
  type DynamoDbItemField = (String, AttributeValue)
  type DynamoDbItemUpdate = collection.Map[String, AttributeValueUpdate]

  case class ItemsWithNextPage(items: Seq[DynamoDbItem], maybeNextPage: Option[() => ItemsWithNextPage])

  object DynamoDbItemKey {
    inline def apply(fields: (String, AttributeValue)*): DynamoDbItemKey =
      fields.toMap
  }

  object DynamoDbItem {
    inline def apply(fields: (String, AttributeValue)*): DynamoDbItem =
      fields.toMap

    inline def from(
        map: scala.collection.Map[String, AttributeValue]
    ): DynamoDbItem =
      map.toMap

    inline def from(map: java.util.Map[String, AttributeValue]): DynamoDbItem =
      map.asScala
  }

  object DynamoDbItemUpdate {
    inline def apply(
        updates: (String, AttributeValueUpdate)*
    ): DynamoDbItemUpdate =
      updates.toMap

    inline def from(
        updates: collection.mutable.Map[String, AttributeValueUpdate]
    ): DynamoDbItemUpdate =
      updates.toMap
  }

  val REMOVE: AttributeValue = AttributeValue.fromNul(true)

  private val DELETE_PROPERTY: AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .build()

  inline def listTables()(using aws: AwsClient): Seq[String] =
    AwsClient.invoke(s"listTables") {
      aws.dynamoDb
        .listTables(ListTablesRequest.builder().build())
        .tableNames()
        .asScala
        .toSeq
    }

  inline def describeTable(
      tableName: String
  )(using aws: AwsClient): TableDescription =
    AwsClient.invoke(s"describeTable ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN}") {
      aws.dynamoDb
        .describeTable(
          DescribeTableRequest.builder().tableName(tableName).build()
        )
        .table()
    }

  inline def describeTimeToLive(
      tableName: String
  )(using aws: AwsClient): TimeToLiveDescription =
    AwsClient.invoke(s"describeTimeToLive ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN}") {
      aws.dynamoDb
        .describeTimeToLive(
          DescribeTimeToLiveRequest.builder().tableName(tableName).build()
        )
        .timeToLiveDescription()
    }

  inline def listTableTags(
      tableArn: String
  )(using aws: AwsClient): Seq[(String, String)] =
    AwsClient.invoke(s"listTagsOfResource $tableArn") {
      aws.dynamoDb
        .listTagsOfResource(
          ListTagsOfResourceRequest.builder().resourceArn(tableArn).build()
        )
        .tags()
        .asScala
        .map(t => (t.key(), t.value()))
        .toSeq
    }

  case class DynamoDbIndex(
      indexName: String,
      indexPartitionKey: (String, String),
      indexSortKey: Option[(String, String)] = None,
      indexNonKeyAttributes: Seq[String] = Seq.empty,
      readCapacityUnits: Int = 1,
      writeCapacityUnits: Int = 1
  )

  /** The CreateTable operation adds a new table to your account. In an Amazon Web Services account, table names must be
    * unique within each Region. That is, you can have two tables with same name if you create the tables in different
    * Regions.
    *
    * CreateTable is an asynchronous operation. Upon receiving a CreateTable request, DynamoDB immediately returns a
    * response with a TableStatus of CREATING. After the table is created, DynamoDB sets the TableStatus to ACTIVE. You
    * can perform read and write operations only on an ACTIVE table.
    *
    * You can optionally define secondary indexes on the new table, as part of the CreateTable operation. If you want to
    * create multiple tables with secondary indexes on them, you must create the tables sequentially. Only one table
    * with secondary indexes can be in the CREATING state at any given time.
    *
    * You can use the DescribeTable action to check the table status.
    */
  def createTable(
      tableName: String,
      partitionKey: String,
      sortKey: Option[(String, String)] = None,
      ignoreTableAlreadyExists: Boolean = true,
      readCapacityUnits: Int = 1,
      writeCapacityUnits: Int = 1,
      indexes: Seq[DynamoDbIndex] = Seq.empty
  )(using aws: AwsClient): DescribeTableResponse =
    AwsClient.invoke(s"createTable ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} with key ${partitionKey.show}") {
      try {
        val dbWaiter: DynamoDbWaiter = aws.dynamoDb.waiter()
        val request: CreateTableRequest = CreateTableRequest
          .builder()
          .attributeDefinitions(
            (Seq(
              AttributeDefinition
                .builder()
                .attributeName(partitionKey)
                .attributeType(ScalarAttributeType.S)
                .build()
            )
              ++ sortKey
                .map((keyName, keyType) =>
                  AttributeDefinition
                    .builder()
                    .attributeName(keyName)
                    .attributeType(keyType)
                    .build
                )
                .toSeq
              ++ indexes.map { index =>
                AttributeDefinition
                  .builder()
                  .attributeName(index.indexPartitionKey._1)
                  .attributeType(index.indexPartitionKey._2)
                  .build
              }).asJava
          )
          .keySchema(
            (Seq(
              KeySchemaElement
                .builder()
                .attributeName(partitionKey)
                .keyType(KeyType.HASH)
                .build()
            )
              ++ sortKey
                .map((sortKey, _) =>
                  KeySchemaElement
                    .builder()
                    .attributeName(sortKey)
                    .keyType(KeyType.RANGE)
                    .build()
                )
                .toSeq).asJava
          )
          .provisionedThroughput(
            ProvisionedThroughput
              .builder()
              .readCapacityUnits(readCapacityUnits.toLong)
              .writeCapacityUnits(writeCapacityUnits.toLong)
              .build()
          )
          .tableName(tableName)
          .globalSecondaryIndexes(indexes.map { index =>
            GlobalSecondaryIndex
              .builder()
              .indexName(index.indexName)
              .keySchema(
                (Seq(
                  KeySchemaElement
                    .builder()
                    .attributeName(index.indexPartitionKey._1)
                    .keyType(KeyType.HASH)
                    .build()
                )
                  ++ index.indexSortKey
                    .map((sortKey, _) =>
                      KeySchemaElement
                        .builder()
                        .attributeName(sortKey)
                        .keyType(KeyType.RANGE)
                        .build()
                    )
                    .toSeq).asJava
              )
              .projection(
                if (index.indexNonKeyAttributes.isEmpty)
                then {
                  Projection
                    .builder()
                    .projectionType(ProjectionType.ALL)
                    .build()
                } else {
                  Projection
                    .builder()
                    .projectionType(ProjectionType.INCLUDE)
                    .nonKeyAttributes(index.indexNonKeyAttributes.asJava)
                    .build()
                }
              )
              .provisionedThroughput(
                ProvisionedThroughput
                  .builder()
                  .readCapacityUnits(readCapacityUnits.toLong)
                  .writeCapacityUnits(writeCapacityUnits.toLong)
                  .build()
              )
              .build
          }.asJava)
          .build()

        val response: CreateTableResponse =
          aws.dynamoDb.createTable(request)

        val tableRequest: DescribeTableRequest = DescribeTableRequest
          .builder()
          .tableName(tableName)
          .build()

        // Wait until the Amazon DynamoDB table is created.
        val waiterResponse: WaiterResponse[DescribeTableResponse] =
          dbWaiter.waitUntilTableExists(tableRequest)

        waiterResponse.matched().response().toScala match {
          case Some(value) => value
          case None =>
            throw new Exception("Didn't receive response from describe table.")
        }

      } catch {
        case e: DynamoDbException =>
          if (
            ignoreTableAlreadyExists
            && e.awsErrorDetails().errorCode() == "ResourceInUseException"
          ) then
            val tableRequest: DescribeTableRequest = DescribeTableRequest
              .builder()
              .tableName(tableName)
              .build()
            aws.dynamoDb.describeTable(tableRequest)
          else throw e
      }
    }

  /** The DeleteTable operation deletes a table and all of its items. After a DeleteTable request, the specified table
    * is in the DELETING state until DynamoDB completes the deletion. If the table is in the ACTIVE state, you can
    * delete it. If a table is in CREATING or UPDATING states, then DynamoDB returns a ResourceInUseException. If the
    * specified table does not exist, DynamoDB returns a ResourceNotFoundException. If table is already in the DELETING
    * state, no error is returned.
    *
    * This operation only applies to Version 2019.11.21 (Current) of global tables.
    *
    * DynamoDB might continue to accept data read and write operations, such as GetItem and PutItem, on a table in the
    * DELETING state until the table deletion is complete.
    *
    * When you delete a table, any indexes on that table are also deleted.
    *
    * If you have DynamoDB Streams enabled on the table, then the corresponding stream on that table goes into the
    * DISABLED state, and the stream is automatically deleted after 24 hours.
    *
    * Use the DescribeTable action to check the status of the table.
    */
  def deleteTable(
      tableName: String
  )(using aws: AwsClient): Unit =
    AwsClient.invoke(s"deleteTable ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN}") {
      try {
        val dbWaiter: DynamoDbWaiter = aws.dynamoDb.waiter()
        val request: DeleteTableRequest = DeleteTableRequest
          .builder()
          .tableName(tableName)
          .build()

        val response: DeleteTableResponse =
          aws.dynamoDb.deleteTable(request)

        val tableRequest: DescribeTableRequest = DescribeTableRequest
          .builder()
          .tableName(tableName)
          .build()

        // Wait until the Amazon DynamoDB table is deleted.
        dbWaiter.waitUntilTableNotExists(tableRequest)
      } catch {
        case e: DynamoDbException => e
      }
    }

  /** Creates a new item, or replaces an old item with a new item. If an item that has the same primary key as the new
    * item already exists in the specified table, the new item completely replaces the existing item.
    *
    * When you add an item, the primary key attributes are the only required attributes.
    *
    * Empty String and Binary attribute values are allowed. Attribute values of type String and Binary must have a
    * length greater than zero if the attribute is used as a key attribute for a table or index. Set type attributes
    * cannot be empty.
    *
    * Invalid Requests with empty values will be rejected with a ValidationException exception.
    */
  def putItemInTable(
      tableName: String,
      item: DynamoDbItem
  )(using AwsClient): PutItemResponse =
    AwsClient.invoke(s"putItem into ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} setting ${item.show}") {
      val refinedItem = item.filterNot(_._2.nul())

      val request: PutItemRequest = PutItemRequest
        .builder()
        .tableName(tableName)
        .item(refinedItem.asJava)
        .build()

      summon[AwsClient].dynamoDb.putItem(request)
    }

  /** Edits an existing item's attributes, or adds a new item to the table if it does not already exist. You can put,
    * delete, or add attribute values. You can also perform a conditional update on an existing item (insert a new
    * attribute name-value pair if it doesn't exist, or replace an existing name-value pair if it has certain expected
    * attribute values).
    */
  def updateItemInTable(
      tableName: String,
      key: (String, AttributeValue),
      update: DynamoDbItemUpdate,
      returnUpdatedItem: Boolean = false,
      onlyIfRecordExists: Boolean = false
  )(using AwsClient): UpdateItemResponse =

    val refinedUpdate: collection.Map[String, AttributeValueUpdate] =
      update.filterNot(x =>
        x._2 == null ||
          (x._2.actionAsString() != "DELETE"
            && (x._2.value().nul() || x._2.value() == null))
      )

    val (request, description): (UpdateItemRequest, () => String) =
      if (
        refinedUpdate.keysIterator.exists(
          DocumentPath.isNestedPath
        ) || refinedUpdate.valuesIterator.exists(_.requiresUpdateExpression)
      )
      then {
        val (
          updateExpression,
          expressionAttributeNames,
          expressionAttributeValues
        ) =
          AttributeUpdate.createUpdateExpression(refinedUpdate)

        (
          UpdateItemRequest
            .builder()
            .tableName(tableName)
            .key(Map(key).asJava)
            .updateExpression(updateExpression)
            .optionally(
              if (expressionAttributeNames.isEmpty) None
              else Some(expressionAttributeNames),
              builder => value => builder.expressionAttributeNames(value.asJava)
            )
            .optionally(
              if (expressionAttributeValues.isEmpty) None
              else Some(expressionAttributeValues),
              builder => value => builder.expressionAttributeValues(value.asJava)
            )
            .conditionally(onlyIfRecordExists, _.conditionExpression(s"${key._1} = :${key._1}"))
            .returnValues(
              if returnUpdatedItem then ReturnValue.ALL_NEW
              else ReturnValue.NONE
            )
            .build(),
          () =>
            s"expression '${updateExpression}' where names are ${expressionAttributeNames
                .map((k, v) => s"$k=$v")
                .mkString(", ")} and values are ${expressionAttributeValues
                .map((k, v) => s"$k=${v.show}")
                .mkString(", ")}"
        )
      } else
        (
          UpdateItemRequest
            .builder()
            .tableName(tableName)
            .key(Map(key).asJava)
            .attributeUpdates(refinedUpdate.asJava)
            .conditionally(
              onlyIfRecordExists,
              _.expected(
                Map(
                  key._1 -> ExpectedAttributeValue
                    .builder()
                    .comparisonOperator(ComparisonOperator.EQ)
                    .value(key._2)
                    .build()
                ).asJava
              )
            )
            .returnValues(
              if returnUpdatedItem then ReturnValue.ALL_NEW
              else ReturnValue.NONE
            )
            .build(),
          () => refinedUpdate.show
        )

    AwsClient.invoke(
      s"updateItem in ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} by key ${key.show} with ${description()}"
    ) {
      try {
        summon[AwsClient].dynamoDb.updateItem(request)
      } catch {
        case NonFatal(e) =>
          println(s"Failed updateItem request: $request")
          throw e
      }
    }

  def updateItemInTableUsingCompositeKey(
      tableName: String,
      keys: Seq[(String, AttributeValue)],
      update: DynamoDbItemUpdate,
      returnUpdatedItem: Boolean = false,
      onlyIfRecordExists: Boolean = false
  )(using AwsClient): UpdateItemResponse =

    val refinedUpdate: collection.Map[String, AttributeValueUpdate] =
      update.filterNot(x =>
        x._2 == null ||
          (x._2.actionAsString() != "DELETE"
            && (x._2.value().nul() || x._2.value() == null))
      )

    val (request, description): (UpdateItemRequest, () => String) =
      if (
        refinedUpdate.keysIterator.exists(
          DocumentPath.isNestedPath
        ) || refinedUpdate.valuesIterator.exists(_.requiresUpdateExpression)
      )
      then {
        val (
          updateExpression,
          expressionAttributeNames,
          expressionAttributeValues
        ) =
          AttributeUpdate.createUpdateExpression(refinedUpdate)

        (
          UpdateItemRequest
            .builder()
            .tableName(tableName)
            .key(keys.toMap.asJava)
            .updateExpression(updateExpression)
            .optionally(
              if (expressionAttributeNames.isEmpty) None
              else Some(expressionAttributeNames),
              builder => value => builder.expressionAttributeNames(value.asJava)
            )
            .optionally(
              if (expressionAttributeValues.isEmpty) None
              else Some(expressionAttributeValues),
              builder => value => builder.expressionAttributeValues(value.asJava)
            )
            .conditionally(onlyIfRecordExists, _.conditionExpression(keys.map((k, v) => s"$k = :$k").mkString(" AND ")))
            .returnValues(
              if returnUpdatedItem then ReturnValue.ALL_NEW
              else ReturnValue.NONE
            )
            .build(),
          () =>
            s"expression '${updateExpression}' where names are ${expressionAttributeNames
                .map((k, v) => s"$k=$v")
                .mkString(", ")}${
                if (expressionAttributeValues.isEmpty) then
                  s" and values are ${expressionAttributeValues
                      .map((k, v) => s"$k=${v.show}")
                      .mkString(", ")}"
                else ""
              }"
        )
      } else
        (
          UpdateItemRequest
            .builder()
            .tableName(tableName)
            .key(keys.toMap.asJava)
            .attributeUpdates(refinedUpdate.asJava)
            .conditionally(
              onlyIfRecordExists,
              _.expected(
                keys
                  .map((k, v) =>
                    k -> ExpectedAttributeValue
                      .builder()
                      .comparisonOperator(ComparisonOperator.EQ)
                      .value(v)
                      .build()
                  )
                  .toMap
                  .asJava
              )
            )
            .returnValues(
              if returnUpdatedItem then ReturnValue.ALL_NEW
              else ReturnValue.NONE
            )
            .build(),
          () => update.show
        )

    AwsClient.invoke(
      s"updateItem in ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} by composite key ${keys.mkString(", ")} with ${description()}"
    ) {
      try {
        summon[AwsClient].dynamoDb.updateItem(request)
      } catch {
        case NonFatal(e) =>
          println(s"Failed updateItem request: $request")
          throw e
      }
    }

  /** Deletes a single item in a table by primary key. You can perform a conditional delete operation that deletes the
    * item if it exists, or if it has an expected attribute value.
    */
  def deleteItemFromTable(
      tableName: String,
      key: (String, AttributeValue)
  )(using AwsClient): DeleteItemResponse =
    AwsClient.invoke(s"deleteItem from ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} by key ${key.show}") {

      val request: DeleteItemRequest = DeleteItemRequest
        .builder()
        .tableName(tableName)
        .key(Map(key).asJava)
        .build()

      summon[AwsClient].dynamoDb.deleteItem(request)
    }

  def deleteItemFromTableUsingCompositeKey(
      tableName: String,
      keys: Seq[(String, AttributeValue)]
  )(using AwsClient): DeleteItemResponse =
    AwsClient.invoke(
      s"deleteItem from ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} by composite key ${keys.map(_.show).mkString(", ")}"
    ) {

      val request: DeleteItemRequest = DeleteItemRequest
        .builder()
        .tableName(tableName)
        .key(keys.toMap.asJava)
        .build()

      summon[AwsClient].dynamoDb.deleteItem(request)
    }

  /** The GetItem operation returns a set of attributes for the item with the given primary key. If there is no matching
    * item, GetItem does not return any data and there will be no Item element in the response.
    *
    * GetItem provides an eventually consistent read by default. If your application requires a strongly consistent
    * read, set ConsistentRead to true. Although a strongly consistent read might take more time than an eventually
    * consistent read, it always returns the last updated value.
    */
  def getItemFromTable(tableName: String, key: (String, AttributeValue))(using
      AwsClient
  ): Option[DynamoDbItem] =
    AwsClient.invoke(s"getItem from ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} by key ${key.show}") {
      val request: GetItemRequest = GetItemRequest
        .builder()
        .key(Map(key).asJava)
        .tableName(tableName)
        .build()

      // If there is no matching item, GetItem does not return any data.
      val returnedItem: Map[String, AttributeValue] =
        summon[AwsClient].dynamoDb.getItem(request).item().asScala.toMap

      if (returnedItem.isEmpty)
        None
      else {
        Some(returnedItem)
      }
    }

  /** The GetItem operation returns a set of attributes for the item with the given primary key. If there is no matching
    * item, GetItem does not return any data and there will be no Item element in the response.
    *
    * GetItem provides an eventually consistent read by default. If your application requires a strongly consistent
    * read, set ConsistentRead to true. Although a strongly consistent read might take more time than an eventually
    * consistent read, it always returns the last updated value.
    */
  def getItemFromTable(
      tableName: String,
      key: (String, AttributeValue),
      projection: Seq[String]
  )(using
      AwsClient
  ): Option[DynamoDbItem] = {
    val projectionOpt =
      if (projection.isEmpty)
      then None
      else Some(projection.filterNot(_ == key._1))

    AwsClient.invoke(
      s"getItem from ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} by key ${key.show} rendering ${projection.mkString(", ")}"
    ) {
      val request: GetItemRequest =
        if (projection.exists(p => dynamoDbReservedWords.contains(p.toUpperCase())))
        then
          GetItemRequest
            .builder()
            .key(Map(key).asJava)
            .tableName(tableName)
            .optionally(
              projectionOpt,
              builder =>
                (filteredProjection: Seq[String]) =>
                  builder
                    .projectionExpression(
                      (key._1 +: filteredProjection.map(n => s"#$n")).mkString(",")
                    )
                    .expressionAttributeNames(
                      filteredProjection.map(n => (s"#$n", n)).toMap.asJava
                    )
            )
            .build()
        else
          GetItemRequest
            .builder()
            .key(Map(key).asJava)
            .tableName(tableName)
            .optionally(
              projectionOpt,
              builder => _ => builder.projectionExpression(projection.mkString(","))
            )
            .build()

      // If there is no matching item, GetItem does not return any data.
      val returnedItem: Map[String, AttributeValue] =
        summon[AwsClient].dynamoDb.getItem(request).item().asScala.toMap

      if (returnedItem.isEmpty)
        None
      else {
        Some(returnedItem)
      }
    }
  }

  def getItemFromTableUsingCompositeKey(
      tableName: String,
      keys: Seq[(String, AttributeValue)],
      projection: Seq[String] = Seq.empty
  )(using
      AwsClient
  ): Option[DynamoDbItem] =
    val ks = keys.map(_._1)
    val projectionOpt =
      if (projection.isEmpty)
      then None
      else Some(projection.filterNot(k => ks.contains(k)))
    AwsClient.invoke(
      s"getItem from ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN} by composite key ${keys.map(_.show).mkString(", ")}${projectionOpt
          .map(_ => s" rendering ${projection.mkString(", ")}")
          .getOrElse("")}"
    ) {
      val request: GetItemRequest =
        if (projection.exists(p => dynamoDbReservedWords.contains(p.toUpperCase())))
        then
          GetItemRequest
            .builder()
            .key(keys.toMap.asJava)
            .tableName(tableName)
            .optionally(
              projectionOpt,
              builder =>
                (filteredProjection: Seq[String]) =>
                  builder
                    .projectionExpression(
                      (ks ++ filteredProjection.map(n => s"#$n")).mkString(",")
                    )
                    .expressionAttributeNames(
                      filteredProjection.map(n => (s"#$n", n)).toMap.asJava
                    )
            )
            .build()
        else
          GetItemRequest
            .builder()
            .key(keys.toMap.asJava)
            .tableName(tableName)
            .optionally(
              projectionOpt,
              builder => _ => builder.projectionExpression(projection.mkString(","))
            )
            .build()

      // If there is no matching item, GetItem does not return any data.
      val returnedItem: Map[String, AttributeValue] =
        summon[AwsClient].dynamoDb.getItem(request).item().asScala.toMap

      if (returnedItem.isEmpty)
        None
      else {
        Some(returnedItem)
      }
    }

  /** You must provide the name of the partition key attribute and a single value for that attribute. Query returns all
    * items with that partition key value. Optionally, you can provide a sort key attribute and use a comparison
    * operator to refine the search results.
    *
    * Use the KeyConditionExpression parameter to provide a specific value for the partition key. The Query operation
    * will return all of the items from the table or index with that partition key value. You can optionally narrow the
    * scope of the Query operation by specifying a sort key value and a comparison operator in KeyConditionExpression.
    * To further refine the Query results, you can optionally provide a FilterExpression. A FilterExpression determines
    * which items within the results should be returned to you. All of the other results are discarded.
    *
    * A Query operation always returns a result set. If no matching items are found, the result set will be empty.
    * Queries that do not return results consume the minimum number of read capacity units for that type of read
    * operation.
    */
  def queryTable(
      tableName: String,
      partitionKey: (String, AttributeValue),
      sortKey: Option[KeyCondition] = None,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None,
      exclusiveStartKey: Option[DynamoDbItemKey] = None
  )(using
      AwsClient
  ): Seq[DynamoDbItem] =
    queryTableFullResponse(tableName, partitionKey, sortKey, indexName, projection, filters, limit, exclusiveStartKey)
      .items()
      .asScala
      .map(_.asScala)
      .toSeq

  def queryTableFullResponse(
      tableName: String,
      partitionKey: (String, AttributeValue),
      sortKey: Option[KeyCondition] = None,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None,
      exclusiveStartKey: Option[DynamoDbItemKey] = None
  )(using
      AwsClient
  ): QueryResponse =
    AwsClient.invoke(
      s"query ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN}${indexName
          .map(i => s" using index $i")
          .getOrElse("")} by partitionKey ${partitionKey.show}${sortKey
          .map(sk => s" and sortKey ${sk.show()}")
          .getOrElse("")}${
          if (filters.isEmpty) then "" else filters.map(_.show()).mkString(" with filters ", " and ", "")
        }"
    ) {
      val (partitionKeyAttributeNames, partitionKeyAttributeValues) =
        partitionKey match {
          case (key, value) =>
            (
              Map("#partitionKeyName" -> key),
              Map(":partitionKeyValue" -> value)
            )
        }
      val (sortKeyAttributeNames, sortKeyAttributeValues) =
        sortKey match {
          case Some(sortKeyCondition) =>
            (
              Map(sortKeyCondition.attributeNameMapping("sortKey")),
              sortKeyCondition.attributeValuesMap("sortKey")
            )
          case None => (Map.empty, Map.empty)
        }

      val keyConditionExpression: String =
        "#partitionKeyName = :partitionKeyValue"
          ++ sortKey
            .map(_.writeAsExpression("sortKey"))
            .map(" and " + _)
            .getOrElse("")

      val (filterAttributeNames, filterAttributeValues) =
        (
          filters.zipWithIndex.map((fc, index) => fc.attributeNameMapping("filter", index)),
          filters.zipWithIndex.flatMap((fc, index) => fc.attributeValuesMap("filter", index))
        )

      val filterExpression: Option[String] =
        if (filters.isEmpty) then None
        else
          Some(
            filters.zipWithIndex
              .map((fc, index) => fc.asExpression("filter", index))
              .mkString(" and ")
          )

      val (projectionExpanded, projectionAttributeNames) =
        if (projection.exists(_.exists(p => dynamoDbReservedWords.contains(p.toUpperCase()))))
        then
          (
            projection.map(_.map(n => s"#$n")).getOrElse(Seq.empty),
            projection.map(_.map(n => (s"#$n", n))).getOrElse(Seq.empty)
          )
        else (projection.getOrElse(Seq.empty), Seq.empty)

      val request: QueryRequest = QueryRequest
        .builder()
        .tableName(tableName)
        .optionally(indexName, _.indexName)
        .keyConditionExpression(keyConditionExpression)
        .optionally(filterExpression, _.filterExpression)
        .optionally(exclusiveStartKey, builder => value => builder.exclusiveStartKey(value.asJava))
        .expressionAttributeNames(
          (partitionKeyAttributeNames ++ sortKeyAttributeNames ++ filterAttributeNames ++ projectionAttributeNames).asJava
        )
        .expressionAttributeValues(
          (partitionKeyAttributeValues ++ sortKeyAttributeValues ++ filterAttributeValues).asJava
        )
        .optionally(
          projection,
          builder => _ => builder.projectionExpression(projectionExpanded.mkString(","))
        )
        .optionally(limit, builder => int => builder.limit(int))
        .build()

      summon[AwsClient].dynamoDb.query(request)
    }

  def queryTableWithScrolling(
      tableName: String,
      partitionKey: (String, AttributeValue),
      sortKey: Option[KeyCondition] = None,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None,
      exclusiveStartKey: Option[DynamoDbItemKey] = None
  )(using
      AwsClient
  ): ItemsWithNextPage = {
    val response =
      queryTableFullResponse(tableName, partitionKey, sortKey, indexName, projection, filters, limit, exclusiveStartKey)
    if (response.hasLastEvaluatedKey())
    then
      ItemsWithNextPage(
        items = response
          .items()
          .asScala
          .map(_.asScala)
          .toSeq,
        maybeNextPage = Some(() =>
          queryTableWithScrolling(
            tableName,
            partitionKey,
            sortKey,
            indexName,
            projection,
            filters,
            limit,
            Some(response.lastEvaluatedKey().asScala)
          )
        )
      )
    else
      ItemsWithNextPage(
        items = response
          .items()
          .asScala
          .map(_.asScala)
          .toSeq,
        maybeNextPage = None
      )
  }

  def queryTableAllPages(
      tableName: String,
      partitionKey: (String, AttributeValue),
      sortKey: Option[KeyCondition] = None,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None
  )(using
      AwsClient
  ): Seq[DynamoDbItem] = {
    val items = Buffer[DynamoDbItem]()

    var maybeNextPage: Option[() => ItemsWithNextPage] =
      Some(() => queryTableWithScrolling(tableName, partitionKey, sortKey, indexName, projection, filters, limit))

    while (maybeNextPage.isDefined) {
      val response = maybeNextPage.get()
      items.appendAll(response.items)
      maybeNextPage = response.maybeNextPage
    }

    items.toSeq
  }

  /** The Scan operation returns one or more items and item attributes by accessing every item in a table or a secondary
    * index. To have DynamoDB return fewer items, you can provide a FilterExpression operation.
    *
    * If the total size of scanned items exceeds the maximum dataset size limit of 1 MB, the scan completes and results
    * are returned to the user. The LastEvaluatedKey value is also returned and the requestor can use the
    * LastEvaluatedKey to continue the scan in a subsequent operation. Each scan response also includes number of items
    * that were scanned (ScannedCount) as part of the request. If using a FilterExpression , a scan result can result in
    * no items meeting the criteria and the Count will result in zero. If you did not use a FilterExpression in the scan
    * request, then Count is the same as ScannedCount.
    *
    * Count and ScannedCount only return the count of items specific to a single scan request and, unless the table is
    * less than 1MB, do not represent the total number of items in the table.
    *
    * A single Scan operation first reads up to the maximum number of items set (if using the Limit parameter) or a
    * maximum of 1 MB of data and then applies any filtering to the results if a FilterExpression is provided. If
    * LastEvaluatedKey is present in the response, pagination is required to complete the full table scan. For more
    * information, see Paginating the Results in the Amazon DynamoDB Developer Guide.
    *
    * Scan operations proceed sequentially; however, for faster performance on a large table or secondary index,
    * applications can request a parallel Scan operation by providing the Segment and TotalSegments parameters. For more
    * information, see Parallel Scan in the Amazon DynamoDB Developer Guide.
    *
    * By default, a Scan uses eventually consistent reads when accessing the items in a table. Therefore, the results
    * from an eventually consistent Scan may not include the latest item changes at the time the scan iterates through
    * each item in the table. If you require a strongly consistent read of each item as the scan iterates through the
    * items in the table, you can set the ConsistentRead parameter to true. Strong consistency only relates to the
    * consistency of the read at the item level.
    *
    * DynamoDB does not provide snapshot isolation for a scan operation when the ConsistentRead parameter is set to
    * true. Thus, a DynamoDB scan operation does not guarantee that all reads in a scan see a consistent snapshot of the
    * table when the scan operation was requested.
    */
  def scanTable(
      tableName: String,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None,
      exclusiveStartKey: Option[DynamoDbItemKey] = None
  )(using
      AwsClient
  ): Seq[DynamoDbItem] =
    scanTableFullResponse(tableName, indexName, projection, filters, limit, exclusiveStartKey)
      .items()
      .asScala
      .map(_.asScala)
      .toSeq

  def scanTableFullResponse(
      tableName: String,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None,
      exclusiveStartKey: Option[DynamoDbItemKey] = None
  )(using
      AwsClient
  ): ScanResponse =
    AwsClient.invoke(
      s"scan ${AnsiColor.YELLOW}$tableName${AnsiColor.CYAN}${indexName
          .map(i => s" using index $i")
          .getOrElse("")}${
          if (filters.isEmpty) then "" else filters.map(_.show()).mkString(" with filters ", " and ", "")
        }"
    ) {
      val (filterAttributeNames, filterAttributeValues) =
        (
          filters.zipWithIndex.map((fc, index) => fc.attributeNameMapping("filter", index)),
          filters.zipWithIndex.flatMap((fc, index) => fc.attributeValuesMap("filter", index))
        )

      val filterExpression: Option[String] =
        if (filters.isEmpty) then None
        else
          Some(
            filters.zipWithIndex
              .map((fc, index) => fc.asExpression("filter", index))
              .mkString(" and ")
          )

      val (projectionExpanded, projectionAttributeNames) =
        if (projection.exists(_.exists(p => dynamoDbReservedWords.contains(p.toUpperCase()))))
        then
          (
            projection.map(_.map(n => s"#$n")).getOrElse(Seq.empty),
            projection.map(_.map(n => (s"#$n", n))).getOrElse(Seq.empty)
          )
        else (projection.getOrElse(Seq.empty), Seq.empty)

      val expressionAttributeNames =
        (filterAttributeNames ++ projectionAttributeNames).toMap

      val expressionAttributeValues =
        (filterAttributeValues).toMap

      val request: ScanRequest = ScanRequest
        .builder()
        .tableName(tableName)
        .optionally(indexName, _.indexName)
        .optionally(filterExpression, _.filterExpression)
        .optionally(exclusiveStartKey, builder => value => builder.exclusiveStartKey(value.asJava))
        .conditionally(expressionAttributeNames.nonEmpty, _.expressionAttributeNames(expressionAttributeNames.asJava))
        .conditionally(
          expressionAttributeValues.nonEmpty,
          _.expressionAttributeValues(expressionAttributeValues.asJava)
        )
        .optionally(
          projection,
          builder => _ => builder.projectionExpression(projectionExpanded.mkString(","))
        )
        .optionally(limit, builder => int => builder.limit(int))
        .build()

      summon[AwsClient].dynamoDb.scan(request)
    }

  def scanTableWithScrolling(
      tableName: String,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None,
      exclusiveStartKey: Option[DynamoDbItemKey] = None
  )(using
      AwsClient
  ): ItemsWithNextPage = {
    val response =
      scanTableFullResponse(tableName, indexName, projection, filters, limit, exclusiveStartKey)
    if (response.hasLastEvaluatedKey())
    then
      ItemsWithNextPage(
        items = response
          .items()
          .asScala
          .map(_.asScala)
          .toSeq,
        maybeNextPage = Some(() =>
          scanTableWithScrolling(
            tableName,
            indexName,
            projection,
            filters,
            limit,
            Some(response.lastEvaluatedKey().asScala)
          )
        )
      )
    else
      ItemsWithNextPage(
        items = response
          .items()
          .asScala
          .map(_.asScala)
          .toSeq,
        maybeNextPage = None
      )
  }

  def scanTableAllPages(
      tableName: String,
      indexName: Option[String] = None,
      projection: Option[Seq[String]] = None,
      filters: Seq[FilterCondition] = Seq.empty,
      limit: Option[Int] = None
  )(using
      AwsClient
  ): Seq[DynamoDbItem] = {
    val items = Buffer[DynamoDbItem]()

    var maybeNextPage: Option[() => ItemsWithNextPage] =
      Some(() => scanTableWithScrolling(tableName, indexName, projection, filters, limit))

    while (maybeNextPage.isDefined) {
      val response = maybeNextPage.get()
      items.appendAll(response.items)
      maybeNextPage = response.maybeNextPage
    }

    items.toSeq
  }

  extension (updateResponse: UpdateItemResponse)
    inline def updatedItem: Option[DynamoDbItem] =
      if (updateResponse.hasAttributes())
      then Some(updateResponse.attributes().asScala)
      else None

  type Condition = (String, CompareFunction, Seq[AttributeValue])

  /** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.KeyConditionExpressions.html
    */
  type KeyCondition = Condition

  /** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.FilterExpression.html
    */
  type FilterCondition = Condition

  extension (condition: Condition) {
    inline def filter(items: Iterable[DynamoDbItem]): Iterable[DynamoDbItem] =
      items.filter(item => condition.matches(item))

    inline def matches(item: DynamoDbItem): Boolean =
      condition._2.compare(item.get(condition._1), condition._3)

    inline def matches(extract: String => Option[AttributeValue]): Boolean =
      condition._2.compare(extract(condition._1), condition._3)

    inline def show(): String =
      condition._2.writeAsExpression(
        condition._1,
        condition._3.map(_.show)
      )

    inline def writeAsExpression(prefix: String): String =
      condition._2.writeAsExpression(
        s"#${prefix}Name",
        if (condition._3.size > 1)
          condition._3.zipWithIndex.map((value, index) => s":${prefix}Value${index}")
        else condition._3.headOption.map(_ => s":${prefix}Value").toSeq
      )

    inline def asExpression(prefix: String, index: Int): String =
      condition._2.writeAsExpression(
        s"#${prefix}Name${index}",
        if (condition._3.size > 1)
          condition._3.zipWithIndex.map((value, index2) => s":${prefix}Value${index}_${index2}")
        else condition._3.headOption.map(_ => s":${prefix}Value${index}").toSeq
      )

    inline def attributeNameMapping(
        prefix: String
    ): (String, String) =
      s"#${prefix}Name" -> condition._1

    inline def attributeNameMapping(
        prefix: String,
        index: Int
    ): (String, String) =
      s"#${prefix}Name$index" -> condition._1

    inline def attributeValuesMap(
        prefix: String
    ): Map[String, AttributeValue] =
      if (condition._3.size > 1)
      then
        condition._3.zipWithIndex
          .map((value, index) => s":${prefix}Value${index}" -> value)
          .toMap
      else
        condition._3.headOption
          .map(value => Map(s":${prefix}Value" -> value))
          .getOrElse(Map.empty)

    inline def attributeValuesMap(
        prefix: String,
        index: Int
    ): Map[String, AttributeValue] =
      if (condition._3.size > 1)
      then
        condition._3.zipWithIndex
          .map((value, index2) => s":${prefix}Value${index}_${index2}" -> value)
          .toMap
      else
        condition._3.headOption
          .map(value => Map(s":${prefix}Value${index}" -> value))
          .getOrElse(Map.empty)
  }

  object KeyCondition extends KeyConditionBuilders
  object FilterCondition extends KeyConditionBuilders, FilterConditionBuilders
  trait KeyConditionBuilders {
    inline def equals[T](name: String, value: T)(using
        Conversion[T, AttributeValue]
    ): Condition =
      (name, CompareFunction.EQUAL, Seq(value))

    inline def lowerThan(name: String, value: Double): Condition =
      (name, CompareFunction.LOWER_THAN, Seq(value))

    inline def lowerThanOrEquals(name: String, value: Double): Condition =
      (name, CompareFunction.LOWER_THAN_OR_EQUAL, Seq(value))

    inline def greaterThan(name: String, value: Double): Condition =
      (name, CompareFunction.GREATER_THAN, Seq(value))

    inline def greaterThanOrEqual(name: String, value: Double): Condition =
      (name, CompareFunction.GREATER_THAN_OR_EQUAL, Seq(value))

    inline def between(
        name: String,
        lower: Double,
        upper: Double
    ): Condition =
      (name, CompareFunction.BETWEEN, Seq(lower, upper))

    inline def beginsWith(name: String, prefix: String): Condition =
      (name, CompareFunction.BEGINS_WITH, Seq(prefix))
  }

  trait FilterConditionBuilders {
    inline def notEqual[T](name: String, value: T)(using
        Conversion[T, AttributeValue]
    ): Condition =
      (name, CompareFunction.NOT_EQUAL, Seq(value))

    inline def in[T](name: String, values: T*)(using
        convert: Conversion[T, AttributeValue]
    ): Condition =
      (name, CompareFunction.IN, values.map(convert))

    inline def contains[T](name: String, value: T)(using
        Conversion[T, AttributeValue]
    ): Condition =
      (name, CompareFunction.CONTAINS, Seq(value))

    inline def attributeExists[T](name: String): Condition =
      (name, CompareFunction.ATTRIBUTE_EXISTS, Seq.empty)

    inline def attributeNotExists[T](name: String): Condition =
      (name, CompareFunction.ATTRIBUTE_NOT_EXISTS, Seq.empty)

    inline def attributeType[T](
        name: String,
        typeDescriptor: String
    ): Condition =
      (name, CompareFunction.ATTRIBUTE_TYPE, Seq(typeDescriptor))

  }

  object Condition {
    private val SPACE = "(?:\\s?)"
    private val PATH = "(\\#?[a-zA-Z0-9_]+)"
    private val OPERAND = "(\\:[a-zA-Z0-9]+)"
    private val TYPE = "(S|SS|N|NS|B|BS|BOOL|NULL|M|L)"

    val beginsWithSyntax: Regex =
      s"begins_with\\($SPACE$PATH$SPACE,$SPACE$OPERAND$SPACE\\)".r

    val containsSyntax: Regex =
      s"contains\\($SPACE$PATH$SPACE,$SPACE$OPERAND$SPACE\\)".r

    val sizeSyntax: Regex =
      s"size\\($SPACE$PATH$SPACE\\)".r

    val attributeExistsSyntax: Regex =
      s"attribute_exists\\($SPACE$PATH$SPACE\\)".r

    val attributeNotExistsSyntax: Regex =
      s"attribute_not_exists\\($SPACE$PATH$SPACE\\)".r

    val attributeTypeSyntax: Regex =
      s"attribute_type\\($SPACE$PATH$SPACE,$SPACE$TYPE$SPACE\\)".r

    inline def parse(
        expression: String,
        resolveName: String => String,
        resolveValue: String => AttributeValue
    ): Array[(String, CompareFunction, Seq[AttributeValue])] =
      expression
        .trim()
        .split("(\\s+)and(\\s+)")
        .map(_.trim())
        .map(condition => parseCondition(condition, resolveName, resolveValue))

    private def parseCondition(
        expression: String,
        resolveName: String => String,
        resolveValue: String => AttributeValue
    ): Condition =
      maybeParseFunction(expression, resolveName, resolveValue)
        .getOrElse {
          val tokens = expression.split("\\s+").map(_.trim())
          if tokens.size < 3 then
            throw new Exception(
              s"Condition expression $expression is invalid. See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax"
            )
          else parseCondition(tokens, resolveName, resolveValue)
        }

    private def parseCondition(
        tokens: Array[String],
        resolveName: String => String,
        resolveValue: String => AttributeValue
    ): Condition = {

      val nameRef = tokens(0)
      val compareFunction = CompareFunction.from(tokens(1))
      val valueRef = tokens(2)
      (
        if (nameRef.startsWith("#")) then resolveName(nameRef) else nameRef,
        compareFunction,
        if (
          compareFunction == CompareFunction.BETWEEN
          && tokens.size == 5
          && tokens(3) == "AND"
          && valueRef.startsWith(":")
          && tokens(4).startsWith(":")
        ) then {
          Seq(resolveValue(valueRef), resolveValue(tokens(4)))
        } else if (compareFunction == CompareFunction.IN) then {
          val expression = tokens.drop(2).mkString(" ").trim()
          if (expression.startsWith("(") && expression.endsWith(")"))
          then
            ArraySeq
              .unsafeWrapArray(
                expression
                  .drop(1)
                  .dropRight(1)
                  .split(",")
              )
              .map(_.trim())
              .map(resolveValue)
          else
            throw new Exception(
              s"Condition expression ${tokens.mkString(" ")} is invalid. See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax"
            )
        } else if (tokens.size == 3 && valueRef.startsWith(":")) then Seq(resolveValue(valueRef))
        else
          throw new Exception(
            s"Condition expression ${tokens.mkString(" ")} is invalid. See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax"
          )
      )
    }

    private def maybeParseFunction(
        expression: String,
        resolveName: String => String,
        resolveValue: String => AttributeValue
    ): Option[Condition] =
      expression match {
        case beginsWithSyntax(nameRef, valueRef) =>
          Some(
            (
              if (nameRef.startsWith("#")) then resolveName(nameRef)
              else nameRef,
              CompareFunction.BEGINS_WITH,
              Seq(resolveValue(valueRef))
            )
          )
        case containsSyntax(nameRef, valueRef) =>
          Some(
            (
              if (nameRef.startsWith("#")) then resolveName(nameRef)
              else nameRef,
              CompareFunction.CONTAINS,
              Seq(resolveValue(valueRef))
            )
          )
        case attributeExistsSyntax(nameRef) =>
          Some(
            (
              if (nameRef.startsWith("#")) then resolveName(nameRef)
              else nameRef,
              CompareFunction.ATTRIBUTE_EXISTS,
              Seq.empty
            )
          )
        case attributeNotExistsSyntax(nameRef) =>
          Some(
            (
              if (nameRef.startsWith("#")) then resolveName(nameRef)
              else nameRef,
              CompareFunction.ATTRIBUTE_NOT_EXISTS,
              Seq.empty
            )
          )
        case attributeTypeSyntax(nameRef, typeDescriptor) =>
          Some(
            (
              if (nameRef.startsWith("#")) then resolveName(nameRef)
              else nameRef,
              CompareFunction.ATTRIBUTE_TYPE,
              Seq(typeDescriptor)
            )
          )
        case _ =>
          None
      }
  }

  /** DynamoDb condition function
    *   - a = b — true if the attribute a is equal to the value b
    *   - a < b — true if a is less than b
    *   - a <= b — true if a is less than or equal to b
    *   - a > b — true if a is greater than b
    *   - a >= b — true if a is greater than or equal to b
    *   - a BETWEEN b AND c — true if a is greater than or equal to b, and less than or equal to c.
    *
    * The following function is also supported: begins_with (a, substr) — true if the value of attribute a begins with a
    * particular substring.
    */
  enum CompareFunction {
    case EQUAL
    case NOT_EQUAL
    case LOWER_THAN
    case LOWER_THAN_OR_EQUAL
    case GREATER_THAN
    case GREATER_THAN_OR_EQUAL
    case BETWEEN
    case IN
    case BEGINS_WITH
    case CONTAINS
    case ATTRIBUTE_EXISTS
    case ATTRIBUTE_NOT_EXISTS
    case ATTRIBUTE_TYPE

    inline def writeAsExpression(name: String, values: Seq[String]): String =
      this match {
        case CompareFunction.EQUAL =>
          s"$name = ${values.head}"
        case CompareFunction.NOT_EQUAL =>
          s"$name <> ${values.head}"
        case CompareFunction.LOWER_THAN =>
          s"$name < ${values.head}"
        case CompareFunction.LOWER_THAN_OR_EQUAL =>
          s"$name <= ${values.head}"
        case CompareFunction.GREATER_THAN =>
          s"$name > ${values.head}"
        case CompareFunction.GREATER_THAN_OR_EQUAL =>
          s"$name >= ${values.head}"
        case CompareFunction.BETWEEN =>
          s"$name BETWEEN ${values(0)} AND ${values(1)}"
        case CompareFunction.IN =>
          s"$name IN (${values.mkString(", ")})"
        case CompareFunction.BEGINS_WITH =>
          s"begins_with($name,${values.head})"
        case CompareFunction.CONTAINS =>
          s"contains($name,${values.head})"
        case CompareFunction.ATTRIBUTE_EXISTS =>
          s"attribute_exists($name)"
        case CompareFunction.ATTRIBUTE_NOT_EXISTS =>
          s"attribute_not_exists($name)"
        case CompareFunction.ATTRIBUTE_TYPE =>
          s"attribute_type($name,${values.head})"
      }

    def compare(
        obtained: Option[AttributeValue],
        expected: Seq[AttributeValue]
    ): Boolean = {

      def compareNumbers(compare: (Double, Double) => Boolean) = (for {
        o <- obtained.flatMap(_.maybeDouble)
        e <- expected.headOption.flatMap(_.maybeDouble)
      } yield compare(o, e)).contains(true)

      this match {
        case CompareFunction.EQUAL                 => obtained == expected.headOption
        case CompareFunction.NOT_EQUAL             => obtained != expected.headOption
        case CompareFunction.LOWER_THAN            => compareNumbers(_ < _)
        case CompareFunction.LOWER_THAN_OR_EQUAL   => compareNumbers(_ <= _)
        case CompareFunction.GREATER_THAN          => compareNumbers(_ > _)
        case CompareFunction.GREATER_THAN_OR_EQUAL => compareNumbers(_ >= _)
        case CompareFunction.BETWEEN =>
          (for {
            o <- obtained.flatMap(_.maybeDouble)
            e1 <- expected.headOption.flatMap(_.maybeDouble)
            e2 <- expected.drop(1).headOption.flatMap(_.maybeDouble)
          } yield o >= e1 && o < e2).contains(true)
        case CompareFunction.IN =>
          obtained.exists(o => expected.exists(_ == o))
        case CompareFunction.BEGINS_WITH =>
          (for {
            o <- obtained.flatMap(_.maybeString)
            e <- expected.headOption.flatMap(_.maybeString)
          } yield o.startsWith(e)).contains(true)
        case CompareFunction.CONTAINS =>
          val operand = expected.head
          obtained match {
            case Some(value) if value.isString && operand.isString =>
              value.s() != operand.s() && value.s().contains(operand.s())
            case Some(value) if value.isSetOfStrings && operand.isString =>
              value.ss().contains(operand.s())
            case Some(value) if value.isSetOfNumbers && operand.isNumber =>
              value.ns().contains(operand.n())
            case Some(value) if value.isList =>
              value.l().contains(operand)
            case _ => false
          }
        case CompareFunction.ATTRIBUTE_EXISTS =>
          obtained.isDefined
        case CompareFunction.ATTRIBUTE_NOT_EXISTS =>
          obtained.isEmpty
        case ATTRIBUTE_TYPE =>
          obtained
            .flatMap(o =>
              expected.headOption
                .flatMap(_.maybeString)
                .map(_ == o.getTypeDescriptor)
            )
            .getOrElse(false)
      }
    }

  }

  object CompareFunction {
    inline def from(token: String): CompareFunction =
      token.trim() match {
        case "="       => CompareFunction.EQUAL
        case "<>"      => CompareFunction.NOT_EQUAL
        case "<"       => CompareFunction.LOWER_THAN
        case "<="      => CompareFunction.LOWER_THAN_OR_EQUAL
        case ">"       => CompareFunction.GREATER_THAN
        case ">="      => CompareFunction.GREATER_THAN_OR_EQUAL
        case "BETWEEN" => CompareFunction.BETWEEN
        case "IN"      => CompareFunction.IN
        case _ =>
          throw new Exception(s"Unsupported compare function '$token'")
      }

    val isAllowedInSortKey: Set[CompareFunction] =
      Set(
        CompareFunction.EQUAL,
        CompareFunction.LOWER_THAN,
        CompareFunction.LOWER_THAN_OR_EQUAL,
        CompareFunction.GREATER_THAN,
        CompareFunction.GREATER_THAN_OR_EQUAL,
        CompareFunction.BETWEEN,
        CompareFunction.BEGINS_WITH
      )
  }

  given fromString: Conversion[String, AttributeValue] with
    inline def apply(value: String): AttributeValue =
      AttributeValue.builder().s(value).build()

  given fromInt: Conversion[Int, AttributeValue] with
    inline def apply(value: Int): AttributeValue =
      AttributeValue.builder().n(value.toString()).build()

  given fromLong: Conversion[Long, AttributeValue] with
    inline def apply(value: Long): AttributeValue =
      AttributeValue.builder().n(value.toString()).build()

  given fromBoolean: Conversion[Boolean, AttributeValue] with
    inline def apply(value: Boolean): AttributeValue =
      AttributeValue.builder().bool(value).build()

  given fromDouble: Conversion[Double, AttributeValue] with
    inline def apply(value: Double): AttributeValue =
      AttributeValue.builder().n(value.toString()).build()

  given fromBigDecimal: Conversion[BigDecimal, AttributeValue] with
    inline def apply(value: BigDecimal): AttributeValue =
      AttributeValue.builder().n(value.toString()).build()

  given fromDynamoDbItem: Conversion[DynamoDbItem, AttributeValue] with
    inline def apply(value: DynamoDbItem): AttributeValue =
      AttributeValue.builder().m(value.asJava).build()

  given fromOption[T](using
      Conversion[T, AttributeValue]
  ): Conversion[Option[T], AttributeValue] with
    inline def apply(value: Option[T]): AttributeValue =
      value match {
        case Some(value) =>
          summon[Conversion[T, AttributeValue]].apply(value)
        case None =>
          AttributeValue
            .builder()
            .nul(true)
            .build()
      }

  given fromMap[T](using
      Conversion[T, AttributeValue]
  ): Conversion[Map[String, T], AttributeValue] with
    inline def apply(value: Map[String, T]): AttributeValue =
      AttributeValue
        .builder()
        .m(
          value.view.mapValues(summon[Conversion[T, AttributeValue]]).toMap.asJava
        )
        .build()

  given fromIterableOfAttributeValue: Conversion[Iterable[AttributeValue], AttributeValue] with
    inline def apply(value: Iterable[AttributeValue]): AttributeValue =
      if (value.forall(_.isString))
      then
        AttributeValue
          .builder()
          .ss(value.map(_.s()).toSeq.asJava)
          .build()
      else if (value.forall(_.isNumber))
      then
        AttributeValue
          .builder()
          .ns(value.map(_.n()).toSeq.asJava)
          .build()
      else if (value.forall(_.isByteArray))
      then
        AttributeValue
          .builder()
          .bs(value.map(_.b()).toSeq.asJava)
          .build()
      else
        AttributeValue
          .builder()
          .l(value.toSeq.asJava)
          .build()

  given fromIterableOfString: Conversion[Iterable[String], AttributeValue] with
    inline def apply(value: Iterable[String]): AttributeValue =
      AttributeValue
        .builder()
        .ss(value.toSeq.asJava)
        .build()

  given fromIterableOfInt: Conversion[Iterable[Int], AttributeValue] with
    inline def apply(value: Iterable[Int]): AttributeValue =
      AttributeValue
        .builder()
        .ns(value.map(_.toString()).toSeq.asJava)
        .build()

  given fromIterableOfLong: Conversion[Iterable[Long], AttributeValue] with
    inline def apply(value: Iterable[Long]): AttributeValue =
      AttributeValue
        .builder()
        .ns(value.map(_.toString()).toSeq.asJava)
        .build()

  given fromIterableOfDouble: Conversion[Iterable[Double], AttributeValue] with
    inline def apply(value: Iterable[Double]): AttributeValue =
      AttributeValue
        .builder()
        .ns(value.map(_.toString()).toSeq.asJava)
        .build()

  given fromIterableOfBigDecimal: Conversion[Iterable[BigDecimal], AttributeValue] with
    inline def apply(value: Iterable[BigDecimal]): AttributeValue =
      AttributeValue
        .builder()
        .ns(value.map(_.toString()).toSeq.asJava)
        .build()

  given fromIterableOfFloat: Conversion[Iterable[Float], AttributeValue] with
    inline def apply(value: Iterable[Float]): AttributeValue =
      AttributeValue
        .builder()
        .ns(value.map(_.toString()).toSeq.asJava)
        .build()

  given fromIterable[T](using
      Conversion[T, AttributeValue]
  ): Conversion[Iterable[T], AttributeValue] with
    inline def apply(value: Iterable[T]): AttributeValue =
      inline scala.compiletime.erasedValue[T] match {
        case _: Int | _: Short | _: Long | _: Double | _: Float | _: BigDecimal | _: BigInt =>
          AttributeValue
            .builder()
            .ns(
              value
                .asInstanceOf[Iterable[Any]]
                .map(_.toString())
                .toSeq
                .asJava
            )
            .build()

        case _: Byte =>
          val array = Array.ofDim[Byte](value.size)
          value
            .asInstanceOf[Iterable[Byte]]
            .zipWithIndex
            .foreach((b, i) => array(i) = b)
          AttributeValue
            .builder()
            .b(SdkBytes.fromByteArrayUnsafe(array))
            .build()

        case _: String =>
          AttributeValue
            .builder()
            .ss(
              value
                .asInstanceOf[Iterable[String]]
                .toSeq
                .asJava
            )
            .build()

        case _ =>
          AttributeValue
            .builder()
            .l(
              value
                .map(
                  scala.compiletime.summonInline[Conversion[T, AttributeValue]]
                )
                .toSeq
                .asJava
            )
            .build()
      }

  given toValue[T](using
      Conversion[T, AttributeValue]
  ): Conversion[T, AttributeValueUpdate] with
    inline def apply(value: T): AttributeValueUpdate =
      AttributeValueUpdate
        .builder()
        .value(value)
        .action(AttributeAction.PUT)
        .build()

  given toUpdate: Conversion[AttributeValue, AttributeValueUpdate] with
    inline def apply(value: AttributeValue): AttributeValueUpdate =
      if (value eq REMOVE)
      then DELETE_PROPERTY
      else
        AttributeValueUpdate
          .builder()
          .value(value)
          .action(AttributeAction.PUT)
          .build()

  val convertFromJson: Conversion[ujson.Value, AttributeValue] =
    new Conversion[ujson.Value, AttributeValue] {
      inline def apply(value: ujson.Value): AttributeValue =
        value match {
          case Str(s) =>
            AttributeValue.builder().s(s).build()
          case num: Num =>
            AttributeValue.builder().n(ujson.write(num)).build()
          case ujson.Bool(b) =>
            AttributeValue.builder().bool(b).build()
          case Obj(value) =>
            AttributeValue
              .builder()
              .m(value.view.mapValues(convertFromJson).toMap.asJava)
              .build()
          case Arr(array) =>
            val attrs = array.map(convertFromJson)
            if (
              attrs.forall(_.isString) && {
                val strings = attrs.map(_.s())
                strings.toSet.size == strings.size
              }
            )
            then
              AttributeValue
                .builder()
                .ss(attrs.map(_.s()).asJava)
                .build()
            else if (
              attrs.forall(_.isNumber) && {
                val numbers = attrs.map(_.n())
                numbers.toSet.size == numbers.size
              }
            )
            then
              AttributeValue
                .builder()
                .ns(attrs.map(_.n()).asJava)
                .build()
            else if (
              attrs.forall(_.isByteArray) && {
                val binaries = attrs.map(_.b())
                binaries.toSet.size == binaries.size
              }
            )
            then
              AttributeValue
                .builder()
                .bs(attrs.map(_.b()).asJava)
                .build()
            else
              AttributeValue
                .builder()
                .l(attrs.asJava)
                .build()
          case ujson.Null =>
            nullAttributeValue
        }
    }

  inline def mapOf(value: DynamoDbItemField*): AttributeValue =
    AttributeValue
      .builder()
      .m(value.toMap.asJava)
      .build()

  inline def listOf(value: AttributeValue*): AttributeValue =
    AttributeValue
      .builder()
      .l(value.toSeq.asJava)
      .build()

  inline def setOfStrings(value: String*): AttributeValue =
    AttributeValue
      .builder()
      .ss(value.toSet.asJava)
      .build()

  inline def setOfNumbers[N: Numeric](value: N*): AttributeValue =
    AttributeValue
      .builder()
      .ns(value.map(_.toString()).toSet.asJava)
      .build()

  inline def add(value: AttributeValue): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(value)
      .build()

  inline def addToSet(string: String): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(AttributeValue.builder().ss(string).build())
      .build()

  inline def addToSet(strings: Set[String]): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(AttributeValue.builder().ss(strings.asJava).build())
      .build()

  inline def removeFromSet(string: String): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .value(AttributeValue.builder().ss(string).build())
      .build()

  inline def removeFromSet[T](set: Set[T]): AttributeValueUpdate =
    inline erasedValue[T] match {
      case _: String =>
        AttributeValueUpdate
          .builder()
          .action(AttributeAction.DELETE)
          .value(AttributeValue.builder().ss(set.map(_.asInstanceOf[String]).asJava).build())
          .build()
      case _: Int | _: Long | _: Double | _: Float | _: BigDecimal =>
        AttributeValueUpdate
          .builder()
          .action(AttributeAction.DELETE)
          .value(AttributeValue.builder().ns(set.map(_.toString()).asJava).build())
          .build()
      case _ =>
        error("Unsupported type for set operations, must be String, Int, Long, Double, Float or BigDecimal")
    }

  inline def addToSet(num: Int): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def removeFromSet(num: Int): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def addToSet(num: Long): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def removeFromSet(num: Long): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def addToSet(num: Double): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def removeFromSet(num: Double): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def addToSet(num: Float): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def removeFromSet(num: Float): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def addToSet(num: BigDecimal): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.ADD)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def removeFromSet(num: BigDecimal): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action(AttributeAction.DELETE)
      .value(AttributeValue.builder().ns(num.toString()).build())
      .build()

  inline def appendToList(values: AttributeValue*): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action("SET:list_append")
      .value(AttributeValue.builder().l(values.asJava).build())
      .build()

  inline def prependToList(values: AttributeValue*): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action("SET:list_prepend")
      .value(AttributeValue.builder().l(values.asJava).build())
      .build()

  inline def removeFromList(index: Int): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action("REMOVE:index")
      .value(AttributeValue.builder().n(index.toString()).build())
      .build()

  inline def ifNotExists(value: AttributeValue): AttributeValueUpdate =
    AttributeValueUpdate
      .builder()
      .action("SET:if_not_exists")
      .value(value)
      .build()

  given fromValue: Conversion[ujson.Value, AttributeValue] with
    inline def apply(value: ujson.Value): AttributeValue = {
      value.match {
        case ujson.Null     => AttributeValue.builder().nul(true).build()
        case ujson.Str(v)   => fromString(v)
        case obj: ujson.Obj => obj.toAttributeValue
        case ujson.Arr(v) =>
          AttributeValue.builder().l(v.map(fromValue).asJava).build()
        case ujson.Num(v) => fromDouble(v)
        case ujson.False  => fromBoolean(false)
        case ujson.True   => fromBoolean(true)
      }
    }

  given fromEntity[T: Writer]: Conversion[T, AttributeValue] with
    inline def apply(value: T): AttributeValue = {
      fromValue(upickle.default.writeJs(value))
    }

  private def showMetadata(metadata: DynamoDbResponseMetadata): String =
    s"requestId = ${metadata.requestId()}".stripMargin

  inline def nullAttributeValue =
    AttributeValue.builder().nul(true).build()

  extension (thisValue: AttributeValueUpdate) {
    inline def requiresUpdateExpression: Boolean =
      thisValue.actionAsString().startsWith("SET:")
        || thisValue.actionAsString().startsWith("REMOVE:")
  }

  extension (thisValue: AttributeValue) {

    inline def isNull: Boolean =
      thisValue.nul() != null && thisValue.nul().booleanValue()
    inline def isString: Boolean = thisValue.s() != null
    inline def isNumber: Boolean = thisValue.n() != null
    inline def isBoolean: Boolean = thisValue.bool() != null
    inline def isByteArray: Boolean = thisValue.b() != null
    inline def isMap: Boolean = thisValue.hasM()
    inline def isList: Boolean = thisValue.hasL()
    inline def isSetOfStrings: Boolean = thisValue.hasSs()
    inline def isSetOfNumbers: Boolean = thisValue.hasNs()
    inline def isSetOfBinary: Boolean = thisValue.hasBs()
    inline def isSet: Boolean =
      isSetOfStrings || isSetOfNumbers || isSetOfBinary
    inline def isBlank: Boolean =
      isString && thisValue.s().isBlank()
    inline def isEmpty: Boolean =
      (isList && thisValue.l().isEmpty())
        || (isString && thisValue.s().isEmpty())

    def isSameAs(otherValue: AttributeValue): Boolean =
      thisValue.getTypeDescriptor == otherValue.getTypeDescriptor
        && {
          if (thisValue.isString) then thisValue.s() == otherValue.s()
          else if (thisValue.isNumber) then thisValue.n() == otherValue.n()
          else if (thisValue.isBoolean) then thisValue.bool() == otherValue.bool()
          else if (thisValue.isByteArray) then thisValue.b() == otherValue.b()
          else if (thisValue.isMap) then
            thisValue.m().size() == otherValue.m().size()
            && thisValue
              .m()
              .asScala
              .iterator
              .forall((k, v1) =>
                otherValue
                  .m()
                  .asScala
                  .get(k)
                  .exists(v2 => v1.isSameAs(v2))
              )
          else if (thisValue.isList) then
            thisValue.l().size() == otherValue.l().size()
            && thisValue.l().asScala.zip(otherValue.l().asScala).forall((l, r) => l.isSameAs(r))
          else if (thisValue.isSetOfStrings) then
            thisValue.ss().size() == otherValue.ss().size()
            && thisValue.ss().asScala.forall(s => otherValue.ss().contains(s))
          else if (thisValue.isSetOfNumbers) then
            thisValue.ns().size() == otherValue.ns().size()
            && thisValue.ns().asScala.forall(s => otherValue.ns().contains(s))
          else if (thisValue.isSetOfBinary) then
            thisValue.bs().size() == otherValue.bs().size()
            && thisValue.bs().asScala.forall(s => otherValue.bs().contains(s))
          else false
        }

    inline def getTypeDescriptor: String =
      if (thisValue.isString) then "S"
      else if (thisValue.isNumber) then "N"
      else if (thisValue.isBoolean) then "BOOL"
      else if (thisValue.isByteArray) then "B"
      else if (thisValue.isMap) then "M"
      else if (thisValue.isList) then "L"
      else if (thisValue.isSetOfStrings) then "SS"
      else if (thisValue.isSetOfNumbers) then "NS"
      else if (thisValue.isSetOfBinary) then "BS"
      else "NULL"

    inline def show: String =
      ujson.write(thisValue.convertToDynamoJson)

    inline def getString(default: => String): String =
      Option(thisValue.s()).getOrElse(default)

    inline def maybeString: Option[String] =
      Option(thisValue.s())

    inline def getInt(default: => Int): Int =
      Option(thisValue.n())
        .flatMap(_.toDoubleOption)
        .map(_.toInt)
        .getOrElse(default)

    inline def maybeInt: Option[Int] =
      Option(thisValue.n())
        .flatMap(_.toDoubleOption)
        .map(_.toInt)

    inline def getLong(default: => Long): Long =
      Option(thisValue.n())
        .flatMap(n => n.toLongOption.orElse(n.toDoubleOption.map(_.toLong)))
        .getOrElse(default)

    inline def maybeLong: Option[Long] =
      Option(thisValue.n())
        .flatMap(n => n.toLongOption.orElse(n.toDoubleOption.map(_.toLong)))

    inline def getDecimal(default: => BigDecimal): BigDecimal =
      Option(thisValue.n())
        .flatMap(x => Try(BigDecimal.apply(x)).toOption)
        .getOrElse(default)

    inline def maybeDecimal: Option[BigDecimal] =
      Option(thisValue.n())
        .flatMap(x => Try(BigDecimal.apply(x)).toOption)

    inline def getDouble(default: => Double): Double =
      Option(thisValue.n())
        .flatMap(_.toDoubleOption)
        .getOrElse(default)

    inline def maybeDouble: Option[Double] =
      Option(thisValue.n())
        .flatMap(_.toDoubleOption)

    inline def getBoolean(default: Boolean): Boolean =
      Option(thisValue.bool())
        .map(_.booleanValue())
        .getOrElse(default)

    inline def maybeBoolean: Option[Boolean] =
      Option(thisValue.bool()).map(_.booleanValue())

    inline def getNestedItem(
        default: => DynamoDbItem
    ): DynamoDbItem =
      (if (thisValue.hasM()) then Option(thisValue.m()) else None)
        .map(_.asScala)
        .getOrElse(default)

    inline def maybeNestedItem: Option[DynamoDbItem] =
      (if (thisValue.hasM()) then Option(thisValue.m()) else None)
        .map(_.asScala)

    inline def getStringList(
        default: => collection.Seq[String]
    ): collection.Seq[String] =
      (if (thisValue.hasSs()) then Option(thisValue.ss()) else None)
        .map(_.asScala)
        .getOrElse(default)

    inline def maybeStringList: Option[collection.Seq[String]] =
      (if (thisValue.hasSs()) then Option(thisValue.ss()) else None)
        .map(_.asScala)

    inline def getIntSet(
        default: => collection.Seq[Int]
    ): collection.Seq[Int] =
      (if (thisValue.hasNs()) then Option(thisValue.ns()) else None)
        .map(_.asScala.map(_.toInt))
        .getOrElse(default)

    inline def maybeIntSet: Option[collection.Seq[Int]] =
      (if (thisValue.hasNs()) then Option(thisValue.ns()) else None)
        .map(_.asScala.map(_.toInt))

    inline def getDoubleSet(
        default: => collection.Seq[Double]
    ): collection.Seq[Double] =
      (if (thisValue.hasNs()) then Option(thisValue.ns()) else None)
        .map(_.asScala.map(_.toDouble))
        .getOrElse(default)

    inline def maybeDoubleSet: Option[collection.Seq[Double]] =
      (if (thisValue.hasNs()) then Option(thisValue.ns()) else None)
        .map(_.asScala.map(_.toDouble))

    inline def getList(
        default: => collection.Seq[AttributeValue]
    ): collection.Seq[AttributeValue] =
      (if (thisValue.hasL()) then Option(thisValue.l()) else None)
        .map(_.asScala)
        .getOrElse(default)

    inline def maybeList: Option[collection.Seq[AttributeValue]] =
      (if (thisValue.hasL()) then Option(thisValue.l()) else None)
        .map(_.asScala)

    inline def getOrDefault[T: Reader](default: => T): T =
      (Option(thisValue.s()))
        .flatMap(x =>
          optionally(upickle.default.read(x))
            .orElse(optionally(upickle.default.read(s"\"$x\"")))
        )
        .orElse(
          Option(thisValue.n()).flatMap(x => optionally(upickle.default.read(x)))
        )
        .getOrElse(default)

    inline def getOrDefault[T: Reader](default: String): T =
      (Option(thisValue.s()))
        .flatMap(x =>
          optionally(upickle.default.read(x))
            .orElse(optionally(upickle.default.read(s"\"$x\"")))
        )
        .getOrElse(
          optionally(upickle.default.read(default))
            .getOrElse(upickle.default.read(s"\"$default\""))
        )

    inline def getOrDefault[T: Reader](default: Int): T =
      (Option(thisValue.n()))
        .flatMap(x =>
          optionally(upickle.default.read(x))
            .orElse(optionally(upickle.default.read(s"\"$x\"")))
        )
        .getOrElse(upickle.default.read(default.toString()))

    inline def unsafeGet[T]: T = {
      inline erasedValue[T] match {
        case _: String      => thisValue.s().asInstanceOf[T]
        case _: Int         => thisValue.n().toInt.asInstanceOf[T]
        case _: Long        => thisValue.n().toLong.asInstanceOf[T]
        case _: Double      => thisValue.n().toDouble.asInstanceOf[T]
        case _: Float       => thisValue.n().toFloat.asInstanceOf[T]
        case _: Boolean     => thisValue.bool().asInstanceOf[T]
        case _: BigDecimal  => BigDecimal(thisValue.n()).asInstanceOf[T]
        case _: Array[Byte] => thisValue.b.asByteArrayUnsafe().asInstanceOf[T]
        case _: Set[String] =>
          if (thisValue.hasSs()) then thisValue.ss().asScala.toSet.asInstanceOf[T]
          else Set.empty.asInstanceOf[T]
        case _: Set[Int] =>
          if (thisValue.hasNs()) then thisValue.ns().asScala.toSet.map(_.toInt).asInstanceOf[T]
          else Set.empty.asInstanceOf[T]
        case _: Set[Long] =>
          if (thisValue.hasNs()) then thisValue.ns().asScala.toSet.map(_.toLong).asInstanceOf[T]
          else Set.empty.asInstanceOf[T]
        case _: Set[Double] =>
          if (thisValue.hasNs()) then thisValue.ns().asScala.toSet.map(_.toDouble).asInstanceOf[T]
          else Set.empty.asInstanceOf[T]
        case _: Set[Float] =>
          if (thisValue.hasNs()) then thisValue.ns().asScala.toSet.map(_.toFloat).asInstanceOf[T]
          else Set.empty.asInstanceOf[T]
        case _: Set[BigDecimal] =>
          if (thisValue.hasNs()) then thisValue.ns().asScala.toSet.map(BigDecimal.apply).asInstanceOf[T]
          else Set.empty.asInstanceOf[T]
        case _: Seq[t] =>
          if (thisValue.hasL()) then thisValue.l().asScala.map(_.unsafeGet[t]).asInstanceOf[T]
          else Seq.empty.asInstanceOf[T]
        case _ =>
          summonFrom {
            case given Reader[T] =>
              optionally(upickle.default.read(thisValue.s()))
                .orElse(optionally(upickle.default.read(s"\"${thisValue.s()}\"")))
                .orElse(optionally(upickle.default.read(thisValue.n())))
                .getOrElse(
                  if (thisValue.hasM()) then upickle.default.read[T](DynamoDbItem.from(thisValue.m()).convertToJson)
                  else throw new Exception(s"Cannot get type T out of AttributeValue")
                )
            case _ =>
              error("Requested type T must have an implicit Reader[T] in scope")
          }
      }
    }

    inline def maybe[T]: Option[T] =
      inline erasedValue[T] match {
        case _: String => Option(thisValue.s()).asInstanceOf[Option[T]]
        case _: Int    => Option(thisValue.n()).flatMap(_.toDoubleOption).map(_.toInt).asInstanceOf[Option[T]]
        case _: Long =>
          Option(thisValue.n())
            .flatMap(n => n.toLongOption.orElse(n.toDoubleOption.map(_.toLong)))
            .asInstanceOf[Option[T]]
        case _: Float       => Option(thisValue.n()).flatMap(_.toDoubleOption).map(_.toFloat).asInstanceOf[Option[T]]
        case _: Double      => Option(thisValue.n()).flatMap(_.toDoubleOption).asInstanceOf[Option[T]]
        case _: Boolean     => Option(thisValue.bool()).asInstanceOf[Option[T]]
        case _: BigDecimal  => Option(thisValue.n()).flatMap(s => optionally(BigDecimal(s))).asInstanceOf[Option[T]]
        case _: Array[Byte] => Option(thisValue.b).map(_.asByteArrayUnsafe()).asInstanceOf[Option[T]]
        case _: Set[String] =>
          if (thisValue.hasSs()) then Option(thisValue.ss()).map(_.asScala.toSet).asInstanceOf[Option[T]]
          else None
        case _: Set[Int] =>
          if (thisValue.hasNs()) then Option(thisValue.ns()).map(_.asScala.toSet.map(_.toInt)).asInstanceOf[Option[T]]
          else None
        case _: Set[Long] =>
          if (thisValue.hasNs()) then Option(thisValue.ns()).map(_.asScala.toSet.map(_.toLong)).asInstanceOf[Option[T]]
          else None
        case _: Set[Double] =>
          if (thisValue.hasNs()) then
            Option(thisValue.ns()).map(_.asScala.toSet.map(_.toDouble)).asInstanceOf[Option[T]]
          else None
        case _: Set[Float] =>
          if (thisValue.hasNs()) then Option(thisValue.ns()).map(_.asScala.toSet.map(_.toFloat)).asInstanceOf[Option[T]]
          else None
        case _: Set[BigDecimal] =>
          if (thisValue.hasNs()) then
            Option(thisValue.ns()).map(_.asScala.toSet.map(BigDecimal.apply)).asInstanceOf[Option[T]]
          else None
        case _: Seq[t] =>
          if (thisValue.hasL()) then Option(thisValue.l()).map(_.asScala.map(_.unsafeGet[t])).asInstanceOf[Option[T]]
          else None
        case _ =>
          summonFrom {
            case given Reader[T] =>
              Option(thisValue.s())
                .flatMap(x =>
                  optionally(upickle.default.read(x))
                    .orElse(optionally(upickle.default.read(s"\"$x\"")))
                )
                .orElse(
                  Option(thisValue.n()).flatMap(x => optionally(upickle.default.read(x)))
                )
                .orElse(
                  Option(thisValue.m()).flatMap(x =>
                    optionally(
                      upickle.default.read[T](DynamoDbItem.from(x).convertToJson)
                    )
                  )
                )
            case _ =>
              error("Requested type T must have an implicit Reader[T] in scope")
          }
      }

    inline def maybe[T: Reader](pathSeq: String*): Option[T] =
      getByPath(pathSeq).flatMap(_.maybe[T])

    inline def maybeClass[T <: Product](using mirror: Mirror.ProductOf[T]): Option[T] =
      if (thisValue.hasM())
      then Option(thisValue.m()).flatMap(_.asScala.maybeClass[T])
      else None

    inline def getByPath(path: String): Option[AttributeValue] =
      val pathSeq: Array[String] = path.split("[.\\[\\]]").filterNot(_.isBlank())
      getByPath(ArraySeq.unsafeWrapArray(pathSeq))

    def getByPath(path: Iterable[String]): Option[AttributeValue] =
      path.headOption.match {
        case None =>
          Some(thisValue)
        case Some(key) =>
          val value: Option[AttributeValue] =
            if (thisValue.hasM()) then thisValue.m().asScala.get(key)
            else {
              key.toIntOption.flatMap { index =>
                thisValue.getByIndex(index)
              }
            }
          value.flatMap(_.getByPath(path.tail))
      }

    def setByPath(
        path: Iterable[String],
        value: AttributeValue
    ): AttributeValue =
      path.headOption.match {
        case None => value
        case Some(key) =>
          if (thisValue.hasM()) then
            val self = thisValue.m().asScala
            fromDynamoDbItem(
              self
                .get(key)
                .map(_.setByPath(path.tail, value))
                .map(newValue => self.clone().addOne(key, newValue))
                .getOrElse(
                  self.clone().addOne(key, createNewAtrributeValue(path.tail, value))
                )
            )
          else {
            key.toIntOption
              .map { index =>
                thisValue
                  .getByIndex(index)
                  .map(_.setByPath(path.tail, value))
                  .map(newValue => thisValue.setByIndex(index, newValue))
                  .getOrElse(
                    thisValue.setByIndex(
                      index,
                      createNewAtrributeValue(path.tail, value)
                    )
                  )
              }
              .getOrElse(thisValue)
          }
      }

    def removeByPath(path: Iterable[String]): AttributeValue =
      if (path.isEmpty)
      then {
        throw new IllegalStateException(
          "Unexpected state, empty path in removeByPath"
        )
      } else {
        val key = path.head
        if (path.size == 1)
        then {
          if (thisValue.hasM()) then
            val self = thisValue.m().asScala
            fromDynamoDbItem(self.view.filterKeys(_ != key).toMap)
          else {
            key.toIntOption
              .map { index =>
                thisValue.removeByIndex(index)
              }
              .getOrElse(thisValue)
          }
        } else {
          if (thisValue.hasM()) then
            val self = thisValue.m().asScala
            fromDynamoDbItem(
              self
                .get(key)
                .map(_.removeByPath(path.tail))
                .map(newValue => self.clone().addOne(key, newValue))
                .getOrElse(self)
            )
          else {
            key.toIntOption
              .map { index =>
                thisValue
                  .getByIndex(index)
                  .map(_.removeByPath(path.tail))
                  .map(newValue => thisValue.setByIndex(index, newValue))
                  .getOrElse(thisValue)
              }
              .getOrElse(thisValue)
          }
        }
      }

    def getByIndex(index: Int): Option[AttributeValue] =
      if (thisValue.hasL()) then
        try (Some(thisValue.l().get(index)))
        catch { case NonFatal(e) => None }
      else if (thisValue.hasNs()) then
        try {
          val n = thisValue.ns().get(index)
          Some(AttributeValue.builder().n(n).build())
        } catch { case NonFatal(e) => None }
      else if (thisValue.hasSs()) then
        try {
          val s = thisValue.ss().get(index)
          Some(AttributeValue.builder().s(s).build())
        } catch { case NonFatal(e) => None }
      else if (thisValue.hasBs()) then
        try {
          val bs = thisValue.bs().get(index)
          Some(AttributeValue.builder().bs(bs).build())
        } catch { case NonFatal(e) => None }
      else None

    def setByIndex(index: Int, value: AttributeValue): AttributeValue =
      val isNonPrimitive =
        value.hasM() || value.hasL() || value.hasNs() || value.hasSs() || value
          .hasBs()
      val isBytes = Option(value.b()).isDefined
      val isString = Option(value.s()).isDefined
      val isNumber = Option(value.n()).isDefined
      val isBoolean = Option(value.bool()).isDefined
      if (thisValue.hasL()) then {
        AttributeValue
          .builder()
          .l(
            thisValue
              .l()
              .asScala
              .putAt(index, value, nullAttributeValue)
              .asJava
          )
          .build()
      } else if (thisValue.hasSs()) then {
        if (isNonPrimitive || isNumber || isBoolean || isBytes)
        then
          AttributeValue
            .builder()
            .l(
              thisValue
                .ss()
                .asScala
                .map(fromString)
                .putAt(index, value, nullAttributeValue)
                .asJava
            )
            .build()
        else
          AttributeValue
            .builder()
            .ss(thisValue.ss().asScala.putAt(index, value.s(), "").asJava)
            .build()
      } else if (thisValue.hasNs()) then {
        if (isNonPrimitive || isString || isBoolean || isBytes)
        then
          AttributeValue
            .builder()
            .l(
              thisValue
                .ns()
                .asScala
                .map(n => fromInt(n.toInt))
                .putAt(index, value, nullAttributeValue)
                .asJava
            )
            .build()
        else
          AttributeValue
            .builder()
            .ns(thisValue.ns().asScala.putAt(index, value.n(), "0").asJava)
            .build()
      } else if (thisValue.hasBs()) then {
        if (isNonPrimitive || isString || isBoolean || isNumber)
        then
          AttributeValue
            .builder()
            .l(
              thisValue
                .bs()
                .asScala
                .map(b => AttributeValue.builder().b(b).build())
                .putAt(index, value, nullAttributeValue)
                .asJava
            )
            .build()
        else
          AttributeValue
            .builder()
            .bs(
              thisValue
                .bs()
                .asScala
                .putAt(
                  index,
                  value.b(),
                  SdkBytes.fromByteArray(Array.emptyByteArray)
                )
                .asJava
            )
            .build()
      } else thisValue

    def removeByIndex(index: Int): AttributeValue =
      if (thisValue.hasL()) then {
        AttributeValue
          .builder()
          .l(
            thisValue
              .l()
              .asScala
              .removeAt(index)
              .asJava
          )
          .build()
      } else thisValue

    def convertToJson: ujson.Value =
      if (thisValue == null) ujson.Obj("NULL" -> ujson.Bool(true))
      else
        Option(thisValue.s())
          .map(s => ujson.Str(s))
          .orElse(Option(thisValue.n()).map(n => ujson.Num(n.toDouble)))
          .orElse(Option(thisValue.bool()).map(b => ujson.Bool(b)))
          .orElse(Option(thisValue.b()).map { b =>
            ujson.Str(
              String(
                Base64.getEncoder().encode(b.asByteArrayUnsafe()),
                StandardCharsets.UTF_8
              )
            )
          })
          .getOrElse {
            if (thisValue.hasM()) then thisValue.m().asScala.convertToJson
            else if (thisValue.hasL()) then ujson.Arr.from(thisValue.l().asScala.map(_.convertToJson))
            else if (thisValue.hasSs()) then ujson.Arr.from(thisValue.ss().asScala.map(s => ujson.Str(s)))
            else if (thisValue.hasNs()) then
              ujson.Arr.from(
                thisValue.ns().asScala.map(n => ujson.Num(n.toDouble))
              )
            else if (thisValue.hasBs()) then
              ujson.Arr.from(
                thisValue
                  .bs()
                  .asScala
                  .map(b =>
                    ujson.Str(
                      String(
                        Base64.getEncoder().encode(b.asByteArrayUnsafe()),
                        StandardCharsets.UTF_8
                      )
                    )
                  )
              )
            else ujson.Null
          }

    def convertToDynamoJson: ujson.Value =
      if (thisValue == null) ujson.Obj("NULL" -> ujson.Bool(true))
      else
        Option(thisValue.s())
          .map(s => ujson.Obj("S" -> ujson.Str(s)))
          .orElse(
            Option(thisValue.n()).map(n => ujson.Obj("N" -> ujson.Num(n.toDouble)))
          )
          .orElse(
            Option(thisValue.bool()).map(b => ujson.Obj("BOOL" -> ujson.Bool(b)))
          )
          .orElse(Option(thisValue.b()).map { b =>
            ujson.Obj(
              "B" -> ujson.Str(
                String(
                  Base64.getEncoder().encode(b.asByteArrayUnsafe()),
                  StandardCharsets.UTF_8
                )
              )
            )
          })
          .getOrElse {
            if (thisValue.hasM()) then ujson.Obj("M" -> thisValue.m().asScala.convertToJson)
            else if (thisValue.hasL()) then
              ujson.Obj(
                "L" -> ujson.Arr.from(thisValue.l().asScala.map(_.convertToJson))
              )
            else if (thisValue.hasSs()) then
              ujson.Obj(
                "SS" -> ujson.Arr
                  .from(thisValue.ss().asScala.map(s => ujson.Str(s)))
              )
            else if (thisValue.hasNs()) then
              ujson.Obj(
                "NS" -> ujson.Arr.from(
                  thisValue.ns().asScala.map(n => ujson.Num(n.toDouble))
                )
              )
            else if (thisValue.hasBs()) then
              ujson.Obj(
                "BS" -> ujson.Arr.from(
                  thisValue
                    .bs()
                    .asScala
                    .map(b =>
                      ujson.Str(
                        String(
                          Base64.getEncoder().encode(b.asByteArrayUnsafe()),
                          StandardCharsets.UTF_8
                        )
                      )
                    )
                )
              )
            else ujson.Obj("NULL" -> ujson.Bool(true))
          }

  }

  extension (key: (String, AttributeValue)) inline def show: String = s"${key._1}=${key._2.show}"

  extension [T: Writer](entity: T) {
    inline def toDynamoDbItem: DynamoDbItem =
      upickle.default.writeJs(entity).match {
        case obj: ujson.Obj => obj.toDynamoDbItem
        case other =>
          throw new Exception(
            s"Cannot convert primitive JSON value '$other' to DynamoDbItem"
          )
      }
  }

  extension (obj: ujson.Obj) {
    inline def toAttributeValue: AttributeValue =
      AttributeValue
        .builder()
        .m(obj.value.view.mapValues(fromValue).toMap.asJava)
        .build

    inline def toDynamoDbItem: DynamoDbItem =
      obj.value.view.mapValues(fromValue).toMap
  }
  extension (maybeItem: Option[DynamoDbItem]) {
    inline def maybe[T](key: String): Option[T] =
      maybeItem.flatMap(_.maybe[T](key))

    inline def maybe[T](pathSeq: String*): Option[T] =
      maybeItem.flatMap(_.maybe[T](pathSeq))

    inline def prettyPrint: String =
      maybeItem
        .map(item => ujson.write(item.convertToDynamoJson, indent = 2))
        .getOrElse("<none>")

    inline def show: String =
      maybeItem
        .map(item => ujson.write(item.convertToDynamoJson))
        .getOrElse("<none>")

    inline def debug: Unit = println(prettyPrint)
  }

  extension (item: DynamoDbItem) {

    inline def prettyPrint: String =
      ujson.write(item.convertToDynamoJson, indent = 2)

    inline def show: String =
      ujson.write(item.convertToDynamoJson)

    inline def debug: Unit = println(prettyPrint)

    inline def getString(inline key: String, default: String): String =
      item.get(key).flatMap(i => Option(i.s())).getOrElse(default)

    inline def maybeString(inline key: String): Option[String] =
      item.get(key).flatMap(i => Option(i.s()))

    inline def getInt(inline key: String, default: Int): Int =
      item
        .get(key)
        .flatMap(i => Option(i.n()))
        .flatMap(_.toIntOption)
        .getOrElse(default)

    inline def maybeInt(inline key: String): Option[Int] =
      item
        .get(key)
        .flatMap(i => Option(i.n()))
        .flatMap(_.toIntOption)

    inline def getDecimal(inline key: String, default: BigDecimal): BigDecimal =
      item
        .get(key)
        .flatMap(i => Option(i.n()))
        .flatMap(x => Try(BigDecimal.apply(x)).toOption)
        .getOrElse(default)

    inline def maybeDecimal(inline key: String): Option[BigDecimal] =
      item
        .get(key)
        .flatMap(i => Option(i.n()))
        .flatMap(x => Try(BigDecimal.apply(x)).toOption)

    inline def getDouble(inline key: String, default: Double): Double =
      item
        .get(key)
        .flatMap(i => Option(i.n()))
        .flatMap(_.toDoubleOption)
        .getOrElse(default)

    inline def maybeDouble(inline key: String): Option[Double] =
      item
        .get(key)
        .flatMap(i => Option(i.n()))
        .flatMap(_.toDoubleOption)

    inline def getBoolean(inline key: String, default: Boolean): Boolean =
      item
        .get(key)
        .flatMap(i => Option(i.bool()))
        .map(_.booleanValue())
        .getOrElse(default)

    inline def maybeBoolean(inline key: String): Option[Boolean] =
      item.get(key).flatMap(i => Option(i.bool())).map(_.booleanValue())

    inline def getNestedItem(
        inline key: String,
        default: DynamoDbItem
    ): DynamoDbItem =
      item
        .get(key)
        .flatMap(i => if (i.hasM()) then Option(i.m()) else None)
        .map(_.asScala)
        .getOrElse(default)

    inline def maybeNestedItem(
        inline key: String
    ): Option[DynamoDbItem] =
      item
        .get(key)
        .flatMap(i => if (i.hasM()) then Option(i.m()) else None)
        .map(_.asScala)

    inline def getStringSet(
        inline key: String,
        default: collection.Set[String]
    ): collection.Set[String] =
      item
        .get(key)
        .flatMap(i => if (i.hasSs()) then Option(i.ss()) else None)
        .map(_.asScala.toSet)
        .getOrElse(default)

    inline def maybeStringSet(
        inline key: String
    ): Option[collection.Set[String]] =
      item
        .get(key)
        .flatMap(i => if (i.hasSs()) then Option(i.ss()) else None)
        .map(_.asScala.toSet)

    inline def getIntSet(
        inline key: String,
        default: collection.Set[Int]
    ): collection.Set[Int] =
      item
        .get(key)
        .flatMap(i => if (i.hasNs()) then Option(i.ns()) else None)
        .map(_.asScala.map(_.toInt).toSet)
        .getOrElse(default)

    inline def maybeIntSet(
        inline key: String
    ): Option[collection.Set[Int]] =
      item
        .get(key)
        .flatMap(i => if (i.hasNs()) then Option(i.ns()) else None)
        .map(_.asScala.map(_.toInt).toSet)

    inline def getList(
        inline key: String,
        default: collection.Seq[AttributeValue]
    ): collection.Seq[AttributeValue] =
      item
        .get(key)
        .flatMap(i => if (i.hasL()) then Option(i.l()) else None)
        .map(_.asScala)
        .getOrElse(default)

    inline def maybeList(
        inline key: String
    ): Option[collection.Seq[AttributeValue]] =
      item
        .get(key)
        .flatMap(i => if (i.hasL()) then Option(i.l()) else None)
        .map(_.asScala)

    inline def maybeListOf[T: Reader](
        inline key: String
    ): Option[collection.Seq[T]] =
      item
        .get(key)
        .flatMap(i => if (i.hasL()) then Option(i.l()) else None)
        .map(_.asScala.map(_.maybe[T]).filter(_.isDefined).map(_.get))

    inline def maybeListOfClass[T <: Product](
        inline key: String
    )(using mirror: Mirror.ProductOf[T]): Option[Seq[T]] =
      item
        .get(key)
        .flatMap(i =>
          if (i.hasL())
          then Option(i.l()).map(_.asScala.map(_.maybeClass[T]).filter(_.isDefined).map(_.get).toSeq)
          else None
        )

    inline def getOrFail[T: Reader](inline key: String): T =
      item
        .get(key)
        .flatMap(i =>
          Option(i.s())
            .flatMap(x =>
              optionally(upickle.default.read(x))
                .orElse(optionally(upickle.default.read(s"\"$x\"")))
            )
            .orElse(
              Option(i.n()).flatMap(x => optionally(upickle.default.read(x)))
            )
            .orElse(
              Option(i.m()).flatMap(x =>
                optionally(
                  upickle.default.read[T](DynamoDbItem.from(x).convertToJson)
                )
              )
            )
        )
        .getOrElse(
          throw new IllegalStateException(
            s"DynamoDB item has no $key attribute, got $item"
          )
        )

    inline def getOrDefault[T: Reader](inline key: String, default: => T): T =
      item
        .get(key)
        .flatMap(i =>
          Option(i.s())
            .flatMap(x =>
              optionally(upickle.default.read(x))
                .orElse(optionally(upickle.default.read(s"\"$x\"")))
            )
            .orElse(
              Option(i.n()).flatMap(x => optionally(upickle.default.read(x)))
            )
            .orElse(
              Option(i.m()).flatMap(x =>
                optionally(
                  upickle.default.read[T](DynamoDbItem.from(x).convertToJson)
                )
              )
            )
        )
        .getOrElse(default)

    inline def getOrDefault[T: Reader](inline key: String, default: String): T =
      item
        .get(key)
        .flatMap(i =>
          Option(i.s())
            .flatMap(x =>
              optionally(upickle.default.read(x))
                .orElse(optionally(upickle.default.read(s"\"$x\"")))
            )
        )
        .getOrElse(
          optionally(upickle.default.read(default))
            .getOrElse(upickle.default.read(s"\"$default\""))
        )

    inline def maybe[T](inline key: String): Option[T] = {
      item
        .get(key)
        .flatMap { i =>
          inline erasedValue[T] match {
            case _: String => Option(i.s()).asInstanceOf[Option[T]]
            case _: Int    => Option(i.n()).flatMap(_.toDoubleOption).map(_.toInt).asInstanceOf[Option[T]]
            case _: Long =>
              Option(i.n()).flatMap(n => n.toLongOption.orElse(n.toDoubleOption.map(_.toLong))).asInstanceOf[Option[T]]
            case _: Float       => Option(i.n()).flatMap(_.toDoubleOption).map(_.toFloat).asInstanceOf[Option[T]]
            case _: Double      => Option(i.n()).flatMap(_.toDoubleOption).asInstanceOf[Option[T]]
            case _: Boolean     => Option(i.bool()).asInstanceOf[Option[T]]
            case _: BigDecimal  => Option(i.n()).flatMap(s => optionally(BigDecimal(s))).asInstanceOf[Option[T]]
            case _: Array[Byte] => Option(i.b).map(_.asByteArrayUnsafe()).asInstanceOf[Option[T]]
            case _: Set[String] =>
              if (i.hasSs()) then Option(i.ss()).map(_.asScala.toSet).asInstanceOf[Option[T]]
              else None
            case _: Set[Int] =>
              if (i.hasNs()) then Option(i.ns()).map(_.asScala.toSet.map(_.toInt)).asInstanceOf[Option[T]]
              else None
            case _: Set[Long] =>
              if (i.hasNs()) then Option(i.ns()).map(_.asScala.toSet.map(_.toLong)).asInstanceOf[Option[T]]
              else None
            case _: Set[Double] =>
              if (i.hasNs()) then Option(i.ns()).map(_.asScala.toSet.map(_.toDouble)).asInstanceOf[Option[T]]
              else None
            case _: Set[Float] =>
              if (i.hasNs()) then Option(i.ns()).map(_.asScala.toSet.map(_.toFloat)).asInstanceOf[Option[T]]
              else None
            case _: Set[BigDecimal] =>
              if (i.hasNs()) then Option(i.ns()).map(_.asScala.toSet.map(BigDecimal.apply)).asInstanceOf[Option[T]]
              else None
            case _: Seq[t] =>
              if (i.hasL()) then Option(i.l()).map(_.asScala.map(_.unsafeGet[t]).toSeq).asInstanceOf[Option[T]]
              else None
            case _: DynamoDbItem =>
              if (i.hasM()) then Option(i.m()).map(m => m.asScala).asInstanceOf[Option[T]]
              else None
            case _ =>
              summonFrom {
                case given Reader[T] =>
                  Option(i.s())
                    .flatMap(x =>
                      optionally(upickle.default.read(x))
                        .orElse(optionally(upickle.default.read(s"\"$x\"")))
                    )
                    .orElse(
                      Option(i.n()).flatMap(x => optionally(upickle.default.read(x)))
                    )
                    .orElse(
                      Option(i.m()).flatMap(x =>
                        optionally(
                          upickle.default.read[T](DynamoDbItem.from(x).convertToJson)
                        )
                      )
                    )
                case _ =>
                  error("Requested type T must have an implicit Reader[T] in scope")
              }
          }
        }
    }

    inline def maybe[T: Reader]: Option[T] =
      Try(upickle.default.read[T](item.convertToJson)).toOption

    inline def set(inline key: String, value: AttributeValue): DynamoDbItem =
      collection.mutable.Map.from(item).clone().addOne(key, value)

    /** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html#Expressions.Attributes.NestedElements.DocumentPathExamples
      */
    inline def filterByPaths(paths: Iterable[String]): DynamoDbItem =
      paths.foldLeft(DynamoDbItem())((newItem, path) =>
        item
          .getByPath(path)
          .map(value => newItem.setByPath(path, value))
          .getOrElse(newItem)
      )

    inline def maybe[T](pathSeq: Seq[String]): Option[T] =
      getByPath(pathSeq).flatMap(_.maybe[T])

    inline def getByPath(path: String): Option[AttributeValue] =
      val pathSeq: Array[String] = path.split("[.\\[\\]]").filterNot(_.isBlank())
      getByPath(ArraySeq.unsafeWrapArray(pathSeq))

    inline def getByPath(path: Iterable[String]): Option[AttributeValue] =
      path.headOption.match {
        case Some(key) => item.get(key).flatMap(_.getByPath(path.tail))
        case None      => Some(fromDynamoDbItem(item))
      }

    inline def setByPath(path: String, value: AttributeValue): DynamoDbItem =
      val pathSeq: Array[String] = path.split("[.\\[\\]]").filterNot(_.isBlank())
      setByPath(ArraySeq.unsafeWrapArray(pathSeq), value)

    inline def setByPath(
        path: Iterable[String],
        value: AttributeValue
    ): DynamoDbItem =
      path.headOption.match {
        case None => item
        case Some(key) =>
          item
            .get(key)
            .map(_.setByPath(path.tail, value))
            .map(newValue => item.set(key, newValue))
            .getOrElse(
              item.set(key, createNewAtrributeValue(path.tail, value))
            )
      }

    inline def removeByPath(path: String): DynamoDbItem =
      val pathSeq: Array[String] = path.split("[.\\[\\]]").filterNot(_.isBlank())
      removeByPath(ArraySeq.unsafeWrapArray(pathSeq))

    inline def removeByPath(path: Iterable[String]): DynamoDbItem =
      path.headOption.match {
        case None => item
        case Some(key) =>
          if (path.tail.isEmpty)
          then item.view.filterKeys(_ != key).toMap
          else
            item
              .get(key)
              .map(_.removeByPath(path.tail))
              .map(newValue => item.set(key, newValue))
              .getOrElse(item)
      }

    inline def convertToJson: ujson.Value =
      ujson.Obj.from(item.view.mapValues(_.convertToJson))

    inline def convertToDynamoJson: ujson.Value =
      ujson.Obj(
        "M" -> ujson.Obj.from(
          item.toSeq.sortBy(_._1).map((k, v) => k -> v.convertToDynamoJson)
        )
      )

    inline def removeNullOrEmptyValues: DynamoDbItem =
      item.filterNot((k, v) => v.isNull || v.isBlank || v.isEmpty)

    inline def appendToList(path: String, valueToAdd: AttributeValue) = {
      item
        .getByPath(path)
        .map { value =>
          if (value.isList)
          then
            item.setByPath(
              path,
              AttributeValue
                .builder()
                .l(
                  (value
                    .getList(
                      throw new Exception(
                        "Cannot append to the property of type other than a list"
                      )
                    )
                    ++ valueToAdd
                      .getList(
                        throw new Exception(
                          "Cannot append value of type other than a list"
                        )
                      )).asJava
                )
                .build()
            )
          else
            throw new Exception(
              s"Cannot append to a property $path=$value"
            )
        }
        .getOrElse(
          throw new Exception(
            s"Cannot append to a non-existent property $path"
          )
        )
    }

    inline def prependToList(path: String, valueToAdd: AttributeValue) = {
      item
        .getByPath(path)
        .map { value =>
          if (value.isList)
          then
            item.setByPath(
              path,
              AttributeValue
                .builder()
                .l(
                  (valueToAdd
                    .getList(
                      throw new Exception(
                        "Cannot prepend value of type other than a list"
                      )
                    )
                    ++ value
                      .getList(
                        throw new Exception(
                          "Cannot prepend to the property of type other than a list"
                        )
                      )).asJava
                )
                .build()
            )
          else
            throw new Exception(
              s"Cannot prepend to a property $path=$value"
            )
        }
        .getOrElse(
          throw new Exception(
            s"Cannot prepend to a non-existent property $path"
          )
        )
    }

    inline def maybeTuple[Types <: Tuple](inline keys: String*): Option[Types] =
      Macros.assertSameSize[Types](keys*)
      extractAllOrNone[Types](item, EmptyTuple, keys*).asInstanceOf[Option[Types]]

    inline def maybeClass[T <: Product](using mirror: Mirror.ProductOf[T]): Option[T] = {
      type Keys = mirror.MirroredElemLabels
      type Types = mirror.MirroredElemTypes
      extractAllOrNone2[Types, Keys](item, EmptyTuple)
        .asInstanceOf[Option[mirror.MirroredElemTypes]]
        .map(mirror.fromTuple)
    }

    inline def maybeClass[T <: Product](inline key: String)(using mirror: Mirror.ProductOf[T]): Option[T] =
      item
        .get(key)
        .flatMap(_.maybeNestedItem.flatMap(_.maybeClass[T]))

    inline def eitherTupleOrListMissingKeys[Types <: Tuple](
        inline keys: String*
    ): Either[List[String], Types] =
      Macros.assertSameSize[Types](keys*)
      extractAllOrListMissingKeys[Types](item, EmptyTuple, Nil, keys)
        .asInstanceOf[Either[List[String], Types]]
        .left
        .map(_.reverse)

    inline def eitherClassOrListMissingKeys[T <: Product](using
        mirror: Mirror.ProductOf[T]
    ): Either[List[String], T] = {
      type Keys = mirror.MirroredElemLabels
      type Types = mirror.MirroredElemTypes
      extractAllOrListMissingKeys2[Types, Keys](item, EmptyTuple, Nil)
        .asInstanceOf[Either[List[String], mirror.MirroredElemTypes]]
        .map(mirror.fromTuple)
    }

    inline def getOrError[T: upickle.default.Reader](
        property: String
    )(using error: ErrorContext): Either[error.Error, T] =
      item
        .maybe[T](property)
        .toRight(
          error(
            errorCode = s"DynamoDbItemIsMissing${property.capitalize}PropertyError",
            errorMessage = s"Got item: ${item.convertToDynamoJson.writeAsString}"
          )
        )

    inline def maybeOrError[T: upickle.default.Reader](property: String)(using
        error: ErrorContext
    ): Either[error.Error, Option[T]] =
      Right(item.maybe[T](property))

    inline def getNestedItemOrError(property: String)(using error: ErrorContext): Either[error.Error, DynamoDbItem] =
      item
        .maybeNestedItem(property)
        .toRight(
          error(
            errorCode = s"DynamoDbItemIsMissing${property.capitalize}PropertyError",
            errorMessage = s"Got item: ${item.convertToDynamoJson.writeAsString}"
          )
        )

    inline def maybeNestedItemOrError(property: String)(using
        error: ErrorContext
    ): Either[error.Error, Option[DynamoDbItem]] =
      Right(item.maybeNestedItem(property))

  }

  extension (item: DynamoDbItemUpdate) {
    @targetName("showDynamoDbItemUpdate")
    inline def show: String =
      item.map((k, v) => s"${v.actionAsString()} $k=${v.value().show}").mkString(" ")
  }

  extension [A](buffer: collection.mutable.Buffer[A])
    def putAt(index: Int, value: A, empty: A): collection.mutable.Buffer[A] = {
      try {
        if (index < 0)
        then buffer.append(value)
        else if (index < buffer.size)
        then buffer.update(index, value)
        else
          buffer
            .appendAll(
              (buffer.size until index).map(_ => empty)
            )
            .append(value)
        buffer
      } catch {
        case _: UnsupportedOperationException =>
          val newBuffer: collection.mutable.Buffer[A] =
            collection.mutable.Buffer[A]()
          newBuffer.appendAll(buffer)
          newBuffer.putAt(index, value, empty)
          newBuffer
      }
    }

    def removeAt(index: Int): collection.mutable.Buffer[A] = {
      try {
        if (index >= 0 && index < buffer.size)
        then
          buffer.remove(index)
          buffer
        else buffer
      } catch {
        case _: UnsupportedOperationException =>
          val newBuffer: collection.mutable.Buffer[A] =
            collection.mutable.Buffer[A]()
          newBuffer.appendAll(buffer)
          newBuffer.removeAt(index)
          newBuffer
      }
    }

  private def createNewAtrributeValue(
      path: Iterable[String],
      value: AttributeValue
  ): AttributeValue =
    path.headOption.match {
      case None => value
      case Some(key) =>
        key.toIntOption.match {
          case None =>
            AttributeValue
              .builder()
              .m(Map(key -> createNewAtrributeValue(path.tail, value)).asJava)
              .build()
          case Some(index) =>
            (if (path.size > 1)
               AttributeValue
                 .builder()
                 .l(Seq.empty.asJava)
                 .build()
             else {
               Option(value.n())
                 .map(n => AttributeValue.builder().ns().build())
                 .orElse(
                   Option(value.b())
                     .map(b => AttributeValue.builder().bs().build())
                 )
                 .orElse(
                   Option(value.s())
                     .map(s => AttributeValue.builder().ss().build())
                 )
                 .getOrElse(
                   AttributeValue.builder().l(Seq.empty.asJava).build()
                 )
             }).setByIndex(index, createNewAtrributeValue(path.tail, value))
        }
    }

  object DocumentPath {

    type Path = Seq[Part]

    sealed trait Part {
      def toPath: String
    }
    final case class Attribute(name: String) extends Part {
      def toPath: String = s".$name"
    }
    final case class Index(index: Int) extends Part {
      def toPath: String = s"[$index]"
    }

    inline def isNestedPath(path: String): Boolean =
      path.contains('.') || path.contains('[')

    inline def parse(path: String): Seq[Part] =
      parsePath(Buffer.empty, path).toSeq

    private def parsePath(
        path: Buffer[Part],
        remaining: String
    ): Buffer[Part] = {

      def parseDot(nextDot: Int) = {
        if (nextDot >= 0)
          if (nextDot == 0) then
            parsePath(
              path,
              remaining.drop(1)
            )
          else
            parsePath(
              path.append(Attribute(remaining.take(nextDot))),
              remaining.drop(nextDot + 1)
            )
        else path.append(Attribute(remaining))
      }

      def parseBrace(nextBrace: Int) = {
        if (nextBrace == 0) then
          val endBrace = remaining.indexOf("]")
          if (endBrace >= 0) then
            val index: Int =
              remaining
                .substring(1, endBrace)
                .toIntOption
                .getOrElse(
                  throw new Exception(
                    s"Index value must be an integer in $remaining"
                  )
                )
            parsePath(
              path.append(Index(index)),
              remaining.drop(endBrace + 1)
            )
          else
            throw new Exception(
              s"Missing matching ending brace in $remaining"
            )
        else
          parsePath(
            path.append(Attribute(remaining.take(nextBrace))),
            remaining.drop(nextBrace)
          )
      }

      if (remaining.isEmpty()) then path
      else {
        val nextDot = remaining.indexOf(".")
        val nextBrace = remaining.indexOf("[")
        if (nextBrace < 0) then parseDot(nextDot)
        else if (nextDot < 0) then parseBrace(nextBrace)
        else if (nextDot < nextBrace) parseDot(nextDot)
        else parseBrace(nextBrace)
      }
    }

    extension (path: Seq[Part])
      inline def toPath: String =
        path.map(_.toPath).mkString.dropWhile(_ == '.')

      inline def toValueReference: String =
        path
          .map {
            case Attribute(name) => name
            case Index(index)    => index.toString()
          }
          .mkString(":", "_", "")

      inline def toNameReference: String =
        path
          .foldLeft(StringBuilder("")) { case (reference, part) =>
            reference.append(part.match {
              case Attribute(name) => s".#$name"
              case Index(index)    => s"[$index]"
            })
          }
          .toString()
          .dropWhile(_ == '.')

      inline def toAttributeNamesMap: Map[String, String] =
        path
          .foldLeft(collection.mutable.Map.empty[String, String]) { case (map, part) =>
            part.match {
              case Attribute(name) =>
                map.update(s"#$name", name)
                map
              case Index(index) => map
            }

          }
          .toMap

      inline def resolveNameReferences(
          attributeNames: collection.Map[String, String]
      ): DocumentPath.Path =
        path.map {
          case Attribute(name) =>
            Attribute(attributeNames.get(name).getOrElse(name))
          case index => index
        }
  }

  extension (string: String)
    def readPattern(pattern: Pattern): Either[String, (String, String)] =
      val matcher = pattern.matcher(string)
      if (matcher.find() && matcher.start() == 0)
      then Right(string.splitAt(matcher.end()))
      else Left(string)

    def readAnyPattern(
        patterns: Pattern*
    ): Either[String, (String, String)] =
      patterns.foldLeft[Either[String, (String, String)]](Left(string)) { (result, pattern) =>
        result match {
          case Left(_) => readPattern(pattern)
          case result  => result
        }
      }

  object AttributeUpdate {

    private val SPACE = "(?:\\s?)"
    private val PATH = "(\\#?[a-zA-Z0-9#._\\[\\]]+)"
    private val OPERAND = "(\\:[a-zA-Z0-9_]+)"
    private val TYPE = "(S|SS|N|NS|B|BS|BOOL|NULL|M|L)"

    val spacePattern = Pattern.compile(SPACE)
    val pathPattern = Pattern.compile(PATH)
    val operandPattern = Pattern.compile(OPERAND)
    val equalSignPattern = Pattern.compile(s"$SPACE=$SPACE")
    val commaSignPattern = Pattern.compile(s"$SPACE,$SPACE")

    val listAppendSyntax: Regex =
      s"list_append\\($SPACE$PATH$SPACE,$SPACE$OPERAND$SPACE\\)".r

    val listPrependSyntax: Regex =
      s"list_append\\($SPACE$OPERAND$SPACE,$SPACE$PATH$SPACE\\)".r

    val ifNotExistsSyntax: Regex =
      s"if_not_exists\\($SPACE$PATH$SPACE,$SPACE$OPERAND$SPACE\\)".r

    val expressionGroupOf: AttributeValueUpdate => String =
      _.actionAsString().match {
        case "PUT"               => "SET"
        case "DELETE"            => "REMOVE"
        case "ADD"               => "ADD"
        case "SET:list_append"   => "SET"
        case "SET:list_prepend"  => "SET"
        case "SET:if_not_exists" => "SET"
        case "REMOVE:index"      => "REMOVE"
        case _                   => ""
      }

    def createUpdateExpression(
        update: DynamoDbItemUpdate
    ): (String, Map[String, String], Map[String, AttributeValue]) = {

      val pathsMap: Map[String, DocumentPath.Path] =
        update.keys.map(key => (key, DocumentPath.parse(key))).toMap

      val expression =
        update
          .groupMap((name, update) => expressionGroupOf(update)) { (name, update) =>
            val nameRef = pathsMap(name).toNameReference
            val valueRef = pathsMap(name).toValueReference
            update.actionAsString() match {
              case "PUT"    => s"$nameRef = $valueRef"
              case "DELETE" => s"$nameRef"
              case "ADD"    => s"$nameRef $valueRef"
              case "SET:list_append" =>
                s"$nameRef = list_append($nameRef, $valueRef)"
              case "SET:list_prepend" =>
                s"$nameRef = list_append($valueRef, $nameRef)"
              case "SET:if_not_exists" =>
                s"$nameRef = if_not_exists($nameRef, $valueRef)"
              case "REMOVE:index" =>
                s"$nameRef[${update.value().getInt(0)}]"
              case _ => ""
            }
          }
          .toSeq
          .sortBy(_._1.toString())
          .map((action, attributes) => s"$action ${attributes.mkString(", ")}")
          .mkString(" ")
      val names: Map[String, String] =
        update.flatMap { (name, _) =>
          pathsMap(name).toAttributeNamesMap
        }.toMap
      val values: Map[String, AttributeValue] =
        update.flatMap { (name, update) =>
          val valueRef = pathsMap(name).toValueReference
          update.actionAsString().match {
            case "PUT"               => Map(valueRef -> update.value())
            case "DELETE"            => Map.empty
            case "ADD"               => Map(valueRef -> update.value())
            case "SET:list_append"   => Map(valueRef -> update.value())
            case "SET:list_prepend"  => Map(valueRef -> update.value())
            case "SET:if_not_exists" => Map(valueRef -> update.value())
            case "REMOVE:index"      => Map.empty
            case _                   => Map.empty
          }
        }.toMap
      (expression, names, values)
    }

    def parseUpdateExpression(
        updateExpression: String,
        attributeNames: collection.Map[String, String],
        attributeValues: collection.Map[String, AttributeValue]
    ): Map[String, AttributeValueUpdate] = {

      def parseSetInput(
          input: String,
          result: Map[String, AttributeValueUpdate]
      ): Map[String, AttributeValueUpdate] =
        if (input.isBlank()) then result
        else {
          input
            .trim()
            .readPattern(pathPattern)
            .fold(
              notFound =>
                throw new Exception(
                  s"Invalid SET expression, must start with name or path reference at '$notFound'"
                ),
              (nameRef, tail) => {
                tail
                  .readPattern(equalSignPattern)
                  .fold(
                    notFound =>
                      throw new Exception(
                        s"Invalid SET expression, must start with '=' at '$notFound'"
                      ),
                    (_, tail) =>
                      tail
                        .trim()
                        .readAnyPattern(
                          operandPattern,
                          listAppendSyntax.pattern,
                          listPrependSyntax.pattern,
                          ifNotExistsSyntax.pattern
                        )
                        .fold(
                          notFound =>
                            throw new Exception(
                              s"Invalid SET expression, must start with value reference or function at '$notFound'"
                            ),
                          (valueRef, tail) => {
                            val path: String =
                              DocumentPath
                                .parse(nameRef)
                                .resolveNameReferences(attributeNames)
                                .toPath
                            val attributeUpdate: AttributeValueUpdate =
                              parseAttributeValueUpdate(
                                valueRef,
                                attributeNames,
                                attributeValues
                              )
                            tail
                              .trim()
                              .readPattern(commaSignPattern)
                              .fold(
                                notFound => result.updated(path, attributeUpdate),
                                (_, tail) =>
                                  parseSetInput(
                                    tail,
                                    result.updated(path, attributeUpdate)
                                  )
                              )
                          }
                        )
                  )
              }
            )
        }

      def parseAttributeValueUpdate(
          expression: String,
          attributeNames: collection.Map[String, String],
          attributeValues: collection.Map[String, AttributeValue]
      ): AttributeValueUpdate = {
        expression
          .match {
            case listAppendSyntax(nameRef, valueRef) =>
              for {
                attributeValue <- attributeValues.get(valueRef)
                attributeName = attributeNames.get(nameRef).getOrElse(nameRef)
              } yield AttributeValueUpdate
                .builder()
                .action("SET:list_append")
                .value(attributeValue)
                .build()

            case listPrependSyntax(valueRef, nameRef) =>
              for {
                attributeValue <- attributeValues.get(valueRef)
                attributeName = attributeNames.get(nameRef).getOrElse(nameRef)
              } yield AttributeValueUpdate
                .builder()
                .action("SET:list_prepend")
                .value(attributeValue)
                .build()

            case ifNotExistsSyntax(nameRef, valueRef) =>
              for {
                attributeValue <- attributeValues.get(valueRef)
                attributeName = attributeNames.get(nameRef).getOrElse(nameRef)
              } yield AttributeValueUpdate
                .builder()
                .action("SET:if_not_exists")
                .value(attributeValue)
                .build()

            case _ =>
              attributeValues
                .get(expression.trim())
                .map { attributeValue =>
                  AttributeValueUpdate
                    .builder()
                    .action("PUT")
                    .value(attributeValue)
                    .build()
                }
          }
          .getOrElse(
            throw new Exception(
              s"Cannot parse attribute value update expression '$expression'"
            )
          )
      }

      def parseAddInput(input: String) =
        input
          .replace(",", " ")
          .split("\\s+")
          .sliding(2, 2)
          .map { s =>
            val path: String =
              DocumentPath
                .parse(s(0))
                .resolveNameReferences(attributeNames)
                .toPath
            val key = s(1)
            (
              path,
              AttributeValueUpdate
                .builder()
                .action(AttributeAction.ADD)
                .value(
                  attributeValues
                    .get(key)
                    .getOrElse(
                      throw new Exception(
                        s"Mising update expression's key: $key"
                      )
                    )
                )
                .build()
            )
          }

      def parseRemoveInput(input: String) =
        input
          .replace(",", " ")
          .split("\\s+")
          .toSeq
          .map { path =>
            (
              DocumentPath
                .parse(path)
                .resolveNameReferences(attributeNames)
                .toPath,
              AttributeValueUpdate
                .builder()
                .action(AttributeAction.DELETE)
                .build()
            )
          }

      splitByActions(updateExpression)
        .flatMap {
          case ("SET", input)    => parseSetInput(input, Map.empty)
          case ("ADD", input)    => parseAddInput(input).toMap
          case ("REMOVE", input) => parseRemoveInput(input).toMap
          case ("DELETE", input) => Map.empty
          case _                 => Map.empty
        }
    }

    private def splitByActions(expression: String): Map[String, String] = {
      Seq("SET", "ADD", "DELETE", "REMOVE")
        .map(c => (c, expression.indexOf(c)))
        .filter(_._2 >= 0)
        .sortBy(_._2)
        .appended(("END", expression.size))
        .sliding(2)
        .foldLeft(Map.empty[String, String]) { (acc, pair) =>
          val start = pair(0)
          val end = pair(1)
          acc.updated(
            start._1,
            expression.substring(
              start._2 + start._1.size + 1,
              end._2
            )
          )
        }
    }
  }

  object ReadWriters {

    given ReadWriter[AttributeValue] =
      readwriter[ujson.Value]
        .bimap(
          _.convertToJson,
          fromValue
        )

    given ReadWriter[collection.Map[String, AttributeValue]] =
      readwriter[Map[String, ujson.Value]]
        .bimap(
          _.view.mapValues(_.convertToJson).toMap,
          _.view.mapValues(fromValue).toMap
        )

    given ReadWriter[ProjectionType] =
      readwriter[String]
        .bimap(
          _.toString(),
          ProjectionType.fromValue
        )

  }

  val dynamoDbReservedWords = Set(
    "ABORT",
    "ABSOLUTE",
    "ACTION",
    "ADD",
    "AFTER",
    "AGENT",
    "AGGREGATE",
    "ALL",
    "ALLOCATE",
    "ALTER",
    "ANALYZE",
    "AND",
    "ANY",
    "ARCHIVE",
    "ARE",
    "ARRAY",
    "AS",
    "ASC",
    "ASCII",
    "ASENSITIVE",
    "ASSERTION",
    "ASYMMETRIC",
    "AT",
    "ATOMIC",
    "ATTACH",
    "ATTRIBUTE",
    "AUTH",
    "AUTHORIZATION",
    "AUTHORIZE",
    "AUTO",
    "AVG",
    "BACK",
    "BACKUP",
    "BASE",
    "BATCH",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "BLOCK",
    "BOOLEAN",
    "BOTH",
    "BREADTH",
    "BUCKET",
    "BULK",
    "BY",
    "BYTE",
    "CALL",
    "CALLED",
    "CALLING",
    "CAPACITY",
    "CASCADE",
    "CASCADED",
    "CASE",
    "CAST",
    "CATALOG",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "CLASS",
    "CLOB",
    "CLOSE",
    "CLUSTER",
    "CLUSTERED",
    "CLUSTERING",
    "CLUSTERS",
    "COALESCE",
    "COLLATE",
    "COLLATION",
    "COLLECTION",
    "COLUMN",
    "COLUMNS",
    "COMBINE",
    "COMMENT",
    "COMMIT",
    "COMPACT",
    "COMPILE",
    "COMPRESS",
    "CONDITION",
    "CONFLICT",
    "CONNECT",
    "CONNECTION",
    "CONSISTENCY",
    "CONSISTENT",
    "CONSTRAINT",
    "CONSTRAINTS",
    "CONSTRUCTOR",
    "CONSUMED",
    "CONTINUE",
    "CONVERT",
    "COPY",
    "CORRESPONDING",
    "COUNT",
    "COUNTER",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURSOR",
    "CYCLE",
    "DATA",
    "DATABASE",
    "DATE",
    "DATETIME",
    "DAY",
    "DEALLOCATE",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DEFINE",
    "DEFINED",
    "DEFINITION",
    "DELETE",
    "DELIMITED",
    "DEPTH",
    "DEREF",
    "DESC",
    "DESCRIBE",
    "DESCRIPTOR",
    "DETACH",
    "DETERMINISTIC",
    "DIAGNOSTICS",
    "DIRECTORIES",
    "DISABLE",
    "DISCONNECT",
    "DISTINCT",
    "DISTRIBUTE",
    "DO",
    "DOMAIN",
    "DOUBLE",
    "DROP",
    "DUMP",
    "DURATION",
    "DYNAMIC",
    "EACH",
    "ELEMENT",
    "ELSE",
    "ELSEIF",
    "EMPTY",
    "ENABLE",
    "END",
    "EQUAL",
    "EQUALS",
    "ERROR",
    "ESCAPE",
    "ESCAPED",
    "EVAL",
    "EVALUATE",
    "EXCEEDED",
    "EXCEPT",
    "EXCEPTION",
    "EXCEPTIONS",
    "EXCLUSIVE",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "EXPLODE",
    "EXPORT",
    "EXPRESSION",
    "EXTENDED",
    "EXTERNAL",
    "EXTRACT",
    "FAIL",
    "FALSE",
    "FAMILY",
    "FETCH",
    "FIELDS",
    "FILE",
    "FILTER",
    "FILTERING",
    "FINAL",
    "FINISH",
    "FIRST",
    "FIXED",
    "FLATTERN",
    "FLOAT",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FORMAT",
    "FORWARD",
    "FOUND",
    "FREE",
    "FROM",
    "FULL",
    "FUNCTION",
    "FUNCTIONS",
    "GENERAL",
    "GENERATE",
    "GET",
    "GLOB",
    "GLOBAL",
    "GO",
    "GOTO",
    "GRANT",
    "GREATER",
    "GROUP",
    "GROUPING",
    "HANDLER",
    "HASH",
    "HAVE",
    "HAVING",
    "HEAP",
    "HIDDEN",
    "HOLD",
    "HOUR",
    "IDENTIFIED",
    "IDENTITY",
    "IF",
    "IGNORE",
    "IMMEDIATE",
    "IMPORT",
    "IN",
    "INCLUDING",
    "INCLUSIVE",
    "INCREMENT",
    "INCREMENTAL",
    "INDEX",
    "INDEXED",
    "INDEXES",
    "INDICATOR",
    "INFINITE",
    "INITIALLY",
    "INLINE",
    "INNER",
    "INNTER",
    "INOUT",
    "INPUT",
    "INSENSITIVE",
    "INSERT",
    "INSTEAD",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "INVALIDATE",
    "IS",
    "ISOLATION",
    "ITEM",
    "ITEMS",
    "ITERATE",
    "JOIN",
    "KEY",
    "KEYS",
    "LAG",
    "LANGUAGE",
    "LARGE",
    "LAST",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEAVE",
    "LEFT",
    "LENGTH",
    "LESS",
    "LEVEL",
    "LIKE",
    "LIMIT",
    "LIMITED",
    "LINES",
    "LIST",
    "LOAD",
    "LOCAL",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCATION",
    "LOCATOR",
    "LOCK",
    "LOCKS",
    "LOG",
    "LOGED",
    "LONG",
    "LOOP",
    "LOWER",
    "MAP",
    "MATCH",
    "MATERIALIZED",
    "MAX",
    "MAXLEN",
    "MEMBER",
    "MERGE",
    "METHOD",
    "METRICS",
    "MIN",
    "MINUS",
    "MINUTE",
    "MISSING",
    "MOD",
    "MODE",
    "MODIFIES",
    "MODIFY",
    "MODULE",
    "MONTH",
    "MULTI",
    "MULTISET",
    "NAME",
    "NAMES",
    "NATIONAL",
    "NATURAL",
    "NCHAR",
    "NCLOB",
    "NEW",
    "NEXT",
    "NO",
    "NONE",
    "NOT",
    "NULL",
    "NULLIF",
    "NUMBER",
    "NUMERIC",
    "OBJECT",
    "OF",
    "OFFLINE",
    "OFFSET",
    "OLD",
    "ON",
    "ONLINE",
    "ONLY",
    "OPAQUE",
    "OPEN",
    "OPERATOR",
    "OPTION",
    "OR",
    "ORDER",
    "ORDINALITY",
    "OTHER",
    "OTHERS",
    "OUT",
    "OUTER",
    "OUTPUT",
    "OVER",
    "OVERLAPS",
    "OVERRIDE",
    "OWNER",
    "PAD",
    "PARALLEL",
    "PARAMETER",
    "PARAMETERS",
    "PARTIAL",
    "PARTITION",
    "PARTITIONED",
    "PARTITIONS",
    "PATH",
    "PERCENT",
    "PERCENTILE",
    "PERMISSION",
    "PERMISSIONS",
    "PIPE",
    "PIPELINED",
    "PLAN",
    "POOL",
    "POSITION",
    "PRECISION",
    "PREPARE",
    "PRESERVE",
    "PRIMARY",
    "PRIOR",
    "PRIVATE",
    "PRIVILEGES",
    "PROCEDURE",
    "PROCESSED",
    "PROJECT",
    "PROJECTION",
    "PROPERTY",
    "PROVISIONING",
    "PUBLIC",
    "PUT",
    "QUERY",
    "QUIT",
    "QUORUM",
    "RAISE",
    "RANDOM",
    "RANGE",
    "RANK",
    "RAW",
    "READ",
    "READS",
    "REAL",
    "REBUILD",
    "RECORD",
    "RECURSIVE",
    "REDUCE",
    "REF",
    "REFERENCE",
    "REFERENCES",
    "REFERENCING",
    "REGEXP",
    "REGION",
    "REINDEX",
    "RELATIVE",
    "RELEASE",
    "REMAINDER",
    "RENAME",
    "REPEAT",
    "REPLACE",
    "REQUEST",
    "RESET",
    "RESIGNAL",
    "RESOURCE",
    "RESPONSE",
    "RESTORE",
    "RESTRICT",
    "RESULT",
    "RETURN",
    "RETURNING",
    "RETURNS",
    "REVERSE",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLES",
    "ROLLBACK",
    "ROLLUP",
    "ROUTINE",
    "ROW",
    "ROWS",
    "RULE",
    "RULES",
    "SAMPLE",
    "SATISFIES",
    "SAVE",
    "SAVEPOINT",
    "SCAN",
    "SCHEMA",
    "SCOPE",
    "SCROLL",
    "SEARCH",
    "SECOND",
    "SECTION",
    "SEGMENT",
    "SEGMENTS",
    "SELECT",
    "SELF",
    "SEMI",
    "SENSITIVE",
    "SEPARATE",
    "SEQUENCE",
    "SERIALIZABLE",
    "SESSION",
    "SET",
    "SETS",
    "SHARD",
    "SHARE",
    "SHARED",
    "SHORT",
    "SHOW",
    "SIGNAL",
    "SIMILAR",
    "SIZE",
    "SKEWED",
    "SMALLINT",
    "SNAPSHOT",
    "SOME",
    "SOURCE",
    "SPACE",
    "SPACES",
    "SPARSE",
    "SPECIFIC",
    "SPECIFICTYPE",
    "SPLIT",
    "SQL",
    "SQLCODE",
    "SQLERROR",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "START",
    "STATE",
    "STATIC",
    "STATUS",
    "STORAGE",
    "STORE",
    "STORED",
    "STREAM",
    "STRING",
    "STRUCT",
    "STYLE",
    "SUB",
    "SUBMULTISET",
    "SUBPARTITION",
    "SUBSTRING",
    "SUBTYPE",
    "SUM",
    "SUPER",
    "SYMMETRIC",
    "SYNONYM",
    "SYSTEM",
    "TABLE",
    "TABLESAMPLE",
    "TEMP",
    "TEMPORARY",
    "TERMINATED",
    "TEXT",
    "THAN",
    "THEN",
    "THROUGHPUT",
    "TIME",
    "TIMESTAMP",
    "TIMEZONE",
    "TINYINT",
    "TO",
    "TOKEN",
    "TOTAL",
    "TOUCH",
    "TRAILING",
    "TRANSACTION",
    "TRANSFORM",
    "TRANSLATE",
    "TRANSLATION",
    "TREAT",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TTL",
    "TUPLE",
    "TYPE",
    "UNDER",
    "UNDO",
    "UNION",
    "UNIQUE",
    "UNIT",
    "UNKNOWN",
    "UNLOGGED",
    "UNNEST",
    "UNPROCESSED",
    "UNSIGNED",
    "UNTIL",
    "UPDATE",
    "UPPER",
    "URL",
    "USAGE",
    "USE",
    "USER",
    "USERS",
    "USING",
    "UUID",
    "VACUUM",
    "VALUE",
    "VALUED",
    "VALUES",
    "VARCHAR",
    "VARIABLE",
    "VARIANCE",
    "VARINT",
    "VARYING",
    "VIEW",
    "VIEWS",
    "VIRTUAL",
    "VOID",
    "WAIT",
    "WHEN",
    "WHENEVER",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WITHIN",
    "WITHOUT",
    "WORK",
    "WRAPPED",
    "WRITE",
    "YEAR",
    "ZONE"
  )

  private inline def optionally[R](f: => R): Option[R] =
    try (Some(f))
    catch { case _: Exception => None }

  private transparent inline def extractAllOrNone[Types <: Tuple](
      item: DynamoDbItem,
      inline result: Tuple,
      inline keys: String*
  ): Option[Tuple] =
    inline erasedValue[Types] match {
      case _: EmptyTuple => Some(result)
      case _: (h *: t) =>
        keys.headOption match {
          case Some(key) =>
            inline erasedValue[h] match {
              case _: Option[j] =>
                extractAllOrNone[t](item, result :* item.maybe[j](key), keys.drop(1)*)
              case _ =>
                item.maybe[h](key) match {
                  case None        => None
                  case Some(value) => extractAllOrNone[t](item, result :* value, keys.drop(1)*)
                }
            }
          case _ => None
        }
    }

  private transparent inline def extractAllOrListMissingKeys[Types <: Tuple](
      item: DynamoDbItem,
      inline result: Tuple,
      inline missing: List[String],
      inline keys: Seq[String]
  ): Either[List[String], Tuple] =
    inline erasedValue[Types] match {
      case _: EmptyTuple => if (missing.isEmpty) Right(result) else Left(missing)
      case _: (h *: t) =>
        keys.headOption match {
          case Some(key) =>
            inline erasedValue[h] match {
              case _: Option[j] =>
                extractAllOrListMissingKeys[t](item, result :* item.maybe[j](key), missing, keys.drop(1))
              case _ =>
                item.maybe[h](key) match {
                  case None =>
                    extractAllOrListMissingKeys[t](item, result, key :: missing, keys.drop(1))
                  case Some(value) =>
                    extractAllOrListMissingKeys[t](item, result :* value, missing, keys.drop(1))
                }

            }
          case _ => if (missing.isEmpty) Right(result) else Left(missing)
        }
    }

  private transparent inline def extractAllOrNone2[Types <: Tuple, Keys <: Tuple](
      item: DynamoDbItem,
      inline result: Tuple
  ): Option[Tuple] =
    inline erasedValue[Types] match {
      case _: EmptyTuple => Some(result)
      case _: (h *: ts) =>
        inline erasedValue[Keys] match {
          case _: EmptyTuple => None
          case _: (k *: ks) =>
            val key: String = constValue[k].asInstanceOf[String]
            inline erasedValue[h] match {
              case _: Option[j] => extractAllOrNone2[ts, ks](item, result :* item.maybe[j](key))
              case _ =>
                item.maybe[h](key) match {
                  case None        => None
                  case Some(value) => extractAllOrNone2[ts, ks](item, result :* value)
                }
            }

        }
    }

  private transparent inline def extractAllOrListMissingKeys2[Types <: Tuple, Keys <: Tuple](
      item: DynamoDbItem,
      inline result: Tuple,
      inline missing: List[String]
  ): Either[List[String], Tuple] =
    inline erasedValue[Types] match {
      case _: EmptyTuple => if (missing.isEmpty) Right(result) else Left(missing)
      case _: (h *: ts) =>
        inline erasedValue[Keys] match {
          case _: EmptyTuple =>
            if (missing.isEmpty) Right(result) else Left(missing)

          case _: (k *: ks) =>
            val key: String = constValue[k].asInstanceOf[String]
            inline erasedValue[h] match {
              case _: Option[j] =>
                extractAllOrListMissingKeys2[ts, ks](item, result :* item.maybe[j](key), missing)
              case _ =>
                item.maybe[h](key) match {
                  case None =>
                    extractAllOrListMissingKeys2[ts, ks](item, result, key :: missing)
                  case Some(value) =>
                    extractAllOrListMissingKeys2[ts, ks](item, result :* value, missing)
                }

            }

        }
    }

  inline transparent def maybeExtractValue[V](name: String)[T <: Product](entity: T)(using
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Either[error.Error, V] =
    type Keys = mirror.MirroredElemLabels
    type Types = mirror.MirroredElemTypes
    getProperty[Types, Keys, T, V](entity, name, 0)
      .map(_.asInstanceOf[V])
      .toRight(
        error(
          errorCode = "PropertyNotFoundOnEntity",
          errorMessage = s"Property $name has not been found on the entity of class ${entity.getClass.getName}"
        )
      )

  private inline transparent def getProperty[Types <: Tuple, Keys <: Tuple, T <: Product, V](
      entity: T,
      name: String,
      index: Int
  ): Option[V] =
    inline erasedValue[Types] match {
      case _: EmptyTuple => None
      case _: (t *: ts) =>
        inline erasedValue[Keys] match {
          case _: EmptyTuple => None
          case _: (k *: ks) =>
            val key: String = constValue[k].asInstanceOf[String]
            if (key == name)
            then
              inline erasedValue[t] match {
                case _: V => Some(entity.productElement(index).asInstanceOf[V])
                case _ =>
                  None
              }
            else getProperty[ts, ks, T, V](entity, name, index + 1)
        }
    }

  inline def translateProductToDynamoDbItemUpdate[T <: Product](entity: T, removeUndefinedProperties: Boolean)(using
      mirror: Mirror.ProductOf[T]
  ): collection.mutable.Map[String, AttributeValueUpdate] =
    type Keys = mirror.MirroredElemLabels
    type Types = mirror.MirroredElemTypes
    translatePropertiesToAttributeUpdates[Types, Keys, T](
      entity,
      0,
      removeUndefinedProperties,
      collection.mutable.Map.empty[String, AttributeValueUpdate]
    )

  private inline transparent def translatePropertiesToAttributeUpdates[Types <: Tuple, Keys <: Tuple, T <: Product](
      entity: T,
      index: Int,
      removeUndefinedProperties: Boolean,
      result: collection.mutable.Map[String, AttributeValueUpdate]
  ): collection.mutable.Map[String, AttributeValueUpdate] =
    inline erasedValue[Types] match {
      case _: EmptyTuple => result
      case _: (t *: ts) =>
        inline erasedValue[Keys] match {
          case _: EmptyTuple => result
          case _: (k *: ks) =>
            val key: String = constValue[k].asInstanceOf[String]
            val value = entity.productElement(index)
            val attributeUpdate = toAttributeValue[t](value.asInstanceOf[t])
            if (removeUndefinedProperties || attributeUpdate != REMOVE)
            then result.update(key, toUpdate(attributeUpdate))
            translatePropertiesToAttributeUpdates[ts, ks, T](entity, index + 1, removeUndefinedProperties, result)
        }
    }

  inline def toAttributeValue[T](value: T): AttributeValue =
    inline erasedValue[T] match {
      case _: Option[j] =>
        val opt = value.asInstanceOf[Option[j]]
        opt.map(v => toAttributeValue[j](v.asInstanceOf[j])).getOrElse(REMOVE)
      case _: AttributeValue => value.asInstanceOf[AttributeValue]
      case _: String         => fromString(value.asInstanceOf[String])
      case _: Int            => fromInt(value.asInstanceOf[Int])
      case _: Long           => fromLong(value.asInstanceOf[Long])
      case _: Short          => fromInt(value.asInstanceOf[Short])
      case _: Byte           => fromInt(value.asInstanceOf[Byte])
      case _: Double         => fromDouble(value.asInstanceOf[Double])
      case _: Float          => fromDouble(value.asInstanceOf[Float])
      case _: Boolean        => fromBoolean(value.asInstanceOf[Boolean])
      case _: BigDecimal     => fromBigDecimal(value.asInstanceOf[BigDecimal])
      case _: Array[Byte] =>
        AttributeValue
          .builder()
          .b(SdkBytes.fromByteArrayUnsafe(value.asInstanceOf[Array[Byte]]))
          .build()
      case _: DynamoDbItem => fromDynamoDbItem(value.asInstanceOf[DynamoDbItem])
      case _: Map[String, j] =>
        AttributeValue
          .builder()
          .m(value.asInstanceOf[Map[String, j]].view.mapValues(v => toAttributeValue[j](v)).toMap.asJava)
          .build()
      case _: Set[String]     => fromIterableOfString(value.asInstanceOf[Iterable[String]])
      case _: Set[Int]        => fromIterableOfInt(value.asInstanceOf[Iterable[Int]])
      case _: Set[Long]       => fromIterableOfLong(value.asInstanceOf[Iterable[Long]])
      case _: Set[Float]      => fromIterableOfFloat(value.asInstanceOf[Iterable[Float]])
      case _: Set[Double]     => fromIterableOfDouble(value.asInstanceOf[Iterable[Double]])
      case _: Set[BigDecimal] => fromIterableOfBigDecimal(value.asInstanceOf[Iterable[BigDecimal]])
      case _: Iterable[j] =>
        AttributeValue
          .builder()
          .l(value.asInstanceOf[Iterable[j]].map(v => toAttributeValue[j](v)).toSeq.asJava)
          .build()
      case _ =>
        summonFrom {
          case conversion: Conversion[T, AttributeValue] =>
            conversion.apply(value.asInstanceOf[T])
          case _ =>
            fromString(value.toString())
        }
    }
}
