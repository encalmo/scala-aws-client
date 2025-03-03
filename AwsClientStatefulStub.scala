package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.CompareFunction
import org.encalmo.aws.AwsDynamoDbApi.Condition
import org.encalmo.aws.AwsDynamoDbApi.DynamoDbItem
import org.encalmo.aws.AwsDynamoDbApi.DynamoDbItemKey
import org.encalmo.aws.AwsDynamoDbApi.FilterCondition
import org.encalmo.aws.AwsDynamoDbApi.KeyCondition
import org.encalmo.aws.AwsDynamoDbApi.appendToList
import org.encalmo.aws.AwsDynamoDbApi.filterByPaths
import org.encalmo.aws.AwsDynamoDbApi.getByPath
import org.encalmo.aws.AwsDynamoDbApi.getList
import org.encalmo.aws.AwsDynamoDbApi.isList
import org.encalmo.aws.AwsDynamoDbApi.isNumber
import org.encalmo.aws.AwsDynamoDbApi.maybeDouble
import org.encalmo.aws.AwsDynamoDbApi.prependToList
import org.encalmo.aws.AwsDynamoDbApi.removeByPath
import org.encalmo.aws.AwsDynamoDbApi.setByPath
import org.encalmo.aws.AwsDynamoDbApi.maybeList
import org.encalmo.aws.AwsDynamoDbApi.maybeInt
import org.encalmo.aws.AwsDynamoDbApi.isSameAs

import software.amazon.awssdk.core.internal.waiters.DefaultWaiterResponse
import software.amazon.awssdk.core.waiters.WaiterResponse
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter
import software.amazon.awssdk.services.iam.IamClient
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.*
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sts.StsClient
import upickle.default.ReadWriter

import scala.collection.mutable.Buffer
import scala.io.AnsiColor
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.lambda.LambdaClient
import software.amazon.awssdk.services.lambda.model.InvokeRequest
import software.amazon.awssdk.services.lambda.model.InvokeResponse
import software.amazon.awssdk.core.SdkBytes
import java.nio.charset.StandardCharsets
import software.amazon.awssdk.services.lambda.model.InvocationType
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse
import software.amazon.awssdk.services.apigatewayv2.ApiGatewayV2Client
import software.amazon.awssdk.services.apigateway.ApiGatewayClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import software.amazon.awssdk.services.sqs.model.SendMessageResponse
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import software.amazon.awssdk.awscore.DefaultAwsResponseMetadata

/** Support for stateless testing of AWS Client interfaces. Work-in-progres. Currently suported are:
  *   - DynamoDB: Get, Put, Delete and Update Item.
  */
class AwsClientStatefulStub extends AwsClient {

  final override def isDebugMode: Boolean =
    Option(System.getenv("AWS_CLIENT_DEBUG_MODE")).getOrElse("ON").contains("ON")

  override lazy val dynamoDb: InMemoryDynamoDb = new InMemoryDynamoDb()
  override lazy val iam: IamClient = ???
  override lazy val sqs: SqsClientStub = new SqsClientStub()
  override lazy val sts: StsClientStub = new StsClientStub()
  override lazy val secrets: InMemorySecretsManager = new InMemorySecretsManager()
  override lazy val kms: KmsClient = ???
  override lazy val s3: S3Client = ???
  override lazy val lambda: LambdaClientStub = new LambdaClientStub()
  override lazy val apigateway: ApiGatewayClient = ???
  override lazy val apigatewayv2: ApiGatewayV2Client = ???

  override def currentRegion: Region =
    Region.of(Option(System.getenv("AWS_DEFAULT_REGION")).getOrElse("us-east-1"))

  final def setupSecrets(newSecrets: Map[String, String]): Unit =
    secrets.setup(newSecrets)

}

object AwsClientStatefulStub {}

final class InMemoryDynamoDb extends DynamoDbClient {

  import InMemoryDynamoDb.*

  override def close(): Unit = ()
  override def serviceName(): String = "dynamodb"

  val tables: collection.mutable.Map[String, InMemoryDynamoDbTable] =
    collection.mutable.Map.empty

  private lazy val snapshots: collection.mutable.Map[String, String] =
    collection.mutable.Map.empty

  final def setup(newTables: Map[String, InMemoryDynamoDbTableDump]): Unit =
    newTables.foreach(t =>
      tables.get(t._1).match {
        case None =>
          tables.update(t._1, InMemoryDynamoDbTable.from(t._2))
        case Some(table) =>
          t._2.items.foreach((k, v) => table.update(k, v))
      }
    )

  final def dump(): Map[String, InMemoryDynamoDbTableDump] =
    tables.view.mapValues(_.dump()).toMap

  final inline def pickle(): String =
    upickle.default.write(dump(), indent = 2, sortKeys = true)

  final inline def unpickle(pickles: String): Unit =
    setup(
      upickle.default
        .read[Map[String, InMemoryDynamoDb.InMemoryDynamoDbTableDump]](pickles)
    )

  final def makeSnapshot(tag: String): String =
    val snapshot = pickle()
    snapshots.put(tag, snapshot)
    snapshot

  final def getSnapshot(tag: String): Option[String] =
    snapshots.get(tag)

  final override def createTable(
      createTableRequest: CreateTableRequest
  ): CreateTableResponse = {
    val tableName = createTableRequest.tableName()
    val keySchema =
      createTableRequest.keySchema().asScala.map(_.attributeName()).toSet

    def prepareIndexes(
        globalSecondaryIndexes: Iterable[GlobalSecondaryIndex]
    ): collection.mutable.Buffer[InMemoryDynamoDbIndex] =
      collection.mutable.Buffer
        .from(globalSecondaryIndexes.map { index =>
          InMemoryDynamoDbIndex(
            indexName = index.indexName(),
            keySchema = index.keySchema().asScala.map(ks => ks.attributeName()).toSet,
            projectionType = index.projection().projectionType,
            nonKeyAttributes = index.projection().nonKeyAttributes().asScala.toSeq
          )
        })

    tables.get(tableName) match {
      case None =>
        tables.addOne(
          tableName -> InMemoryDynamoDbTable(
            keySchema,
            collection.mutable.Map.empty[DynamoDbItemKey, DynamoDbItem],
            prepareIndexes(createTableRequest.globalSecondaryIndexes().asScala)
          )
        )
        CreateTableResponse.builder().build()

      case Some(table) =>
        if (table.keySchema == keySchema)
        then CreateTableResponse.builder().build()
        else
          throw new DynamoDbCreateTableKeySchemaNotMatching(
            s"tableName=${tableName}, existing keySchema=${keySchema}, received keySchema=${table.keySchema}"
          )
    }
  }

  override def waiter(): DynamoDbWaiter = new DynamoDbWaiter {
    override def close(): Unit = ()
    override def waitUntilTableExists(
        describeTableRequest: DescribeTableRequest
    ): WaiterResponse[DescribeTableResponse] =
      DefaultWaiterResponse
        .builder()
        .attemptsExecuted(1)
        .response(describeTable(describeTableRequest))
        .build()
  }

  final override def describeTable(
      describeTableRequest: DescribeTableRequest
  ): DescribeTableResponse = {
    val tableName = describeTableRequest.tableName()
    tables.get(tableName) match {
      case Some(table) =>
        DescribeTableResponse
          .builder()
          .table(
            TableDescription
              .builder()
              .tableName(tableName)
              .keySchema(table.keySchema.toList match {
                case hash :: Nil =>
                  Seq(
                    KeySchemaElement
                      .builder()
                      .attributeName(hash)
                      .keyType(KeyType.HASH)
                      .build()
                  ).asJava
                case hash :: range :: Nil =>
                  Seq(
                    KeySchemaElement
                      .builder()
                      .attributeName(hash)
                      .keyType(KeyType.HASH)
                      .build(),
                    KeySchemaElement
                      .builder()
                      .attributeName(range)
                      .keyType(KeyType.RANGE)
                      .build()
                  ).asJava
                case _ =>
                  throw new Exception(
                    s"Unexpected table keySchema ${table.keySchema}"
                  )
              })
              .itemCount(table.items.size.toLong)
              .globalSecondaryIndexes(
                table.indexes.map { index =>
                  GlobalSecondaryIndexDescription
                    .builder()
                    .indexName(index.indexName)
                    .keySchema(index.keySchema.toList match {
                      case hash :: Nil =>
                        Seq(
                          KeySchemaElement
                            .builder()
                            .attributeName(hash)
                            .keyType(KeyType.HASH)
                            .build()
                        ).asJava
                      case hash :: range :: Nil =>
                        Seq(
                          KeySchemaElement
                            .builder()
                            .attributeName(hash)
                            .keyType(KeyType.HASH)
                            .build(),
                          KeySchemaElement
                            .builder()
                            .attributeName(range)
                            .keyType(KeyType.RANGE)
                            .build()
                        ).asJava
                      case _ =>
                        throw new Exception(
                          s"Unexpected index keySchema ${index.keySchema}"
                        )
                    })
                    .build()
                }.asJava
              )
              .build()
          )
          .build()

      case None =>
        throw new DynamoDbUndefinedTable(s"tableName=$tableName")
    }
  }

  final override def putItem(
      putItemRequest: PutItemRequest
  ): PutItemResponse = {
    val tableName = putItemRequest.tableName()
    tables.get(tableName) match {
      case None => throw new DynamoDbUndefinedTable(s"$tableName")
      case Some(table) =>
        val entity = putItemRequest.item().asScala
        val key = entity.collect {
          case (k, v) if table.keySchema.contains(k) => (k, v)
        }
        val item =
          entity.view.filterKeys(key => !table.keySchema.contains(key)).toMap
        if key.size == table.keySchema.size
        then {
          table.items.update(key, item)
          PutItemResponse.builder().build()
        } else
          throw new DynamoDbInvalidItemKey(
            s"tableName=$tableName, expectedKeySchema=${table.keySchema}, receivedKey=$key"
          )
    }
  }

  final override def getItem(
      getItemRequest: GetItemRequest
  ): GetItemResponse = {
    val tableName = getItemRequest.tableName()
    tables.get(tableName) match {
      case None => throw new DynamoDbUndefinedTable(s"tableName=$tableName")
      case Some(table) =>
        val key = getItemRequest.key().asScala
        if (key.keySet == table.keySchema)
        then
          table.items.getByKey(key).match {
            case None =>
              GetItemResponse.builder().build() // empty item returned

            case Some(item) =>
              GetItemResponse
                .builder()
                .item(
                  maybeApplyProjectionExpression(
                    getItemRequest.projectionExpression(),
                    getItemRequest.expressionAttributeNames().asScala,
                    item ++ key
                  ).asJava
                )
                .build()
          }
        else
          throw new DynamoDbInvalidItemKey(
            s"tableName=$tableName, expectedKeySchema=${table.keySchema}, receivedKey=$key"
          )
    }
  }

  final override def deleteItem(
      deleteItemRequest: DeleteItemRequest
  ): DeleteItemResponse = {
    val tableName = deleteItemRequest.tableName()
    tables.get(tableName) match {
      case None => throw new DynamoDbUndefinedTable(s"tableName=$tableName")
      case Some(table) =>
        val key = deleteItemRequest.key().asScala
        if (key.keySet == table.keySchema)
        then {
          table.items.remove(key)
          DeleteItemResponse.builder().build()
        } else
          throw new DynamoDbInvalidItemKey(
            s"tableName=$tableName, expectedKeySchema=${table.keySchema}, receivedKey=$key"
          )
    }
  }

  final override def updateItem(
      updateItemRequest: UpdateItemRequest
  ): UpdateItemResponse = {
    val tableName = updateItemRequest.tableName()
    tables.get(tableName) match {
      case None => throw new DynamoDbUndefinedTable(s"tableName=$tableName")
      case Some(table) =>
        val key = updateItemRequest.key().asScala
        if (key.keySet != table.keySchema)
        then
          throw new DynamoDbInvalidItemKey(
            s"tableName=$tableName, expectedKeySchema=${table.keySchema}, receivedKey=$key"
          )
        else {
          val item: DynamoDbItem = table.items.get(key).getOrElse(Map.empty)
          UpdateItemResponse.builder().build() // no item found
          val attributeNames =
            updateItemRequest.expressionAttributeNames().asScala
          val attributeValues =
            updateItemRequest.expressionAttributeValues().asScala
          val updateExpression = updateItemRequest.updateExpression()
          val attributeUpdates: collection.Map[String, AttributeValueUpdate] =
            if updateItemRequest.attributeUpdates().isEmpty() then
              AwsDynamoDbApi.AttributeUpdate.parseUpdateExpression(
                updateExpression,
                attributeNames,
                attributeValues
              )
            else updateItemRequest.attributeUpdates().asScala

          val updatedItem: DynamoDbItem =
            attributeUpdates.foldLeft(item) { case (item, (path, update)) =>
              update.actionAsString() match {
                case "PUT" =>
                  item.setByPath(path, update.value())

                case "ADD" =>
                  item
                    .getByPath(path)
                    .map { value =>
                      if ((update.value().n() != null) && (value.n() != null))
                      then
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .n(
                                (value.n().toDouble + update
                                  .value()
                                  .n()
                                  .toDouble).toString()
                              )
                              .build()
                          )
                      else if (update.value().hasSs() && value.hasSs()) then
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .ss(
                                (value.ss().asScala
                                  ++ update.value().ss().asScala).toSet.asJava
                              )
                              .build()
                          )
                      else if (update.value().hasNs() && value.hasNs()) then
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .ns(
                                (value.ns().asScala
                                  ++ update.value().ns().asScala).toSet.asJava
                              )
                              .build()
                          )
                      else if (update.value().hasBs() && value.hasBs()) then
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .bs(
                                (value.bs().asScala
                                  ++ update.value().bs().asScala).toSet.asJava
                              )
                              .build()
                          )
                      else item
                    }
                    .getOrElse {
                      if (update.value().hasSs()) then
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .ss(update.value().ss())
                              .build()
                          )
                      else if (update.value().hasNs()) then
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .ns(update.value().ns())
                              .build()
                          )
                      else if (update.value().hasBs()) then
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .bs(update.value().bs())
                              .build()
                          )
                      else
                        throw new Exception(s"Unsupported ADD operation on path '$path' using value: ${update.value()}")
                    }

                case "DELETE" =>
                  if (update.value() != null && update.value().hasSs()) then
                    item
                      .getByPath(path)
                      .map { value =>
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .ss((value.ss().asScala.toSet -- update.value().ss().asScala.toSet).asJava)
                              .build()
                          )
                      }
                      .getOrElse(item)
                  else if (update.value() != null && update.value().hasNs()) then
                    item
                      .getByPath(path)
                      .map { value =>
                        item
                          .setByPath(
                            path,
                            AttributeValue
                              .builder()
                              .ns((value.ns().asScala.toSet -- update.value().ns().asScala.toSet).asJava)
                              .build()
                          )
                      }
                      .getOrElse(item)
                  else item.removeByPath(path)

                case "SET:list_append" =>
                  item.appendToList(path, update.value())

                case "SET:list_prepend" =>
                  item.prependToList(path, update.value())

                case "SET:if_not_exists" =>
                  item.getByPath(path).match {
                    case None    => item.setByPath(path, update.value())
                    case Some(_) => item
                  }

                case "REMOVE:index" =>
                  update.value().maybeInt.match {
                    case Some(index) =>
                      item.getByPath(path).match {
                        case Some(elem) =>
                          elem.maybeList match {
                            case Some(list) =>
                              item.setByPath(
                                path,
                                AttributeValue.builder
                                  .l((list.take(Math.max(0, index - 1)) ++ list.drop(index)).asJava)
                                  .build()
                              )
                            case None =>
                              throw new Exception(
                                s"Unsupported DELETE operation on path '$path' using value: ${update.value()}; target element is not a list"
                              )
                          }
                        case None => item
                      }
                    case None =>
                      throw new Exception(
                        s"Unsupported DELETE operation on path '$path' using value: ${update.value()}; index value must be an integer"
                      )
                  }

                case _ =>
                  item
              }
            }
          table.items.update(key, updatedItem) // save modified item back
          if (updateItemRequest.returnValues() == ReturnValue.ALL_NEW)
          then
            UpdateItemResponse
              .builder()
              .attributes(updatedItem.asJava)
              .build()
          else UpdateItemResponse.builder().build()
        }
    }
  }

  final override def query(
      queryRequest: QueryRequest
  ): QueryResponse = {
    val tableName = queryRequest.tableName()
    tables.get(tableName) match {
      case None => throw new DynamoDbUndefinedTable(s"tableName=$tableName")
      case Some(table) =>
        val keyConditionExpression = queryRequest.keyConditionExpression()
        val attributeNames =
          queryRequest.expressionAttributeNames().asScala
        val attributeValues =
          queryRequest.expressionAttributeValues().asScala

        val kce: KeyConditionExpression =
          parseKeyConditionExpression(
            keyConditionExpression,
            attributeNames,
            attributeValues
          )

        val selectedItems: Seq[DynamoDbItem] =
          Option(queryRequest.indexName()).match {
            case None =>
              if (table.keySchema == kce.keySchema)
              then
                table.items
                  .filter((keys, _) => kce.matches(keys))
                  .toSeq
                  .map((k, i) => i ++ k)
              else
                throw new DynamoDbInvalidItemKey(
                  s"tableName=$tableName, expectedKeySchema=${table.keySchema}, receivedKeys=${kce.keySchema}"
                )

            case Some(indexName) =>
              table.indexes.find(_.indexName == indexName) match {
                case None =>
                  throw new DynamoDbUndefinedIndex(
                    s"tableName=$tableName, cannot find indexName=$indexName, got only ${table.indexes.map(_.indexName).mkString(",")}"
                  )
                case Some(index) =>
                  if (index.keySchema == kce.keySchema)
                  then
                    index
                      .filter(
                        table.items,
                        kce
                      )
                      .toSeq
                  else
                    throw new DynamoDbInvalidItemKey(
                      s"tableName=$tableName, indexName=$indexName, expectedKeySchema=${index.keySchema}, receivedKeys=${kce.keySchema}"
                    )
              }
          }

        val filterConditions: Iterable[FilterCondition] =
          Option(queryRequest.filterExpression())
            .map(filterExpression =>
              parseFilterExpression(
                filterExpression,
                attributeNames,
                attributeValues
              )
            )
            .getOrElse(Seq.empty)

        val filteredItems: Iterable[DynamoDbItem] =
          filterConditions.foldLeft(kce.sort(selectedItems))((items, filterCondition) => filterCondition.filter(items))

        val projections: Option[Iterable[String]] =
          parseProjectionExpression(
            queryRequest.projectionExpression(),
            attributeNames
          )

        QueryResponse
          .builder()
          .scannedCount(selectedItems.size)
          .count(filteredItems.size)
          .items(
            filteredItems
              .map(item =>
                maybeApplyProjections(
                  projections,
                  item
                ).asJava
              )
              .toSeq
              .asJava
          )
          .build()
    }
  }

  final override def scan(
      scanRequest: ScanRequest
  ): ScanResponse = {
    val tableName = scanRequest.tableName()
    tables.get(tableName) match {
      case None => throw new DynamoDbUndefinedTable(s"tableName=$tableName")
      case Some(table) =>
        val attributeNames =
          scanRequest.expressionAttributeNames().asScala
        val attributeValues =
          scanRequest.expressionAttributeValues().asScala

        val selectedItems: Seq[DynamoDbItem] =
          Option(scanRequest.indexName()).match {
            case None =>
              table.items.toSeq
                .map((k, i) => i ++ k)

            case Some(indexName) =>
              table.indexes.find(_.indexName == indexName) match {
                case None =>
                  throw new DynamoDbUndefinedIndex(
                    s"tableName=$tableName, cannot find indexName=$indexName, got only ${table.indexes.map(_.indexName).mkString(",")}"
                  )
                case Some(index) =>
                  index.items(table.items).toSeq
              }
          }

        val filterConditions: Iterable[FilterCondition] =
          Option(scanRequest.filterExpression())
            .map(filterExpression =>
              parseFilterExpression(
                filterExpression,
                attributeNames,
                attributeValues
              )
            )
            .getOrElse(Seq.empty)

        val filteredItems: Iterable[DynamoDbItem] =
          filterConditions.foldLeft[Iterable[DynamoDbItem]](selectedItems)((items, filterCondition) =>
            filterCondition.filter(items)
          )

        val projections: Option[Iterable[String]] =
          parseProjectionExpression(
            scanRequest.projectionExpression(),
            attributeNames
          )

        ScanResponse
          .builder()
          .scannedCount(selectedItems.size)
          .count(filteredItems.size)
          .items(
            filteredItems
              .map(item =>
                maybeApplyProjections(
                  projections,
                  item
                ).asJava
              )
              .toSeq
              .asJava
          )
          .build()
    }
  }
}

object InMemoryDynamoDb {

  import AwsDynamoDbApi.ReadWriters.given

  case class InMemoryDynamoDbTable(
      keySchema: Set[String],
      items: collection.mutable.Map[DynamoDbItemKey, DynamoDbItem],
      indexes: collection.mutable.Buffer[InMemoryDynamoDbIndex] = collection.mutable.Buffer.empty
  ) {

    inline def update(key: DynamoDbItemKey, item: DynamoDbItem): this.type =
      items.update(key, item)
      this

    inline def dump(): InMemoryDynamoDbTableDump =
      InMemoryDynamoDbTableDump(
        keySchema,
        items.toMap,
        indexes.toSeq
      )
  }

  extension (items: collection.mutable.Map[DynamoDbItemKey, DynamoDbItem]) {
    inline def getByKey(lookupKey: DynamoDbItemKey): Option[DynamoDbItem] =
      items.find((itemKey, item) => isSameKey(itemKey, lookupKey)).map(_._2)
  }

  inline def isSameKey(key1: DynamoDbItemKey, key2: DynamoDbItemKey): Boolean =
    key1.size == key2.size
      && key1.iterator.forall((k, v1) =>
        key2
          .get(k)
          .exists(v2 => v1.isSameAs(v2))
      )

  object InMemoryDynamoDbTable {
    inline def from(dump: InMemoryDynamoDbTableDump): InMemoryDynamoDbTable =
      InMemoryDynamoDbTable(
        keySchema = dump.keySchema,
        items = collection.mutable.Map.from(dump.items),
        indexes = Buffer.from(dump.indexes)
      )
  }

  case class InMemoryDynamoDbTableDump(
      keySchema: Set[String],
      items: Map[DynamoDbItemKey, DynamoDbItem],
      indexes: Seq[InMemoryDynamoDbIndex] = Seq.empty
  ) derives ReadWriter

  case class InMemoryDynamoDbIndex(
      indexName: String,
      keySchema: Set[String],
      projectionType: ProjectionType,
      nonKeyAttributes: Seq[String]
  ) derives ReadWriter {
    inline def filter(
        data: scala.collection.Map[DynamoDbItemKey, DynamoDbItem],
        kce: KeyConditionExpression
    ): Iterable[DynamoDbItem] =
      val items = data.filter(kce.matches)
      println(
        s"${AnsiColor.CYAN}[InMemoryDynamoDB][$indexName] filtering records using $kce, found ${items.size} out of ${data.size}${AnsiColor.RESET}"
      )
      if (projectionType == ProjectionType.KEYS_ONLY)
      then items.keySet
      else if (projectionType == ProjectionType.ALL)
      then items.map((k, i) => k ++ i)
      else {
        val projection = nonKeyAttributes ++ keySchema
        items
          .map((k, i) => k ++ maybeApplyProjections(Some(projection), i))
      }

    inline def items(
        data: scala.collection.Map[DynamoDbItemKey, DynamoDbItem]
    ): Iterable[DynamoDbItem] =
      val items = data.filter((k, i) => keySchema.forall(p => k.contains(p) || i.contains(p)))
      println(
        s"${AnsiColor.CYAN}[InMemoryDynamoDB][$indexName] filtering records using key schema, found ${items.size} out of ${data.size}${AnsiColor.RESET}"
      )
      if (projectionType == ProjectionType.KEYS_ONLY)
      then items.keySet
      else if (projectionType == ProjectionType.ALL)
      then items.map((k, i) => k ++ i)
      else {
        val projection = nonKeyAttributes ++ keySchema
        items
          .map((k, i) => k ++ maybeApplyProjections(Some(projection), i))
      }

  }

  class DynamoDbCreateTableKeySchemaNotMatching(m: String) extends Exception(m)
  class DynamoDbUndefinedTable(m: String) extends Exception(m)
  class DynamoDbUndefinedIndex(m: String) extends Exception(m)
  class DynamoDbInvalidItemKey(m: String) extends Exception(m)
  class DynamoDbInvalidKeyConditionExpression(m: String) extends Exception(m)
  class DynamoDbInvalidNameExpression(m: String) extends Exception(m)
  class DynamoDbInvalidValueExpression(m: String) extends Exception(m)
  class DynamoDbInvalidKeyCondition(m: String) extends Exception(m)

  inline def parseProjectionExpression(
      projectionExpression: String,
      attributeNames: scala.collection.Map[String, String]
  ): Option[Iterable[String]] =
    Option(projectionExpression).map(
      _.split(",")
        .map(_.trim())
        .map(key => attributeNames.getOrElse(key, key))
    )

  inline def maybeApplyProjectionExpression(
      projectionExpression: String,
      attributeNames: scala.collection.Map[String, String],
      item: DynamoDbItem
  ): DynamoDbItem =
    maybeApplyProjections(
      parseProjectionExpression(projectionExpression, attributeNames),
      item
    )

  inline def maybeApplyProjections(
      projectionExpression: Option[Iterable[String]],
      item: DynamoDbItem
  ): DynamoDbItem =
    projectionExpression.match {
      case Some(fields) if fields.nonEmpty => item.filterByPaths(fields)
      case _                               => item
    }

  case class KeyConditionExpression(
      partitionKeyCondition: KeyCondition,
      otherKeyConditions: KeyCondition*
  ) {

    final override def toString(): String =
      s"$partitionKeyCondition ${otherKeyConditions.map(_.toString()).mkString(" ")}"

    val keySchema: Set[String] =
      Set(partitionKeyCondition._1) ++ otherKeyConditions.map(_._1).toSet

    inline def matches(itemKey: DynamoDbItemKey): Boolean =
      partitionKeyCondition.matches(itemKey)
        && otherKeyConditions.forall(_.matches(itemKey))

    inline def matches(item: (DynamoDbItemKey, DynamoDbItem)): Boolean = {
      def extractValue(name: String): Option[AttributeValue] =
        item._1.get(name).orElse(item._2.get(name))

      partitionKeyCondition.matches(extractValue)
      && otherKeyConditions.forall(_.matches(extractValue))
    }

    inline def sort(items: Seq[DynamoDbItem]): Iterable[DynamoDbItem] =
      otherKeyConditions.headOption
        .map(sk =>
          sk._3.headOption
            .map(attr =>
              if (attr.isNumber)
              then
                items.sortBy(item =>
                  item
                    .get(sk._1)
                    .flatMap(_.maybeDouble)
                    .getOrElse(0d)
                )
              else items.sortBy(item => item.get(sk._1).map(_.toString()).getOrElse(""))
            )
            .getOrElse(items)
        )
        .getOrElse(items)
  }

  final def parseKeyConditionExpression(
      keyConditionExpression: String,
      attributeNames: collection.Map[String, String],
      attributeValues: collection.Map[String, AttributeValue]
  ): KeyConditionExpression = {

    def resolveName(nameRef: String): String =
      attributeNames
        .get(nameRef)
        .getOrElse(
          throw new DynamoDbInvalidNameExpression(
            s"Name reference $nameRef is not found in the expressionAttributeNames ${attributeNames.mkString(",")}"
          )
        )

    def resolveValue(valueRef: String): AttributeValue =
      attributeValues
        .get(valueRef)
        .getOrElse(
          throw new DynamoDbInvalidValueExpression(
            s"Value reference $valueRef is not found in the expressionAttributeValues ${attributeValues
                .mkString(",")}"
          )
        )

    val conditions: Iterable[Condition] =
      Condition.parse(keyConditionExpression, resolveName, resolveValue)

    val partitionKeyCondition: Condition =
      conditions.headOption.getOrElse {
        throw new DynamoDbInvalidKeyConditionExpression(
          s"keyConditionExpression $keyConditionExpression is invalid, missing a partitionKey"
        )
      }

    if (partitionKeyCondition._2 != CompareFunction.EQUAL) then
      throw new DynamoDbInvalidValueExpression(
        s"Partition key condition must be '=' in ${partitionKeyCondition.show()}"
      )

    val sortKeyCondition: Option[Condition] = conditions.drop(1).headOption

    sortKeyCondition.foreach(c =>
      if (!CompareFunction.isAllowedInSortKey(c._2)) then
        throw new DynamoDbInvalidValueExpression(
          s"Condition ${c.show()} is not allowed for a sort key"
        )
    )

    KeyConditionExpression(partitionKeyCondition, sortKeyCondition.toSeq*)
  }

  final def parseFilterExpression(
      filterExpression: String,
      attributeNames: collection.Map[String, String],
      attributeValues: collection.Map[String, AttributeValue]
  ): Iterable[FilterCondition] = {

    def resolveName(nameRef: String): String =
      attributeNames
        .get(nameRef)
        .getOrElse(
          throw new DynamoDbInvalidNameExpression(
            s"Name reference $nameRef is not found in the expressionAttributeNames ${attributeNames.mkString(",")}"
          )
        )

    def resolveValue(valueRef: String): AttributeValue =
      attributeValues
        .get(valueRef)
        .getOrElse(
          throw new DynamoDbInvalidValueExpression(
            s"Value reference $valueRef is not found in the expressionAttributeValues ${attributeValues
                .mkString(",")}"
          )
        )

    Condition.parse(filterExpression, resolveName, resolveValue)
  }

}

class InMemorySecretsManager extends SecretsManagerClient {

  import InMemorySecretsManager.*

  val secrets = collection.mutable.Map[String, String]()

  final def setup(newSecrets: Map[String, String]): Unit =
    secrets.addAll(newSecrets)

  final override def close(): Unit = ()
  final override def serviceName(): String = "secretsmanager"

  override def getSecretValue(
      getSecretValueRequest: GetSecretValueRequest
  ): GetSecretValueResponse = {
    secrets
      .get(getSecretValueRequest.secretId())
      .map(secret => GetSecretValueResponse.builder().secretString(secret).build())
      .getOrElse(throw new UnknownSecretId(getSecretValueRequest.secretId()))
  }

  override def putSecretValue(
      putSecretValueRequest: PutSecretValueRequest
  ): PutSecretValueResponse = {
    secrets.addOne(
      putSecretValueRequest.secretId() -> putSecretValueRequest
        .secretString()
    )
    PutSecretValueResponse.builder().versionId("0").build()
  }

}

object InMemorySecretsManager {
  class UnknownSecretId(m: String) extends Exception(m)
}

final class LambdaClientStub extends LambdaClient {

  override def close(): Unit = ()
  override def serviceName(): String = "lambda"

  type Handler = (String, String) => String
  private var handler: Handler = (name, input) => input

  final def setHandler(newHandler: Handler): Unit =
    handler = newHandler

  override def invoke(invokeRequest: InvokeRequest): InvokeResponse =
    val statusCode: Int =
      invokeRequest.invocationType() match {
        case InvocationType.EVENT   => 202
        case InvocationType.DRY_RUN => 204
        case _                      => 200
      }
    InvokeResponse
      .builder()
      .statusCode(statusCode)
      .responseMetadata(DefaultAwsResponseMetadata.create(Map("AWS_REQUEST_ID" -> UUID.randomUUID().toString()).asJava))
      .asInstanceOf[InvokeResponse.Builder]
      .payload(
        SdkBytes
          .fromString(
            handler(
              invokeRequest.functionName(),
              invokeRequest.payload().asUtf8String()
            ),
            StandardCharsets.UTF_8
          )
      )
      .build

}

final class StsClientStub() extends StsClient {

  override def close(): Unit = ()
  override def serviceName(): String = "sts"

  private var accountId: Option[String] = None

  final def setAwsAccountId(newAccountId: String): Unit =
    accountId = Some(newAccountId)

  override def getCallerIdentity(): GetCallerIdentityResponse =
    val account = accountId
      .orElse(Option(System.getenv().get("AWS_ACCOUNT_ID")))
      .getOrElse(throw new Exception("Undefined AWS account in StsClient stub."))

    GetCallerIdentityResponse.builder().account(account).build()

}

final class SqsClientStub() extends SqsClient {

  override def close(): Unit = ()
  override def serviceName(): String = "sqs"

  type Message = (String, String) // body + messageId

  private lazy val queues =
    ConcurrentHashMap[String, ConcurrentLinkedDeque[Message]]().asScala

  def offer(queueUrl: String, messageWithId: Message): Unit =
    val queue = queues.getOrElseUpdate(queueUrl, new ConcurrentLinkedDeque[Message]())
    queue.offer(messageWithId)
    println(
      s"${AnsiColor.CYAN}[AwsClientStatefulStub] added new message $messageWithId to the queue $queueUrl, now queue size is ${queue.size}"
    )

  def peek(queueUrl: String): Option[Message] =
    queues.get(queueUrl).flatMap { queue =>
      if (queue.isEmpty()) None else Some(queue.peek())
    }

  def poll(queueUrl: String): Option[Message] =
    queues.get(queueUrl).flatMap { queue =>
      if (queue.isEmpty()) None else Some(queue.poll())
    }

  override def sendMessage(sendMessageRequest: SendMessageRequest): SendMessageResponse =
    val messageId = UUID.randomUUID().toString()
    offer(sendMessageRequest.queueUrl(), (sendMessageRequest.messageBody(), messageId))
    SendMessageResponse
      .builder()
      .messageId(messageId)
      .build()

}
