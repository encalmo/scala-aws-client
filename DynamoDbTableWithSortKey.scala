package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.*
import org.encalmo.aws.AwsDynamoDbApi.given
import org.encalmo.aws.Utils.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate
import scala.deriving.Mirror

trait DynamoDbTableWithSortKey[PartitionKeyType, SortKeyType](partitionKeyName: String, sortKeyName: String)(using
    partitionKeyToAttributeValue: Conversion[PartitionKeyType, AttributeValue],
    sortKeyToAttributeValue: Conversion[SortKeyType, AttributeValue]
) {

  inline def baseTableName: String

  final inline def tableName(using env: DynamoDbEnvironment, error: ErrorContext): String =
    s"${env.dynamodbTableArnPrefix}${baseTableName}${env.dynamodbTableArnSuffix}"

  inline def partitionKeyOf(partitionKey: PartitionKeyType): (String, AttributeValue) =
    (partitionKeyName, partitionKeyToAttributeValue(partitionKey))

  inline def sortKeyOf(sortKey: SortKeyType): (String, AttributeValue) =
    (sortKeyName, sortKeyToAttributeValue(sortKey))

  inline def getItem(
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType
  )(using awsClient: AwsClient, env: DynamoDbEnvironment, error: ErrorContext): Option[DynamoDbItem] =
    AwsClient.optionally {
      getItemFromTableUsingCompositeKey(tableName, Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey)))
    }

  inline def getItem(
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline projection: Seq[String]
  )(using awsClient: AwsClient, env: DynamoDbEnvironment, error: ErrorContext): Option[DynamoDbItem] =
    AwsClient.optionally {
      getItemFromTableUsingCompositeKey(tableName, Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey)), projection)
    }

  inline def getItemOrError(
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType
  )(using awsClient: AwsClient, env: DynamoDbEnvironment, error: ErrorContext): Either[error.Error, DynamoDbItem] =
    AwsClient.maybe {
      getItemFromTableUsingCompositeKey(tableName, Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey)))
        .toRight(
          error(
            errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
            errorMessage =
              s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey' and sortKey '$sortKey'"
          )
        )
    }.eitherErrorOrResultFlatten

  inline def getItemProperty[T: upickle.default.ReadWriter](
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline property: String
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Option[T] =
    getItem(partitionKey, sortKey, Seq(property)).maybe[T](property)

  inline def getItemPropertyOrError[T: upickle.default.ReadWriter](
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline property: String
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, T] =
    getItem(partitionKey, sortKey, Seq(property))
      .toRight(
        error(
          errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
          errorMessage =
            s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey' and sortKey '$sortKey'"
        )
      )
      .flatMap(
        _.maybe[T](property)
          .toRight(
            error(
              errorCode = s"${baseTableName.capitalize}RecordIsMissing${property.capitalize}Error",
              errorMessage =
                s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey' and sortKey '$sortKey'"
            )
          )
      )

  inline def getItemProperties[Types <: Tuple](
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline properties: String*
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Option[Types] =
    getItem(partitionKey, sortKey, properties)
      .flatMap(_.maybeTuple[Types](properties*))

  inline def getItemPropertiesOrError[Types <: Tuple](
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline properties: String*
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Types] =
    getItem(partitionKey, sortKey, properties)
      .toRight(
        error(
          errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
          errorMessage =
            s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey' and sortKey '$sortKey'"
        )
      )
      .flatMap(
        _.eitherTupleOrListMissingKeys[Types](properties*).left
          .map(missingKeys =>
            error(
              errorCode = s"${baseTableName.capitalize}RecordIsMissingPropertiesError",
              errorMessage =
                s"DynamoDB table '$baseTableName' record for a primary key '$partitionKey' and sortKey '$sortKey' is missing keys: ${missingKeys
                    .mkString(", ")}"
            )
          )
      )

  private inline def labels[Tup <: Tuple](result: List[String]): List[String] =
    inline scala.compiletime.erasedValue[Tup] match {
      case _: EmptyTuple => result
      case _: (h *: t)   => labels[t](scala.compiletime.constValue[h].asInstanceOf[String] :: result)
    }

  inline def getItemAsClass[T <: Product](partitionKey: PartitionKeyType, sortKey: SortKeyType)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Option[T] =
    getItem(partitionKey, sortKey, labels[mirror.MirroredElemLabels](Nil))
      .flatMap(_.maybeClass[T])

  inline def getItemAsClassOrError[T <: Product](partitionKey: PartitionKeyType, sortKey: SortKeyType)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Either[error.Error, T] =
    def labels[Tup <: Tuple](result: List[String]): List[String] =
      inline scala.compiletime.erasedValue[Tup] match {
        case _: EmptyTuple => result
        case _: (h *: t)   => scala.compiletime.constValue[t].asInstanceOf[String] :: result
      }
    getItem(partitionKey, sortKey, labels[mirror.MirroredElemLabels](Nil))
      .toRight(
        error(
          errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
          errorMessage =
            s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey' and sortKey '$sortKey'"
        )
      )
      .flatMap(
        _.eitherClassOrListMissingKeys[T].left
          .map(missingKeys =>
            error(
              errorCode = s"${baseTableName.capitalize}RecordIsMissingPropertiesError",
              errorMessage =
                s"DynamoDB table '$baseTableName' record for a primary key '$partitionKey' and sortKey '$sortKey' is missing expected keys: ${missingKeys
                    .mkString(", ")}"
            )
          )
      )

  inline def getItemPropertyAsClass[T <: Product](
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline property: String
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Option[T] =
    getItem(partitionKey, sortKey, Seq(property)).flatMap(_.maybeClass[T](property))

  inline def getItemPropertyAsListOfClass[T <: Product](
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline property: String
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Option[Seq[T]] =
    getItem(partitionKey, sortKey, Seq(property)).flatMap(_.maybeListOfClass[T](property))

  inline def setItemProperty(
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      inline property: String,
      value: AttributeValue
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        updateItemInTableUsingCompositeKey(
          tableName,
          Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey)),
          Map(
            property -> value
          )
        )
      )
      .eitherErrorOrUnit

  inline def setItemProperties(
      partitionKey: PartitionKeyType,
      sortKey: SortKeyType,
      propertyUpdates: (String, AttributeValueUpdate)*
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        updateItemInTableUsingCompositeKey(
          tableName,
          Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey)),
          propertyUpdates.toMap
        )
      )
      .eitherErrorOrUnit

  inline def setItem[T <: Product](item: T, removeUndefinedProperties: Boolean = true)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Either[error.Error, Unit] =
    for {
      partitionKey <- maybeExtractValue[PartitionKeyType](partitionKeyName)(item)
      sortKey <- maybeExtractValue[SortKeyType](sortKeyName)(item)
      result <- AwsClient
        .maybe(
          updateItemInTableUsingCompositeKey(
            tableName,
            Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey)), {
              val update = translateProductToDynamoDbItemUpdate(item, removeUndefinedProperties)
              update.remove(partitionKeyName)
              update.remove(sortKeyName)
              DynamoDbItemUpdate.from(update)
            }
          )
        )
        .eitherErrorOrUnit
    } yield result

  inline def removeItemProperty(partitionKey: PartitionKeyType, sortKey: SortKeyType, inline property: String)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        updateItemInTableUsingCompositeKey(
          tableName,
          Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey)),
          Map(
            property -> REMOVE
          )
        )
      )
      .eitherErrorOrUnit

  inline def removeItem(partitionKey: PartitionKeyType, sortKey: SortKeyType)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        deleteItemFromTableUsingCompositeKey(
          tableName,
          Seq(partitionKeyOf(partitionKey), sortKeyOf(sortKey))
        )
      )
      .eitherErrorOrUnit
}
