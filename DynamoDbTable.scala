package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.*
import org.encalmo.aws.AwsDynamoDbApi.given
import org.encalmo.aws.Utils.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate
import scala.deriving.Mirror

trait DynamoDbTable[PartitionKeyType](partitionKeyName: String)(using
    keyToAttributeValue: Conversion[PartitionKeyType, AttributeValue]
) {

  inline def baseTableName: String

  final inline def tableName(using env: DynamoDbEnvironment, error: ErrorContext): String =
    s"${env.dynamodbTableArnPrefix}${baseTableName}${env.dynamodbTableArnSuffix}"

  inline def partitionKeyOf(partitionKey: PartitionKeyType): (String, AttributeValue) =
    (partitionKeyName, keyToAttributeValue(partitionKey))

  inline def getItem(
      partitionKey: PartitionKeyType
  )(using awsClient: AwsClient, env: DynamoDbEnvironment, error: ErrorContext): Option[DynamoDbItem] =
    AwsClient.optionally {
      getItemFromTable(tableName, partitionKeyOf(partitionKey))
    }

  inline def getItemOrError(
      partitionKey: PartitionKeyType
  )(using awsClient: AwsClient, env: DynamoDbEnvironment, error: ErrorContext): Either[error.Error, DynamoDbItem] =
    AwsClient.maybe {
      getItemFromTable(tableName, partitionKeyOf(partitionKey))
        .toRight(
          error(
            errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
            errorMessage = s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey'"
          )
        )
    }.eitherErrorOrResultFlatten

  inline def getItem(
      partitionKey: PartitionKeyType,
      inline projection: Seq[String]
  )(using awsClient: AwsClient, env: DynamoDbEnvironment, error: ErrorContext): Option[DynamoDbItem] =
    AwsClient.optionally {
      getItemFromTable(tableName, partitionKeyOf(partitionKey), projection)
    }

  inline def getItemOrError(
      partitionKey: PartitionKeyType,
      inline projection: Seq[String]
  )(using awsClient: AwsClient, env: DynamoDbEnvironment, error: ErrorContext): Either[error.Error, DynamoDbItem] =
    AwsClient.maybe {
      getItemFromTable(tableName, partitionKeyOf(partitionKey), projection)
        .toRight(
          error(
            errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
            errorMessage = s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey'"
          )
        )
    }.eitherErrorOrResultFlatten

  inline def getItemProperty[T: upickle.default.ReadWriter](partitionKey: PartitionKeyType, inline property: String)(
      using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Option[T] =
    getItem(partitionKey, Seq(property)).maybe[T](property)

  inline def getNestedItem(partitionKey: PartitionKeyType, inline property: String)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Option[DynamoDbItem] =
    getItem(partitionKey, Seq(property))
      .flatMap(_.get(property))
      .flatMap(_.maybeNestedItem)

  inline def getNestedItemByPath(partitionKey: PartitionKeyType, inline propertyPath: Seq[String])(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Option[DynamoDbItem] =
    getItem(partitionKey, Seq(propertyPath.mkString(".")))
      .flatMap(_.get(propertyPath.head))
      .flatMap(_.getByPath(propertyPath.drop(1)))
      .flatMap(_.maybeNestedItem)

  inline def getItemPropertyOrError[T: upickle.default.ReadWriter](
      partitionKey: PartitionKeyType,
      inline property: String
  )(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, T] =
    getItem(partitionKey, Seq(property))
      .toRight(
        error(
          errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
          errorMessage = s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey'"
        )
      )
      .flatMap(
        _.maybe[T](property)
          .toRight(
            error(
              errorCode = s"${baseTableName.capitalize}RecordIsMissing${property.capitalize}Error",
              errorMessage = s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey'"
            )
          )
      )

  inline def getItemProperties[Types <: Tuple](partitionKey: PartitionKeyType, inline properties: String*)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Option[Types] =
    getItem(partitionKey, properties)
      .flatMap(_.maybeTuple[Types](properties*))

  inline def getItemPropertiesOrError[Types <: Tuple](partitionKey: PartitionKeyType, inline properties: String*)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Types] =
    getItem(partitionKey, properties)
      .toRight(
        error(
          errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
          errorMessage = s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey'"
        )
      )
      .flatMap(
        _.eitherTupleOrListMissingKeys[Types](properties*).left
          .map(missingKeys =>
            error(
              errorCode = s"${baseTableName.capitalize}RecordIsMissingPropertiesError",
              errorMessage =
                s"DynamoDB table '$baseTableName' record for a primary key '$partitionKey' is missing expected keys: ${missingKeys
                    .mkString(", ")}"
            )
          )
      )

  private inline def labels[Tup <: Tuple](result: List[String]): List[String] =
    inline scala.compiletime.erasedValue[Tup] match {
      case _: EmptyTuple => result
      case _: (h *: t)   => labels[t](scala.compiletime.constValue[h].asInstanceOf[String] :: result)
    }

  inline def getItemAsClass[T <: Product](partitionKey: PartitionKeyType)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Option[T] =
    getItem(partitionKey, labels[mirror.MirroredElemLabels](Nil))
      .flatMap(_.maybeClass[T])

  inline def getItemAsClassOrError[T <: Product](partitionKey: PartitionKeyType)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Either[error.Error, T] =
    getItem(partitionKey, labels[mirror.MirroredElemLabels](Nil))
      .toRight(
        error(
          errorCode = s"${baseTableName.capitalize}RecordNotFoundError",
          errorMessage = s"DynamoDB table '$baseTableName' is missing a record for a primary key '$partitionKey'"
        )
      )
      .flatMap(
        _.eitherClassOrListMissingKeys[T].left
          .map(missingKeys =>
            error(
              errorCode = s"${baseTableName.capitalize}RecordIsMissingPropertiesError",
              errorMessage =
                s"DynamoDB table '$baseTableName' record for a primary key '$partitionKey' is missing expected keys: ${missingKeys
                    .mkString(", ")}"
            )
          )
      )

  inline def getItemPropertyAsClass[T <: Product](partitionKey: PartitionKeyType, inline property: String)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Option[T] =
    getItem(partitionKey, Seq(property)).flatMap(_.maybeClass[T](property))

  inline def getItemPropertyAsClassOrError[T <: Product](partitionKey: PartitionKeyType, inline property: String)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Either[error.Error, T] =
    getItemOrError(partitionKey, Seq(property)).flatMap(
      _.maybeClass[T](property)
        .toRight(
          error(
            errorCode = s"${baseTableName.capitalize}RecordUnmarshallingError",
            errorMessage =
              s"DynamoDB table '$baseTableName' record property '$property' for a primary key '$partitionKey' cannot be unmarshalled as a class product"
          )
        )
    )

  inline def getItemPropertiesAsClass[T <: Product](partitionKey: PartitionKeyType, inline properties: String*)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Option[T] =
    getItem(partitionKey, properties)
      .flatMap(_.maybeTuple[mirror.MirroredElemTypes](properties*))
      .map(mirror.fromTuple)

  inline def getItemPropertiesAsClassOrError[T <: Product](partitionKey: PartitionKeyType, inline properties: String*)(
      using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Either[error.Error, T] =
    getItemOrError(partitionKey, properties)
      .flatMap(
        _.eitherTupleOrListMissingKeys[mirror.MirroredElemTypes](properties*).left
          .map(missingKeys =>
            error(
              errorCode = s"${baseTableName.capitalize}RecordIsMissingPropertiesError",
              errorMessage =
                s"DynamoDB table '$baseTableName' record for a primary key '$partitionKey' is missing expected keys: ${missingKeys
                    .mkString(", ")}"
            )
          )
      )
      .map(mirror.fromTuple)

  inline def getItemPropertyAsListOfClass[T <: Product](partitionKey: PartitionKeyType, inline property: String)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      mirror: Mirror.ProductOf[T]
  ): Option[Seq[T]] =
    getItem(partitionKey, Seq(property)).flatMap(_.maybeListOfClass[T](property))

  inline def setItemProperty(partitionKey: PartitionKeyType, inline property: String, value: AttributeValue)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        updateItemInTable(
          tableName,
          (partitionKeyOf(partitionKey)),
          Map(
            property -> value
          )
        )
      )
      .eitherErrorOrUnit

  inline def setNestedItem(partitionKey: PartitionKeyType, inline property: String, value: DynamoDbItem)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        updateItemInTable(
          tableName,
          (partitionKeyOf(partitionKey)),
          Map(
            property -> value
          )
        )
      )
      .eitherErrorOrUnit

  inline def setNestedItemProperty[T](partitionKey: PartitionKeyType, inline propertyPath: Seq[String], value: T)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      convert: Conversion[T, AttributeValue]
  ): Either[error.Error, Unit] =
    AwsClient.maybe {
      val nestedItem =
        getItemFromTable(tableName, partitionKeyOf(partitionKey), Seq(propertyPath.head)).match {
          case Some(originaNestedItem: DynamoDbItem) =>
            originaNestedItem
              .maybeNestedItem(propertyPath.head)
              .getOrElse(Map.empty)
              .setByPath(propertyPath.drop(1), convert(value))

          case None =>
            propertyPath.tail.init.foldRight[DynamoDbItem](DynamoDbItem(propertyPath.last -> convert(value))) {
              (property, item) =>
                DynamoDbItem(property -> item)
            }

        }
      updateItemInTable(
        tableName,
        (partitionKeyOf(partitionKey)),
        Map(
          propertyPath.head -> nestedItem
        )
      )
    }.eitherErrorOrUnit

  inline def setItemPropertyIfSome[T](partitionKey: PartitionKeyType, inline property: String, valueOpt: Option[T])(
      using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext,
      convert: Conversion[T, AttributeValue]
  ): Either[error.Error, Unit] =
    valueOpt
      .map(value =>
        AwsClient
          .maybe(
            updateItemInTable(
              tableName,
              (partitionKeyOf(partitionKey)),
              Map(
                property -> convert(value)
              )
            )
          )
          .eitherErrorOrUnit
      )
      .getOrElse(Right(()))

  inline def setItemProperties(partitionKey: PartitionKeyType, propertyUpdates: (String, AttributeValueUpdate)*)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        updateItemInTable(
          tableName,
          (partitionKeyOf(partitionKey)),
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
    maybeExtractValue[PartitionKeyType](partitionKeyName)(item)
      .flatMap(partitionKey =>
        AwsClient
          .maybe(
            updateItemInTable(
              tableName,
              (partitionKeyOf(partitionKey)), {
                val update = translateProductToDynamoDbItemUpdate(item, removeUndefinedProperties)
                update.remove(partitionKeyName)
                DynamoDbItemUpdate.from(update)
              }
            )
          )
          .eitherErrorOrUnit
      )

  inline def removeItemProperty(partitionKey: PartitionKeyType, inline property: String)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        updateItemInTable(
          tableName,
          (partitionKeyOf(partitionKey)),
          Map(
            property -> REMOVE
          )
        )
      )
      .eitherErrorOrUnit

  inline def removeItem(partitionKey: PartitionKeyType)(using
      awsClient: AwsClient,
      env: DynamoDbEnvironment,
      error: ErrorContext
  ): Either[error.Error, Unit] =
    AwsClient
      .maybe(
        deleteItemFromTable(
          tableName,
          (partitionKeyOf(partitionKey))
        )
      )
      .eitherErrorOrUnit

  inline def executeOnlyOnce(using
      error: ErrorContext
  )(key: PartitionKeyType, flag: String, action: AwsClient ?=> Either[error.Error, Unit])(using
      AwsClient,
      DynamoDbEnvironment
  ): Either[error.Error, Unit] =
    AwsClient.optionally(getItemProperty[Boolean](key, flag)).match {
      case Some(true) => Right(())
      case Some(false) | None =>
        action
          .flatMap(_ => AwsClient.maybe(setItemProperty(key, flag, true)).eitherErrorOrUnit)

    }
}
