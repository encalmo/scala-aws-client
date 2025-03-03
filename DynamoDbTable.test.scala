package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.{*, given}
import org.encalmo.models.CUID2
import org.encalmo.models.Amount

class DynamoDbTableSpec extends TestSuite {

  given AwsClient = AwsClient.initializeWithProperties(
    localAwsSecurityCredentials ++ debugModeProperties
  )

  given DynamoDbEnvironment = DefaultDynamoDbEnvironment
  given ErrorContext = DefaultErrorContext

  test("DynamoDbTable should provide methods to set, get and remove item properties") {

    val id = CUID2.randomCUID2()

    assert(ExampleTable.getItemOrError(id).isLeft)
    ExampleTable.setItemProperties(id, "status" -> "fine", "amount" -> Amount(1234))
    assert(ExampleTable.getItemOrError(id).isRight)
    ExampleTable.removeItemProperty(id, "status")
    assert(ExampleTable.getItemOrError(id).isRight)
    ExampleTable.removeItem(id)
    assert(ExampleTable.getItemOrError(id).isLeft)
  }

  test("README example") {

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

    Items.removeItem(id)
  }

  test("set and get item as a class instance") {

    val id = CUID2.randomCUID2()

    val example =
      Example(
        id,
        status = Some("idle"),
        amount = Amount(0),
        codes = Seq(18, 97, 345),
        attributes = Map("alpha" -> 1, "beta" -> 2)
      )

    val id2 = maybeExtractValue[CUID2]("id")(example)
    assert(id2.isRight)
    id2.foreach(assertEquals(_, id))

    ExampleTable.setItem(example)

    val example0 = ExampleTable.getItemAsClass[Example](id)
    assert(example0.isDefined)
    assertEquals(example0.get.id, id)
    assertEquals(example0.get.amount, Amount(0))
    assertEquals(example0.get.status, Some("idle"))

    ExampleTable.setItemProperties(id, "amount" -> Amount(1234), "status" -> REMOVE)
    val example1 = ExampleTable.getItemAsClass[Example](id)
    assert(example1.isDefined)
    assertEquals(example1.get.id, id)
    assertEquals(example1.get.amount, Amount(1234))
    assertEquals(example1.get.status, None)
    ExampleTable.setItemProperties(id, "status" -> "fine")
    val example2 = ExampleTable.getItemAsClass[Example](id)
    assert(example2.isDefined)
    assertEquals(example2.get.id, id)
    assertEquals(example2.get.amount, Amount(1234))
    assertEquals(example2.get.status, Some("fine"))
  }

}

case class Example(
    id: CUID2,
    status: Option[String] = None,
    amount: Amount,
    codes: Seq[Int],
    attributes: Map[String, Long]
)

object ExampleTable extends DynamoDbTable[CUID2]("id") {
  override inline def baseTableName: String = "example"
}

case class Item(
    item_id: CUID2,
    price: Amount,
    is_for_sale: Boolean,
    description: Option[String] = None
)

object Items extends DynamoDbTable[CUID2]("item_id") {
  override inline def baseTableName: String = "items"
}
