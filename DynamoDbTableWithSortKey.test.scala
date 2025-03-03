package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.{*, given}
import org.encalmo.models.CUID2
import org.encalmo.models.Amount

import java.time.Instant

class DynamoDbTableWithSortKeySpec extends TestSuite {

  given AwsClient = AwsClient.initializeWithProperties(
    localAwsSecurityCredentials ++ debugModeProperties
  )

  given DynamoDbEnvironment = DefaultDynamoDbEnvironment
  given ErrorContext = DefaultErrorContext

  test("DynamoDbTableWithSortKey should provide methods to set, get and remove item properties") {

    val id = CUID2.randomCUID2()
    val createdAt = Instant.now().getEpochSecond()

    assert(OrdersTable.getItemOrError(id, createdAt).isLeft)
    OrdersTable.setItemProperties(id, createdAt, "status" -> "submitted")
    assert(OrdersTable.getItemOrError(id, createdAt).isRight)
    OrdersTable.removeItemProperty(id, createdAt, "status")
    assert(OrdersTable.getItemOrError(id, createdAt).isRight)
    OrdersTable.removeItem(id, createdAt)
    assert(OrdersTable.getItemOrError(id, createdAt).isLeft)
  }

  test("set and get item as a class instance") {

    val id = CUID2.randomCUID2()
    val createdAt = Instant.now().getEpochSecond()

    val order =
      Order(
        id,
        createdAt,
        status = Some("idle"),
        amount = Amount(0),
        codes = Seq(18, 97, 345),
        attributes = Map("alpha" -> 1, "beta" -> 2)
      )

    OrdersTable.setItem(order)

    val order0 = OrdersTable.getItemAsClass[Order](id, createdAt)
    println(order0)

    assert(order0.isDefined)
    assertEquals(order0.get.order_id, id)
    assertEquals(order0.get.amount, Amount(0))
    assertEquals(order0.get.status, Some("idle"))

    OrdersTable.setItemProperties(id, createdAt, "amount" -> Amount(1234), "status" -> REMOVE)

    val order1 = OrdersTable.getItemAsClass[Order](id, createdAt)
    assert(order1.isDefined)
    assertEquals(order1.get.order_id, id)
    assertEquals(order1.get.amount, Amount(1234))
    assertEquals(order1.get.status, None)

    OrdersTable.setItemProperties(id, createdAt, "status" -> "fine")

    val order2 = OrdersTable.getItemAsClass[Order](id, createdAt)
    assert(order2.isDefined)
    assertEquals(order2.get.order_id, id)
    assertEquals(order2.get.amount, Amount(1234))
    assertEquals(order2.get.status, Some("fine"))
  }

}

case class Order(
    order_id: CUID2,
    createdAt: Long,
    status: Option[String] = None,
    amount: Amount,
    codes: Seq[Int],
    attributes: Map[String, Long]
)

object OrdersTable extends DynamoDbTableWithSortKey[CUID2, Long]("order_id", "createdAt") {
  override inline def baseTableName: String = "orders"
}
