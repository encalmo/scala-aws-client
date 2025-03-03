package org.encalmo.aws

import org.encalmo.aws.AwsDynamoDbApi.*
import org.encalmo.aws.AwsDynamoDbApi.DocumentPath.toAttributeNamesMap
import org.encalmo.aws.AwsDynamoDbApi.given

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import upickle.default.ReadWriter

import java.util.regex.Pattern
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

class AwsDynamoDbApiSpec extends TestSuite {

  override val munitTimeout = Duration(300, "s")

  final case class XYZ(foo: String, bar: Int)
  final case class XYZOpt(foo: Option[String] = None, bar: Option[Int] = None)

  val nested = mapOf(
    "a" -> 1,
    "b" -> "2",
    "c" -> listOf(
      111,
      "abc",
      true,
      mapOf("d" -> 9, "e" -> mapOf("f" -> "the end"))
    )
  )

  val item: DynamoDbItem =
    Map(
      "key" -> "bar",
      "int" -> 7,
      "int2" -> 7.0,
      "ints" -> Seq(1, 2, 3),
      "strings" -> Seq("a", "ab", "abc"),
      "booleans" -> Seq(true, false, false),
      "list" -> listOf(1, "a", true),
      "decimal" -> BigDecimal("0.99999"),
      "double" -> 1.1d,
      "nested" -> nested
    )

  test("convert to/from AttributeValue") {
    assertEquals(fromInt(5).getInt(0), 5)
    assertEquals(fromDouble(5.7d).getInt(3), 5)
    assertEquals(fromDouble(5.7d).getDouble(0d), 5.7d)
    assertEquals(
      fromBigDecimal(BigDecimal("999.999999999999"))
        .getDecimal(BigDecimal("0.00")),
      BigDecimal("999.999999999999")
    )
    assertEquals(
      fromBigDecimal(BigDecimal("999.999999999999")).getInt(0),
      999
    )
    assertEquals(fromLong(8989889898989889898L).getInt(0), 2147483647)
    assertEquals(
      fromLong(8989889898989889898L).getLong(0L),
      8989889898989889898L
    )
    assertEquals(fromBoolean(true).getBoolean(false), true)
    assertEquals(fromBoolean(false).getBoolean(true), false)
    assertEquals(fromString("true").getBoolean(false), false)
    assertEquals(fromString("true").getString("false"), "true")
    assertEquals(fromInt(5).getString("false"), "false")
  }

  test("maybe convert to/from AttributeValue") {
    assertEquals(fromInt(5).maybeInt, Some(5))
    assertEquals(fromDouble(5.7d).maybeInt, Some(5))
    assertEquals(fromDouble(5.7d).maybeDouble, Some(5.7d))
    assertEquals(
      fromBigDecimal(BigDecimal("999.999999999999")).maybeDecimal,
      Some(BigDecimal("999.999999999999"))
    )
    assertEquals(
      fromBigDecimal(BigDecimal("999.999999999999")).maybeInt,
      Some(999)
    )
    assertEquals(fromLong(8989889898989889898L).maybeInt, Some(2147483647))
    assertEquals(
      fromLong(8989889898989889898L).maybeLong,
      Some(8989889898989889898L)
    )
    assertEquals(fromBoolean(true).maybeBoolean, Some(true))
    assertEquals(fromBoolean(false).maybeBoolean, Some(false))
    assertEquals(fromString("true").maybeBoolean, None)
    assertEquals(fromString("true").maybeString, Some("true"))
    assertEquals(convertFromJson(ujson.Str("true")).maybeString, Some("true"))
    assertEquals(convertFromJson(ujson.Num(5.73d)).maybeDouble, Some(5.73d))
    assertEquals(
      fromDynamoDbItem(DynamoDbItem("foo" -> 5)).m().get("foo"),
      fromInt(5)
    )
    assertEquals(
      fromDynamoDbItem(DynamoDbItem("foo" -> "a", "bar" -> 1)).maybeClass[XYZ],
      Some(XYZ("a", 1))
    )
  }

  test("compare AttributeValue") {
    assert(fromBoolean(true).isSameAs(fromBoolean(true)))
    assert(!fromBoolean(true).isSameAs(fromBoolean(false)))
    assert(!fromBoolean(true).isSameAs(fromString("true")))
    assert(fromString("true").isSameAs(fromString("true")))
    assert(!fromString("true").isSameAs(fromString("false")))
    assert(fromInt(5).isSameAs(fromInt(5)))
    assert(!fromInt(5).isSameAs(fromInt(6)))
    assert(fromIterableOfInt(Seq(5, 6, 7)).isSameAs(fromIterableOfInt(Seq(5, 6, 7))))
    assert(!fromIterableOfInt(Seq(5, 6, 7)).isSameAs(fromIterableOfInt(Seq(5, 6))))
    assert(fromIterableOfString(Seq("a", "b", "c")).isSameAs(fromIterableOfString(Seq("a", "b", "c"))))
    assert(!fromIterableOfString(Seq("a", "b", "c")).isSameAs(fromIterableOfString(Seq("a", "b"))))
    assert(fromIterable[String](Seq("a", "b", "c")).isSameAs(fromIterable[String](Seq("a", "b", "c"))))
    assert(!fromIterable[String](Seq("a", "b")).isSameAs(fromIterable[String](Seq("a", "b", "c"))))
    assert(fromDynamoDbItem(Map("a" -> "b")).isSameAs(fromDynamoDbItem(Map("a" -> "b"))))
    assert(!fromDynamoDbItem(Map("a" -> "b")).isSameAs(fromDynamoDbItem(Map("b" -> "a"))))
    assert(!fromDynamoDbItem(Map("a" -> "b")).isSameAs(fromDynamoDbItem(Map("b" -> "a", "a" -> "b"))))
    assert(!fromDynamoDbItem(Map("a" -> "b")).isSameAs(fromIterable[String](Seq("a", "b", "c"))))
  }

  test("retrieve data from DynamoDb item") {
    assertEquals(item.maybeString("key"), Some("bar"))
    assertEquals(item.maybe[String]("key"), Some("bar"))
    assertEquals(item.maybeBoolean("key"), None)
    assertEquals(item.maybeInt("key"), None)
    assertEquals(item.maybeInt("int"), Some(7))
    assertEquals(item.getBoolean("key", true), true)
    assertEquals(item.getInt("key", 6), 6)
    assertEquals(item.getInt("int", 6), 7)
    assertEquals(item.getDouble("int", 0d), 7d)
    assertEquals(item.maybeDouble("int"), Some(7d))
    assertEquals(item.maybeDouble("double"), Some(1.1d))
    assertEquals(item.getDecimal("int", 0d), BigDecimal("7"))
    assertEquals(item.maybeDecimal("int"), Some(BigDecimal("7")))
    assertEquals(item.maybeDecimal("decimal"), Some(BigDecimal("0.99999")))
    assertEquals(item.getString("key", "foo"), "bar")
    assertEquals(item.getString("key2", "foo"), "foo")
    assertEquals(item.maybeIntSet("key"), None)
    assertEquals(item.maybeIntSet("strings"), None)
    assertEquals(item.maybeIntSet("ints"), Some(Set(1, 2, 3)))
    assertEquals(item.maybeStringSet("key"), None)
    assertEquals(item.maybeStringSet("ints"), None)
    assertEquals(item.maybeStringSet("strings"), Some(Set("a", "ab", "abc")))
    assertEquals(item.maybeList("key"), None)
    assertEquals(item.maybeList("ints"), None)
    assertEquals(
      item.maybeList("booleans"),
      Some(Seq(fromBoolean(true), fromBoolean(false), fromBoolean(false)))
    )
    assertEquals(
      item.maybeList("list"),
      Some(Seq(fromInt(1), fromString("a"), fromBoolean(true)))
    )
    assertEquals(item.maybeNestedItem("key"), None)
    assertEquals(item.maybe[DynamoDbItem]("key"), None)
    assertEquals(item.maybeNestedItem("nested"), Some(nested.m().asScala))
    assertEquals(item.maybe[DynamoDbItem]("nested"), Some(nested.m().asScala))
  }

  test("retrieve data from DynamoDb item - 2") {
    assertEquals(item.maybe[String]("key"), Some("bar"))
    assertEquals(item.maybe[Boolean]("key"), None)
    assertEquals(item.maybe[Int]("key"), None)
    assertEquals(item.maybe[Int]("int"), Some(7))
    assertEquals(item.maybe[Int]("int2"), Some(7))
    assertEquals(item.maybe[Double]("int"), Some(7d))
    assertEquals(item.maybe[Double]("double"), Some(1.1d))
    assertEquals(item.maybe[BigDecimal]("int"), Some(BigDecimal("7")))
    assertEquals(item.maybe[BigDecimal]("decimal"), Some(BigDecimal("0.99999")))
    assertEquals(item.maybe[Set[Int]]("key"), None)
    assertEquals(item.maybe[Set[Int]]("strings"), None)
    assertEquals(item.maybe[Set[Int]]("ints"), Some(Set(1, 2, 3)))
    assertEquals(item.maybe[Set[String]]("key"), None)
    assertEquals(item.maybe[Set[String]]("ints"), None)
    assertEquals(item.maybe[Set[String]]("strings"), Some(Set("a", "ab", "abc")))
    assertEquals(item.maybe[List[String]]("key"), None)
    assertEquals(item.maybe[List[String]]("ints"), None)
    assertEquals(
      item.maybe[Seq[Boolean]]("booleans"),
      Some(Seq(true, false, false))
    )
  }

  case class Foo(string: String, int: Int) derives ReadWriter

  test("marshall and maybe unmarshall scala object") {
    val foo1 = Foo("Hello", 5)
    val value: AttributeValue = Foo("Hello", 5)
    val foo2 = value.maybe[Foo]
    assertEquals(foo2, Some(foo1))
  }

  test("entity toDynamoDbItem") {
    assertEquals(
      Foo("bar", 5).toDynamoDbItem,
      DynamoDbItem(
        "string" -> "bar",
        "int" -> 5d
      )
    )
  }

  test("DynamoDbItem.maybe") {
    assertEquals(
      DynamoDbItem(
        "string" -> "bar",
        "int" -> 5d
      ).maybe[Foo],
      Some(Foo("bar", 5))
    )
  }

  test("unmarshall scala object") {
    val foo = Foo("Hello", 5)
    val value: AttributeValue =
      AttributeValue.builder().s("""{"string":"Hello","int":5}""").build()
    val foo1 = value.maybe[Foo]
    assertEquals(foo1, Some(foo))
    val foo2 = value.getOrDefault[Foo](Foo("a", 1))
    assertEquals(foo1, Some(foo))
    val value2: AttributeValue =
      AttributeValue.builder().s("foo").build()
    val foo3 = value2.maybe[Foo]
    assertEquals(foo3, None)
    val foo4 = value2.getOrDefault[Foo](Foo("a", 1))
    assertEquals(foo4, Foo("a", 1))
    val foo5 = value2.getOrDefault[Foo]("""{"string":"b","int":2}""")
    assertEquals(foo5, Foo("b", 2))
  }

  test("item getByPath") {
    assertEquals(item.getByPath("int"), Some(fromInt(7)))
    assertEquals(item.getByPath("key"), Some(fromString("bar")))
    assertEquals(item.getByPath("ints"), Some(fromIterable[Int](Seq(1, 2, 3))))
    assertEquals(item.getByPath("double"), Some(fromDouble(1.1d)))
    assertEquals(
      item.getByPath("decimal"),
      Some(fromBigDecimal(BigDecimal("0.99999")))
    )
    assertEquals(item.getByPath("nested"), Some(nested))
    assertEquals(item.getByPath("ints[0]"), Some(fromInt(1)))
    assertEquals(item.getByPath("ints[1]"), Some(fromInt(2)))
    assertEquals(item.getByPath("ints[2]"), Some(fromInt(3)))
    assertEquals(item.getByPath("ints[3]"), None)
    assertEquals(item.getByPath("nested.a"), Some(fromInt(1)))
    assertEquals(item.getByPath("nested.b"), Some(fromString("2")))
    assertEquals(item.getByPath("nested.c[0]"), Some(fromInt(111)))
    assertEquals(item.getByPath("nested.c[1]"), Some(fromString("abc")))
    assertEquals(item.getByPath("nested.c[2]"), Some(fromBoolean(true)))
    assertEquals(
      item.getByPath("nested.c[3]"),
      Some(mapOf("d" -> 9, "e" -> mapOf("f" -> "the end")))
    )
    assertEquals(item.getByPath("nested.c[3].d"), Some(fromInt(9)))
    assertEquals(
      item.getByPath("nested.c[3].e.f"),
      Some(fromString("the end"))
    )
  }

  test("value getByPath") {
    val value = fromDynamoDbItem(item)
    assertEquals(value.getByPath("int"), Some(fromInt(7)))
    assertEquals(value.getByPath("key"), Some(fromString("bar")))
    assertEquals(value.getByPath("ints"), Some(fromIterable[Int](Seq(1, 2, 3))))
    assertEquals(value.getByPath("double"), Some(fromDouble(1.1d)))
    assertEquals(
      value.getByPath("decimal"),
      Some(fromBigDecimal(BigDecimal("0.99999")))
    )
    assertEquals(value.getByPath("nested"), Some(nested))
    assertEquals(value.getByPath("ints[0]"), Some(fromInt(1)))
    assertEquals(value.getByPath("ints[1]"), Some(fromInt(2)))
    assertEquals(value.getByPath("ints[2]"), Some(fromInt(3)))
    assertEquals(value.getByPath("ints[3]"), None)
    assertEquals(value.getByPath("nested.a"), Some(fromInt(1)))
    assertEquals(value.getByPath("nested.b"), Some(fromString("2")))
    assertEquals(value.getByPath("nested.c[0]"), Some(fromInt(111)))
    assertEquals(value.getByPath("nested.c[1]"), Some(fromString("abc")))
    assertEquals(value.getByPath("nested.c[2]"), Some(fromBoolean(true)))
    assertEquals(
      value.getByPath("nested.c[3]"),
      Some(mapOf("d" -> 9, "e" -> mapOf("f" -> "the end")))
    )
    assertEquals(value.getByPath("nested.c[3].d"), Some(fromInt(9)))
    assertEquals(
      value.getByPath("nested.c[3].e.f"),
      Some(fromString("the end"))
    )
  }

  test("item maybe[T]") {
    assertEquals(item.maybe[String]("key"), Some("bar"))
    assertEquals(item.maybe[Int]("int"), Some(7))
    assertEquals(item.maybe[Int]("nested", "a"), Some(1))
    assertEquals(item.maybe[String]("nested", "b"), Some("2"))
    assertEquals(item.maybe[Int]("nested", "c", "0"), Some(111))
    assertEquals(item.maybe[String]("nested", "c", "1"), Some("abc"))
  }

  test("item getOrFail[T]") {
    assertEquals(item.getOrFail[String]("key"), "bar")
    intercept[IllegalStateException] {
      item.getOrFail[String]("key2")
    }
  }

  test("item getOrDefault[T]") {
    assertEquals(item.getOrDefault[String]("key", "baz"), "bar")
    assertEquals(item.getOrDefault[String]("key2", "baz"), "baz")
    assertEquals(item.getOrDefault[Int]("int", 2), 7)
    assertEquals(item.getOrDefault[Int]("int3", 2), 2)
  }

  test("value getOrDefault[T]") {
    assertEquals(fromString("bar").getOrDefault[String]("baz"), "bar")
    assertEquals(fromString("bar").getOrDefault[Int](124), 124)
    assertEquals(fromInt(123).getOrDefault[Int](124), 123)
    assertEquals(fromInt(123).getOrDefault[String]("baz"), "baz")
  }

  test("maybe item maybe[T]") {
    assertEquals(Some(item).maybe[String]("key"), Some("bar"))
    assertEquals(Some(item).maybe[Int]("int"), Some(7))
    assertEquals(Some(item).maybe[Int]("nested", "a"), Some(1))
    assertEquals(Some(item).maybe[String]("nested", "b"), Some("2"))
    assertEquals(Some(item).maybe[Int]("nested", "c", "0"), Some(111))
    assertEquals(Some(item).maybe[String]("nested", "c", "1"), Some("abc"))
    assertEquals(None.maybe[String]("key"), None)
    assertEquals(None.maybe[Int]("int"), None)
  }

  test("item setByPath") {
    val value = fromString("bar")
    assertEquals(
      item.set("foo", value).maybeString("foo"),
      Some("bar")
    )
    assertEquals(
      item.setByPath("foo", value).maybeString("foo"),
      Some("bar")
    )
    assertEquals(
      item.setByPath("foo.baz", value).getByPath("foo.baz"),
      Some(value)
    )
    assertEquals(
      item.setByPath("foo.baz.zoo", value).getByPath("foo.baz.zoo"),
      Some(value)
    )
    assertEquals(
      item
        .setByPath("foo.baz.zoo.one.hop", value)
        .getByPath("foo.baz.zoo.one.hop"),
      Some(value)
    )
    assertEquals(
      item.setByPath("foo.1", value).getByPath("foo.1"),
      Some(value)
    )
    assertEquals(
      item.setByPath("foo.1.baz", value).getByPath("foo.1.baz"),
      Some(value)
    )
    assertEquals(
      item.setByPath("foo.1.baz.3.one", value).getByPath("foo.1.baz.3.one"),
      Some(value)
    )
    assertEquals(
      item
        .setByPath("foo.1", value)
        .setByPath("foo.1", nested)
        .getByPath("foo.1"),
      Some(nested)
    )
    assertEquals(
      item
        .setByPath("foo.1", value)
        .setByPath("foo.1", fromString("one"))
        .getByPath("foo.1"),
      Some(fromString("one"))
    )
    assertEquals(
      item
        .setByPath("foo.1", value)
        .setByPath("foo.1", fromInt(999))
        .getByPath("foo.1"),
      Some(fromInt(999))
    )
    assertEquals(
      item
        .setByPath("foo.1", value)
        .setByPath("foo.1", fromInt(999))
        .setByPath("foo.1", fromBoolean(false))
        .getByPath("foo.1"),
      Some(fromBoolean(false))
    )
    assertEquals(
      item
        .setByPath("foo[0][0]", value)
        .setByPath("foo[0][1]", fromInt(999))
        .setByPath("foo[0][2]", fromBoolean(false))
        .getByPath("foo[0][1]"),
      Some(fromInt(999))
    )
    assertEquals(
      item
        .setByPath("foo[0].bar[0]", value)
        .setByPath("foo[0].bar[1]", fromInt(999))
        .setByPath("foo[0].bar[2]", fromBoolean(false))
        .getByPath("foo[0].bar[1]"),
      Some(fromInt(999))
    )
    assertEquals(
      item
        .setByPath("foo[0].bar[0]", value)
        .setByPath("foo[1].bar[1]", fromInt(999))
        .setByPath("foo[2].bar[2]", fromBoolean(false))
        .getByPath("foo[1].bar[1]"),
      Some(fromInt(999))
    )
  }

  test("item removeByPath") {
    assertEquals(
      DynamoDbItem("foo" -> 123, "bar" -> "baz").removeByPath("foo"),
      DynamoDbItem("bar" -> "baz")
    )
    assertEquals(
      DynamoDbItem("foo" -> 123, "bar" -> "baz").removeByPath("bar"),
      DynamoDbItem("foo" -> 123)
    )
    assertEquals(
      DynamoDbItem("foo" -> 123, "bar" -> "baz").removeByPath("fox"),
      DynamoDbItem("foo" -> 123, "bar" -> "baz")
    )
    assertEquals(
      DynamoDbItem("foo" -> 123, "bar" -> DynamoDbItem("a" -> 1, "b" -> "2"))
        .removeByPath("bar.a"),
      DynamoDbItem("foo" -> 123, "bar" -> DynamoDbItem("b" -> "2"))
    )
    assertEquals(
      DynamoDbItem("foo" -> 123, "bar" -> DynamoDbItem("a" -> 1, "b" -> "2"))
        .removeByPath("bar.b"),
      DynamoDbItem("foo" -> 123, "bar" -> DynamoDbItem("a" -> 1))
    )
    assertEquals(
      DynamoDbItem(
        "foo" -> 123,
        "bar" -> DynamoDbItem("a" -> 1, "b" -> "2"),
        "zoo" -> listOf("1", "2", "3")
      )
        .removeByPath("zoo[0]"),
      DynamoDbItem(
        "foo" -> 123,
        "bar" -> DynamoDbItem("a" -> 1, "b" -> "2"),
        "zoo" -> listOf("2", "3")
      )
    )
    assertEquals(
      DynamoDbItem(
        "foo" -> 123,
        "bar" -> DynamoDbItem("a" -> 1, "b" -> "2"),
        "zoo" -> listOf("1", "2", "3")
      )
        .removeByPath("zoo[1]"),
      DynamoDbItem(
        "foo" -> 123,
        "bar" -> DynamoDbItem("a" -> 1, "b" -> "2"),
        "zoo" -> listOf("1", "3")
      )
    )
  }

  test("filterByPaths") {
    assertEquals(
      item.getByPath("ints"),
      Some(fromIterable[Int](Seq(1, 2, 3)))
    )
    val newItem = DynamoDbItem()
    assertEquals(
      newItem.setByPath("ints", fromIterable[Int](Seq(1, 2, 3))),
      DynamoDbItem("ints" -> Seq(1, 2, 3))
    )
    assertEquals(
      item.filterByPaths(Seq("ints")),
      DynamoDbItem("ints" -> Seq(1, 2, 3))
    )
    assertEquals(
      item.filterByPaths(Seq("ints", "nested")),
      DynamoDbItem("ints" -> Seq(1, 2, 3), "nested" -> nested)
    )
    assertEquals(
      item.filterByPaths(Seq("ints", "nested.c[3].d", "nested.c[3].e")),
      DynamoDbItem(
        "ints" -> Seq(1, 2, 3),
        "nested" -> mapOf(
          "c" -> listOf(
            nullAttributeValue,
            nullAttributeValue,
            nullAttributeValue,
            mapOf("d" -> 9, "e" -> mapOf("f" -> "the end"))
          )
        )
      )
    )
  }

  test("convert to JSON") {
    assertEquals(fromString("hello").convertToJson, ujson.Str("hello"))
    assertEquals(fromInt(123).convertToJson, ujson.Num(123))
    assertEquals(fromDouble(123.45).convertToJson, ujson.Num(123.45d))
    assertEquals(fromBoolean(true).convertToJson, ujson.Bool(true))
    assertEquals(fromBoolean(false).convertToJson, ujson.Bool(false))
    assertEquals(
      fromBigDecimal(BigDecimal("123.456")).convertToJson,
      ujson.Num(123.456d)
    )
    assertEquals(
      fromIterable[String](Seq("a", "ab", "abc")).convertToJson,
      ujson.Arr(ujson.Str("a"), ujson.Str("ab"), ujson.Str("abc"))
    )
    assertEquals(
      fromIterable[Double](Seq(1, 3.1d, 6.567d)).convertToJson,
      ujson.Arr(ujson.Num(1), ujson.Num(3.1d), ujson.Num(6.567d))
    )
    assertEquals(
      nested.convertToJson,
      ujson.Obj(
        "a" -> ujson.Num(1),
        "b" -> ujson.Str("2"),
        "c" -> ujson.Arr(
          ujson.Num(111),
          ujson.Str("abc"),
          ujson.Bool(true),
          ujson.Obj(
            "d" -> ujson.Num(9),
            "e" -> ujson.Obj("f" -> ujson.Str("the end"))
          )
        )
      )
    )
  }

  test("convert from JSON") {
    assertEquals(
      convertFromJson(
        ujson.Obj(
          "a" -> ujson.Num(1),
          "b" -> ujson.Str("2"),
          "c" -> ujson.Arr(
            ujson.Num(111),
            ujson.Str("abc"),
            ujson.Bool(true),
            ujson.Obj(
              "d" -> ujson.Num(9),
              "e" -> ujson.Obj("f" -> ujson.Str("the end"))
            )
          )
        )
      ),
      nested
    )
  }

  test("parse document path") {
    assertEquals(
      DocumentPath.parse("foo"),
      Seq(DocumentPath.Attribute("foo"))
    )
    assertEquals(
      DocumentPath.parse("foo").toNameReference,
      "#foo"
    )
    assertEquals(
      DocumentPath.parse("foo").toAttributeNamesMap,
      Map("#foo" -> "foo")
    )
    assertEquals(
      DocumentPath.parse("foo").toValueReference,
      ":foo"
    )
    assertEquals(
      DocumentPath.parse("foo.bar"),
      Seq(DocumentPath.Attribute("foo"), DocumentPath.Attribute("bar"))
    )
    assertEquals(
      DocumentPath.parse("foo.bar").toNameReference,
      "#foo.#bar"
    )
    assertEquals(
      DocumentPath.parse("foo.bar").toAttributeNamesMap,
      Map("#foo" -> "foo", "#bar" -> "bar")
    )
    assertEquals(
      DocumentPath.parse("foo.bar").toValueReference,
      ":foo_bar"
    )
    assertEquals(
      DocumentPath.parse("foo.bar.zoo"),
      Seq(
        DocumentPath.Attribute("foo"),
        DocumentPath.Attribute("bar"),
        DocumentPath.Attribute("zoo")
      )
    )
    assertEquals(
      DocumentPath.parse("foo.bar.zoo").toNameReference,
      "#foo.#bar.#zoo"
    )
    assertEquals(
      DocumentPath.parse("foo.bar.zoo").toAttributeNamesMap,
      Map("#foo" -> "foo", "#bar" -> "bar", "#zoo" -> "zoo")
    )
    assertEquals(
      DocumentPath.parse("foo.bar.zoo").toValueReference,
      ":foo_bar_zoo"
    )
    assertEquals(
      DocumentPath.parse("[0]"),
      Seq(DocumentPath.Index(0))
    )
    assertEquals(
      DocumentPath.parse("[0]").toNameReference,
      "[0]"
    )
    assertEquals(
      DocumentPath.parse("[0]").toAttributeNamesMap,
      Map.empty
    )
    assertEquals(
      DocumentPath.parse("[0][1]"),
      Seq(DocumentPath.Index(0), DocumentPath.Index(1))
    )
    assertEquals(
      DocumentPath.parse("[0][1]").toNameReference,
      "[0][1]"
    )
    assertEquals(
      DocumentPath.parse("foo[0][1]"),
      Seq(
        DocumentPath.Attribute("foo"),
        DocumentPath.Index(0),
        DocumentPath.Index(1)
      )
    )
    assertEquals(
      DocumentPath.parse("foo[0][1]").toValueReference,
      ":foo_0_1"
    )
    assertEquals(
      DocumentPath.parse("foo[0][1]").toNameReference,
      "#foo[0][1]"
    )
    assertEquals(
      DocumentPath.parse("foo.fas[0][1].bar[2][3].zoo"),
      Seq(
        DocumentPath.Attribute("foo"),
        DocumentPath.Attribute("fas"),
        DocumentPath.Index(0),
        DocumentPath.Index(1),
        DocumentPath.Attribute("bar"),
        DocumentPath.Index(2),
        DocumentPath.Index(3),
        DocumentPath.Attribute("zoo")
      )
    )
    assertEquals(
      DocumentPath.parse("foo.fas[0][1].bar[2][3].zoo").toPath,
      "foo.fas[0][1].bar[2][3].zoo"
    )
    assertEquals(
      DocumentPath.parse("foo.fas[0][1].bar[2][3].zoo").toValueReference,
      ":foo_fas_0_1_bar_2_3_zoo"
    )
    assertEquals(
      DocumentPath.parse("foo.fas[0][1].bar[2][3].zoo").toNameReference,
      "#foo.#fas[0][1].#bar[2][3].#zoo"
    )
    assertEquals(
      DocumentPath.parse("foo.fas[0][1].bar[2][3].zoo").toAttributeNamesMap,
      Map(
        "#foo" -> "foo",
        "#fas" -> "fas",
        "#bar" -> "bar",
        "#zoo" -> "zoo"
      )
    )
    assertEquals(
      DocumentPath.parse(
        "fooBar.fooBar.fastTech[10][123][18].barSS[28][3001].zooM[0]"
      ),
      Seq(
        DocumentPath.Attribute("fooBar"),
        DocumentPath.Attribute("fooBar"),
        DocumentPath.Attribute("fastTech"),
        DocumentPath.Index(10),
        DocumentPath.Index(123),
        DocumentPath.Index(18),
        DocumentPath.Attribute("barSS"),
        DocumentPath.Index(28),
        DocumentPath.Index(3001),
        DocumentPath.Attribute("zooM"),
        DocumentPath.Index(0)
      )
    )
    assertEquals(
      DocumentPath
        .parse(
          "fooBar.fooBar.fastTech[10][123][18].barSS[28][3001].zooM[0]"
        )
        .toPath,
      "fooBar.fooBar.fastTech[10][123][18].barSS[28][3001].zooM[0]"
    )
    assertEquals(
      DocumentPath
        .parse(
          "fooBar.fooBar.fastTech[10][123][18].barSS[28][3001].zooM[0]"
        )
        .toAttributeNamesMap,
      Map(
        "#fooBar" -> "fooBar",
        "#fastTech" -> "fastTech",
        "#barSS" -> "barSS",
        "#zooM" -> "zooM"
      )
    )
  }

  test("create update expression") {
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo" -> 1
        )
      ),
      ("SET #foo = :foo", Map("#foo" -> "foo"), Map(":foo" -> fromInt(1)))
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo" -> 1,
          "bar" -> "Hello!"
        )
      ),
      (
        "SET #foo = :foo, #bar = :bar",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo" -> fromInt(1), ":bar" -> fromString("Hello!"))
      )
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo" -> 1,
          "bar" -> "Hello!",
          "zoo" -> REMOVE
        )
      ),
      (
        "REMOVE #zoo SET #foo = :foo, #bar = :bar",
        Map("#foo" -> "foo", "#bar" -> "bar", "#zoo" -> "zoo"),
        Map(":foo" -> fromInt(1), ":bar" -> fromString("Hello!"))
      )
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo" -> 1,
          "bar" -> "Hello!",
          "zoo" -> REMOVE,
          "one" -> add(1)
        )
      ),
      (
        "ADD #one :one REMOVE #zoo SET #foo = :foo, #bar = :bar",
        Map("#foo" -> "foo", "#bar" -> "bar", "#zoo" -> "zoo", "#one" -> "one"),
        Map(
          ":foo" -> fromInt(1),
          ":bar" -> fromString("Hello!"),
          ":one" -> fromInt(1)
        )
      )
    )
  }

  test("create update expression with nested paths") {
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo[1].bar[0]" -> 1
        )
      ),
      (
        "SET #foo[1].#bar[0] = :foo_1_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_1_bar_0" -> fromInt(1))
      )
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo[1].bar[0]" -> 1,
          "foo[0].bar" -> REMOVE
        )
      ),
      (
        "REMOVE #foo[0].#bar SET #foo[1].#bar[0] = :foo_1_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_1_bar_0" -> fromInt(1))
      )
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo[1].bar[0]" -> add(3)
        )
      ),
      (
        "ADD #foo[1].#bar[0] :foo_1_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_1_bar_0" -> fromInt(3))
      )
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo[0].bar[0]" -> 1,
          "foo[1].bar[1]" -> add(3),
          "foo[3].bar" -> REMOVE
        )
      ),
      (
        "ADD #foo[1].#bar[1] :foo_1_bar_1 REMOVE #foo[3].#bar SET #foo[0].#bar[0] = :foo_0_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_0_bar_0" -> fromInt(1), ":foo_1_bar_1" -> fromInt(3))
      )
    )
  }

  test("create update expression with function") {
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo" -> appendToList("a")
        )
      ),
      (
        "SET #foo = list_append(#foo, :foo)",
        Map("#foo" -> "foo"),
        Map(":foo" -> listOf("a"))
      )
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo" -> prependToList("a")
        )
      ),
      (
        "SET #foo = list_append(:foo, #foo)",
        Map("#foo" -> "foo"),
        Map(":foo" -> listOf("a"))
      )
    )
    assertEquals(
      AttributeUpdate.createUpdateExpression(
        Map(
          "foo" -> ifNotExists(fromString("a"))
        )
      ),
      (
        "SET #foo = if_not_exists(#foo, :foo)",
        Map("#foo" -> "foo"),
        Map(":foo" -> fromString("a"))
      )
    )
  }

  test("read pattern") {
    assertEquals(
      "#foo = :foo, #bar = :bar".readPattern(
        Pattern.compile("#([A-Za-z0-1])+\\s+")
      ),
      Right(("#foo ", "= :foo, #bar = :bar"))
    )
    assertEquals(
      "%#foo = :foo, #bar = :bar".readPattern(
        Pattern.compile("#([A-Za-z0-1])+\\s+")
      ),
      Left("%#foo = :foo, #bar = :bar")
    )
    assertEquals(
      "#foo = :foo, #bar = :bar".readPattern(
        Pattern.compile("$([A-Za-z0-1])+\\s+")
      ),
      Left("#foo = :foo, #bar = :bar")
    )
  }

  test("read multiple patterns") {
    assertEquals(
      "abc".readAnyPattern(
        Pattern.compile("a"),
        Pattern.compile("b")
      ),
      Right(("a", "bc"))
    )
    assertEquals(
      "bca".readAnyPattern(
        Pattern.compile("b")
      ),
      Right(("b", "ca"))
    )
    assertEquals(
      "bca".readAnyPattern(
        Pattern.compile("a"),
        Pattern.compile("b")
      ),
      Right(("b", "ca"))
    )
  }

  test("parse update expression") {
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "SET #foo = :foo",
        Map("#foo" -> "foo"),
        Map(":foo" -> fromInt(1))
      ),
      DynamoDbItemUpdate("foo" -> 1)
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "SET #foo = :foo, #bar = :bar",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo" -> fromInt(1), ":bar" -> fromString("Hello!"))
      ),
      DynamoDbItemUpdate(
        "foo" -> 1,
        "bar" -> "Hello!"
      )
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "REMOVE #zoo SET #foo = :foo, #bar = :bar",
        Map("#foo" -> "foo", "#bar" -> "bar", "#zoo" -> "zoo"),
        Map(":foo" -> fromInt(1), ":bar" -> fromString("Hello!"))
      ),
      DynamoDbItemUpdate(
        "foo" -> 1,
        "bar" -> "Hello!",
        "zoo" -> REMOVE
      )
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "ADD #one :one REMOVE #zoo SET #foo = :foo, #bar = :bar",
        Map("#foo" -> "foo", "#bar" -> "bar", "#zoo" -> "zoo", "#one" -> "one"),
        Map(
          ":foo" -> fromInt(1),
          ":bar" -> fromString("Hello!"),
          ":one" -> fromInt(1)
        )
      ),
      DynamoDbItemUpdate(
        "foo" -> 1,
        "bar" -> "Hello!",
        "zoo" -> REMOVE,
        "one" -> add(1)
      )
    )
  }

  test("parse update expression with nested paths") {
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "SET #foo[1].#bar[0] = :foo_1_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_1_bar_0" -> fromInt(1))
      ),
      DynamoDbItemUpdate("foo[1].bar[0]" -> 1)
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "REMOVE #foo[0].#bar SET #foo[1].#bar[0] = :foo_1_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_1_bar_0" -> fromInt(1))
      ),
      DynamoDbItemUpdate(
        "foo[1].bar[0]" -> 1,
        "foo[0].bar" -> REMOVE
      )
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "ADD #foo[1].#bar[0] :foo_1_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_1_bar_0" -> fromInt(3))
      ),
      DynamoDbItemUpdate(
        "foo[1].bar[0]" -> add(3)
      )
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "ADD #foo[1].#bar[1] :foo_1_bar_1 REMOVE #foo[3].#bar SET #foo[0].#bar[0] = :foo_0_bar_0",
        Map("#foo" -> "foo", "#bar" -> "bar"),
        Map(":foo_0_bar_0" -> fromInt(1), ":foo_1_bar_1" -> fromInt(3))
      ),
      DynamoDbItemUpdate(
        "foo[0].bar[0]" -> 1,
        "foo[1].bar[1]" -> add(3),
        "foo[3].bar" -> REMOVE
      )
    )
  }

  test("parse update expression with function") {
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "SET #foo = list_append(#foo, :foo)",
        Map("#foo" -> "foo"),
        Map(":foo" -> listOf("a"))
      ),
      DynamoDbItemUpdate("foo" -> appendToList("a"))
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "SET #foo = list_append(:foo, #foo)",
        Map("#foo" -> "foo"),
        Map(":foo" -> listOf("a"))
      ),
      DynamoDbItemUpdate("foo" -> prependToList("a"))
    )
    assertEquals(
      AttributeUpdate.parseUpdateExpression(
        "SET #foo = if_not_exists(#foo, :foo)",
        Map("#foo" -> "foo"),
        Map(":foo" -> fromString("a"))
      ),
      DynamoDbItemUpdate("foo" -> ifNotExists(fromString("a")))
    )
  }

  test("parse real update expression") {
    AttributeUpdate.parseUpdateExpression(
      "SET #zerohash.#velocity_status = :zerohash_velocity_status, #provider = :provider, #lastUpdated = :lastUpdated, #zerohash.#status = :zerohash_status, #status = :status, #events = list_append(:events, #events)",
      Map(
        "#velocity_status" -> "velocity_status",
        "#status" -> "status",
        "#events" -> "events",
        "#provider" -> "provider",
        "#zerohash" -> "zerohash",
        "#lastUpdated" -> "lastUpdated"
      ),
      Map(
        ":zerohash_status" -> fromString("submitted"),
        ":zerohash_velocity_status" -> fromString("approved"),
        ":status" -> fromString("ProviderTransactionSubmitted"),
        ":lastUpdated" -> fromString("submitted"),
        ":provider" -> fromString("zerohash"),
        ":events" -> listOf(
          DynamoDbItem(
            "actor" -> fromString(
              "remittances:ZeroHash:postPaymentAndWaitForStatus"
            ),
            "timestamp" -> fromInt(1717529874),
            "event" -> fromString("status-change")
          )
        ),
        ":zerohash_status" -> fromString("submitted"),
        ":zerohash_status" -> fromString("submitted"),
        ":zerohash_status" -> fromString("submitted")
      )
    )
  }

  test("removeNullOrEmptyValues") {
    assertEquals(
      DynamoDbItem(
        "foo" -> nullAttributeValue,
        "bar" -> "",
        "zoo" -> 5,
        "one" -> None
      ).removeNullOrEmptyValues,
      DynamoDbItem("zoo" -> 5)
    )
  }

  test("ujson.Obj.toDynamoDbItem") {
    assertEquals(
      ujson
        .Obj(
          "foo" -> ujson.Str("bar"),
          "zoo" -> ujson.Num(5.1),
          "one" -> ujson.Bool(true),
          "list" -> ujson.Arr(ujson.Str("a"), ujson.Num(1), ujson.Bool(false)),
          "nested" -> ujson.Obj(
            "a" -> ujson.Num(2.3),
            "b" -> ujson.Str("2")
          )
        )
        .toDynamoDbItem,
      DynamoDbItem(
        "foo" -> "bar",
        "zoo" -> 5.1d,
        "one" -> true,
        "list" -> listOf("a", 1.0, false),
        "nested" -> DynamoDbItem(
          "a" -> 2.3,
          "b" -> "2"
        )
      )
    )
  }

  test("extract tuple from item") {
    val item = DynamoDbItem("a" -> 1, "b" -> "foo", "c" -> false, "m" -> DynamoDbItem("n" -> 1, "o" -> "p"))
    assertEquals(
      obtained = item.maybeTuple[(Int, String)]("a", "b"),
      expected = Some((1, "foo"))
    )
    assertEquals(
      obtained = item.maybeTuple[(Int, String)]("a", "bb"),
      expected = None
    )
    assertEquals(
      obtained = item.maybeTuple[(Int, Option[String])]("a", "bb"),
      expected = Some((1, None))
    )
    assertEquals(
      obtained = item.maybeTuple[(Option[Int], Option[String])]("aa", "bb"),
      expected = Some((None, None))
    )
    assertEquals(
      obtained = item.maybeTuple[(Int, String)]("aa", "b"),
      expected = None
    )
    assertEquals(
      obtained = item.maybeTuple[(Int, String)]("aa", "bb"),
      expected = None
    )
    assertEquals(
      obtained = item.maybeTuple[(Int, Option[DynamoDbItem])]("a", "m"),
      expected = Some((1, Some(DynamoDbItem("n" -> 1, "o" -> "p"))))
    )
    assertEquals(
      obtained = item.eitherTupleOrListMissingKeys[(Int, String)]("a", "b"),
      expected = Right((1, "foo"))
    )
    assertEquals(
      obtained = item.eitherTupleOrListMissingKeys[(Int, Option[String])]("a", "bb"),
      expected = Right((1, None))
    )
    assertEquals(
      obtained = item.eitherTupleOrListMissingKeys[(Int, String)]("aa", "bb"),
      expected = Left(List("aa", "bb"))
    )
    assertEquals(
      obtained = item.eitherTupleOrListMissingKeys[(Option[Int], Option[String])]("aa", "bb"),
      expected = Right((None, None))
    )
  }

  final case class X(name: String, age: Int, isBlond: Boolean)
  final case class Y(name: String, age: Int, height: Option[Int])
  final case class Z(name: Option[String] = None, age: Option[Int] = None, height: Option[Int] = None)

  test("extract class from item") {
    val item = DynamoDbItem("name" -> "Joe", "age" -> 81, "isBlond" -> false)
    assertEquals(item.maybeClass[X], Some(X("Joe", 81, false)))
    assertEquals(item.maybeClass[X], Some(X("Joe", 81, false)))
    assertEquals(item.maybeClass[Y], Some(Y("Joe", 81, None)))
    assertEquals(item.maybeClass[Z], Some(Z(Some("Joe"), Some(81), None)))

    val item2 = DynamoDbItem("firstName" -> "Joe", "age" -> 81, "isBlond" -> false)
    assertEquals(item2.maybeClass[X], None)
    assertEquals(item2.maybeClass[Y], None)
    assertEquals(item2.maybeClass[Z], Some(Z(None, Some(81), None)))
  }

  test("extract class from item - 2") {
    val item = DynamoDbItem("name" -> "Joe", "age" -> 81, "isBlond" -> false)
    assertEquals(item.eitherClassOrListMissingKeys[X], Right(X("Joe", 81, false)))
    assertEquals(item.eitherClassOrListMissingKeys[Y], Right(Y("Joe", 81, None)))
    assertEquals(item.eitherClassOrListMissingKeys[Z], Right(Z(Some("Joe"), Some(81), None)))

    val item2 = DynamoDbItem("firstName" -> "Joe", "age" -> 81, "isBlond" -> false)
    assertEquals(item2.eitherClassOrListMissingKeys[X], Left(List("name")))
    assertEquals(item2.eitherClassOrListMissingKeys[Y], Left(List("name")))
    assertEquals(item2.eitherClassOrListMissingKeys[Z], Right(Z(None, Some(81), None)))
  }

  test("return list of erorrs") {
    val item = DynamoDbItem("a" -> 1, "b" -> "foo", "c" -> false)
    assertEquals(
      obtained = item.eitherTupleOrListMissingKeys[(Int, String, String)]("a", "b1", "cc"),
      expected = Left(List("b1", "cc"))
    )
    assertEquals(
      obtained = item.eitherTupleOrListMissingKeys[(Int, String, String)]("d", "b1", "cc"),
      expected = Left(List("d", "b1", "cc"))
    )
  }
}
