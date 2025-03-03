package org.encalmo.aws

class MacrosSpec extends TestSuite {

  test("same size") {
    assertEquals(
      obtained = Macros.sameSize[(String, Int)]("a", "b"),
      expected = true
    )
    assertEquals(
      obtained = Macros.sameSize[(String, Int)]("a"),
      expected = false
    )
    assertEquals(
      obtained = Macros.sameSize[(String, Int, Int)]("a", "b"),
      expected = false
    )
  }

  test("size of varargs") {
    assertEquals(
      obtained = Macros.sizeOfVarargs(),
      expected = 0
    )
    assertEquals(
      obtained = Macros.sizeOfVarargs("a"),
      expected = 1
    )
    assertEquals(
      obtained = Macros.sizeOfVarargs("a", "b"),
      expected = 2
    )
    assertEquals(
      obtained = Macros.sizeOfVarargs("a", "b", "c"),
      expected = 3
    )
  }
}
