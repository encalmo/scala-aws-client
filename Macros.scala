package org.encalmo.aws

import scala.quoted.{Quotes, *}

object Macros {

  inline transparent def sizeOfVarargs(inline args: String*): Int = ${ sizeOfVarargsImpl('args) }

  def sizeOfVarargsImpl(args: Expr[Seq[String]])(using Quotes): Expr[Int] =
    val size: Int = args match {
      case Varargs(sc) => sc.size
      case _           => 0
    }
    Expr(size)

  inline transparent def sameSize[Tup <: Tuple](inline args: String*): Boolean =
    ${ sameSizeImpl[Tup]('args) }

  def sameSizeImpl[Tup <: Tuple: Type](args: Expr[Seq[String]])(using Quotes): Expr[Boolean] = {
    val size1: Int = args match {
      case Varargs(sc) => sc.size
      case other =>
        quotes.reflect.report.errorAndAbort("Expected varargs literal but got collection reference")
    }
    def sizeOfTuple[A: Type]: Int = Type.of[A] match
      case '[field *: fields] => 1 + sizeOfTuple[fields]
      case _                  => 0

    val result = size1 == sizeOfTuple[Tup]
    Expr(result)
  }

  inline transparent def assertSameSize[Tup <: Tuple](inline args: String*): Unit =
    ${ assertSameSizeImpl[Tup]('args) }

  def assertSameSizeImpl[Tup <: Tuple: Type](args: Expr[Seq[String]])(using Quotes): Expr[Boolean] = {
    val size1: Int = args match {
      case Varargs(sc) => sc.size
      case other =>
        quotes.reflect.report.errorAndAbort("Expected varargs literal but got collection reference")
    }
    def sizeOfTuple[A: Type]: Int = Type.of[A] match
      case '[field *: fields] => 1 + sizeOfTuple[fields]
      case _                  => 0

    val expectedSize = sizeOfTuple[Tup]
    if (size1 == expectedSize) then Expr(true)
    else {
      quotes.reflect.report.errorAndAbort(
        s"Number of keys $size1 must be equal to the expected result size $expectedSize"
      )
    }
  }

  inline transparent def assertSameSize[Tup1 <: Tuple, Tup2 <: Tuple]: Unit =
    ${ assertSameSizeImpl[Tup1, Tup2] }

  def assertSameSizeImpl[Tup1 <: Tuple: Type, Tup2 <: Tuple: Type](using Quotes): Expr[Boolean] = {
    def sizeOfTuple[A: Type]: Int = Type.of[A] match
      case '[field *: fields] => 1 + sizeOfTuple[fields]
      case _                  => 0

    val tup1Size = sizeOfTuple[Tup1]
    val tup2Size = sizeOfTuple[Tup2]

    if (tup1Size == tup2Size) then Expr(true)
    else {
      quotes.reflect.report.errorAndAbort(
        s"Provided tuples have different sizes: $tup1Size and $tup2Size respectively"
      )
    }
  }

}
