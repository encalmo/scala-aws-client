package org.encalmo.aws

trait ErrorContext {

  type Error

  def apply(errorCode: String, errorMessage: String): Error
  def from(t: Throwable): Error
}

object DefaultErrorContext extends ErrorContext {

  case class Error(errorCode: String, errorMessage: String)

  def apply(errorCode: String, errorMessage: String): Error =
    Error.apply(errorCode, errorMessage)

  def from(t: Throwable): Error =
    Error(t.getClass().getSimpleName(), t.getMessage())

}
