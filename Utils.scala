package org.encalmo.aws

import upickle.default.{ReadWriter, writeJs}

object Utils {

  extension [T](value: Either[Throwable, T])
    inline def eitherErrorOrUnit(using error: ErrorContext): Either[error.Error, Unit] =
      value
        .map(_ => ())
        .left
        .map(error.from)

    inline def eitherErrorOrResult(using error: ErrorContext): Either[error.Error, T] =
      value.left.map(error.from)

    inline def eitherErrorOr[R](result: R)(using error: ErrorContext): Either[error.Error, R] =
      value.map(_ => result).left.map(error.from)

  extension [T](using error: ErrorContext)(value: Either[Throwable, Either[error.Error, T]])
    inline def eitherErrorOrResultFlatten: Either[error.Error, T] =
      value.left
        .map(error.from)
        .flatten

  extension [T: ReadWriter](entity: T)
    inline def writeAsJson: ujson.Value = writeJs(entity)
    inline def writeAsJsonObject: ujson.Obj =
      writeJs(entity).asInstanceOf[ujson.Obj]
    inline def writeAsString: String = ujson.write(writeJs(entity))
}
