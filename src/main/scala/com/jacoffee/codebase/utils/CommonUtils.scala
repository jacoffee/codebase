package com.jacoffee.codebase.utils

import scala.util.{Failure, Success, Try}

object CommonUtils {

  def safeRelease[S <: AutoCloseable, R](resource: S)(handler: S => R)(
      cleanUp: S => Unit = (s: S) => s.close
  ): Try[R] = {
    try {
      Success(handler(resource))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      if (resource != null) {
        cleanUp(resource)
      }
    }
  }

}
