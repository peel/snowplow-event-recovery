package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows._

import config._
import inspectable.Inspectable._
import inspectable.Inspectable.ops._

object steps {
  sealed trait Step[A <: Payload] {
    val recover: A => Either[A, A]
  }

  class Precondition[A <: Payload] extends Step[A] {
    val recover: A => Either[A, A] = a => Right(a)
  }

  class PassThrough[A <: Payload] extends Step[A] {
    val recover: A => Either[A, A] = a => Right(a)
  }

  class Modify[A <: Payload : Inspectable](config: StepConfig) extends Step[A] {
    val recover: A => Either[A, A] = a => config match {
      case Replacement(context, matcher, replacement) =>
        a.replace(context, matcher, replacement)
      case Removal(context, matcher) =>
        a.remove(context, matcher)
      case _ =>
        Left(a)
    }
  }
}
