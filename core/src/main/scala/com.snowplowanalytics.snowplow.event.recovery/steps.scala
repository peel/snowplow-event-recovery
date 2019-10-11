package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows._
import cats.implicits._

import inspectable._
import inspectable.Inspectable.ops._
import config._

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
      case Replacement(Body, matcher, replacement) =>
        Right(a.body.set(replacement.some)(a))
      case Replacement(Query, matcher, replacement) =>
        Right(a.query.set(List(NVP(replacement, replacement.some)))(a))
      case Removal(Body, matcher) =>
        Right(a.body.set(None)(a))        
      case Removal(Query, matcher) =>
        Right(a.query.set(List.empty)(a))
      case _ =>
        Left(a)
    }
  }
}
