package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows._
import cats.implicits._
import com.github.reugn.dynamic._

// import inspectable._
// import inspectable.Inspectable.ops._
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

  val Body = "body"
  val Query = "querystring"

  // TODO it should be possible to abstract the two with shapeless, but somehow instance derivation fails
  class ModifyCollectorPayload(config: StepConfig) extends Step[Payload.CollectorPayload] {
    val recover: Payload.CollectorPayload => Either[Payload.CollectorPayload, Payload.CollectorPayload] = a => config match {
      case Replacement(Body, matcher, replacement) =>
        Right(a.copy(body=replacement.some))
      case Replacement(Query, matcher, replacement) =>
        Right(a.copy(querystring=List(NVP(replacement, replacement.some))))
      case Removal(Body, matcher) =>
        Right(a.copy(body=None))
      case Removal(Query, matcher) =>
        Right(a.copy(querystring=List.empty))
      case _ =>
        Left(a)
    }
  }

  class ModifyEnrichmentPayload(config: StepConfig) extends Step[Payload.EnrichmentPayload] {
    val recover: Payload.EnrichmentPayload => Either[Payload.EnrichmentPayload, Payload.EnrichmentPayload] = a => config match {
      case Replacement(context, matcher, replacement) =>
        // FIXME update values
        Right(a.copy(rawEvent=a.rawEvent))
      case Removal(context, matcher) =>
        // FIXME remove values
        Either.catchNonFatal(copy(a, context, "")) match {
          case Right(m) => Right(m)
          case _ => Left(a)
        }
      case _ =>
        Left(a)
    }
  }

  // TODO this was a great idea but does not work due to type class instances derivation issue (maybe 2.13 will solve this)
  // import modify.Replace
  // import modify.Replace.ops._
  // class Modify[A <: Payload : Replace](config: StepConfig) extends Step[A] {
  //   val recover: A => Either[A, A] = a => config match {
  //     case Replacement(context, matcher, replacement) =>
  //       Right(a.replace(context)(_.toUpperCase)(Replace("body")))
  //     case Removal(context, matcher) =>
  //       Right(a.replace(context)(_ => ""))
  //     case _ =>
  //       Left(a)
  //   }
  // }
}
