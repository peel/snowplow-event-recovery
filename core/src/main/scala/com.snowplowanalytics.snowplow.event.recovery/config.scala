package com.snowplowanalytics.snowplow
package event.recovery

import cats.syntax.functor._
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import io.circe.syntax._
import io.circe.generic.semiauto.{deriveEncoder, deriveDecoder}
import io.circe.generic.extras.semiauto.{deriveEnumerationEncoder, deriveEnumerationDecoder}

object config {
  object Flow {
    val show = (f: Flow) => f.key.toString
    val read = (s: String) => Vector(AdapterFailures).find(_.toString == s)
  }
  sealed trait Flow
  case object AdapterFailures extends Flow

  sealed trait Context
  case object Query extends Context
  case object Body extends Context
  case object Path extends Context

  type Config = Map[Flow, List[StepConfig]]
  type Matcher = String

  sealed trait StepConfig
  case class Replacement(context: Context, matcher: Matcher, replacement: String) extends StepConfig
  case class Removal(context: Context, matcher: Matcher) extends StepConfig

  object json {
    implicit val contextE: Encoder[Context] = deriveEnumerationEncoder[Context]
    implicit val contextD: Decoder[Context] = deriveEnumerationDecoder[Context]
    implicit val replacementE: Encoder[Replacement] = deriveEncoder[Replacement]
    implicit val replacementD: Decoder[Replacement] = deriveDecoder[Replacement]
    implicit val removalE: Encoder[Removal] = deriveEncoder[Removal]
    implicit val removalD: Decoder[Removal] = deriveDecoder[Removal]
    implicit val stepE: Encoder[StepConfig] = Encoder.instance {
      case r: Replacement => r.asJson
      case r: Removal => r.asJson
    }
    implicit val stepD: Decoder[StepConfig] = List[Decoder[StepConfig]] (
      Decoder[Replacement].widen,
      Decoder[Removal].widen
    ).reduceLeft(_ or _)

    implicit val flowKeyE: KeyEncoder[Flow] = new KeyEncoder[Flow]{
      override def apply(flow: Flow): String = Flow.show(flow)
    }
    implicit val flowKeyD: KeyDecoder[Flow] = new KeyDecoder[Flow]{
      override def apply(value: String): Option[Flow] = Flow.read(value)
    }
  }
}
