package com.snowplowanalytics.snowplow
package event.recovery

import cats.syntax.functor._
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import io.circe.syntax._
import io.circe.generic.semiauto.{deriveEncoder, deriveDecoder}

object config {
  object Flow {
    val show = (f: Flow) => f.key.toString
    val read = (s: String) => Vector(AdapterFailures).find(_.toString == s)
  }
  sealed trait Flow
  case object AdapterFailures extends Flow
  case object TrackerProtocolViolations extends Flow
  case object SchemaViolations extends Flow
  case object EnrichmentFailures extends Flow

  case class Conf(schema: String, data: Config)
  type Config = Map[Flow, List[StepConfig]]
  type Matcher = String
  type Context = String

  sealed trait StepConfig
  case class Replacement(context: Context, matcher: Matcher, replacement: String) extends StepConfig
  case class Removal(context: Context, matcher: Matcher) extends StepConfig

  object json {
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
    implicit val confD: Decoder[Conf] = deriveDecoder[Conf]
    implicit val confE: Encoder[Conf] = deriveEncoder[Conf]
  }
}
