package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.BadRow._
import com.snowplowanalytics.snowplow.badrows.Payload
import cats.data._
import cats.implicits._
import config._
import config.{AdapterFailures => AdapterFailuresFlow}
import steps._

object recoverable {
  trait Recoverable[A <: BadRow, B <: Payload] { self =>
    def recover(a: A)(config: Config): Either[A, A]
    def payload(a: A): B
    def apply(config: Config, flow: Flow, payload: B)(implicit i: inspectable.Inspectable[B]) =
      config.get(flow)
        .getOrElse(List.empty)
        .map(c => Kleisli(new Modify[B](c).recover))
        .foldLeft(Kleisli(new Precondition[B]().recover) >>>
                    Kleisli(new PassThrough[B]().recover))(_ >>> _)(payload)

  }

  object Recoverable {
    def apply[A <: BadRow, B <: Payload](implicit rs: Recoverable[A, B]): Recoverable[A, B] = rs
    def recover[A <: BadRow, B <: Payload](a: A)(config: Config)(implicit rs: Recoverable[A, B]) = rs.recover(a)(config)
    object ops {
      implicit class RecoverableOps[A <: BadRow, B <: Payload](a: A)(implicit rec: Recoverable[A, B]) {
        def recover(config: Config) = Recoverable[A, B].recover(a)(config)
        def payload = Recoverable[A, B].payload(a)
      }
    }

    implicit val adapterFailuresRecovery: Recoverable[AdapterFailures, Payload.CollectorPayload] =
      new Recoverable[AdapterFailures, Payload.CollectorPayload] {
        override def payload(b: AdapterFailures) = b.payload
        override def recover(b: AdapterFailures)(config: Config) = {
          def update(b: AdapterFailures, p: Payload.CollectorPayload) = b.copy(payload = p)
          apply(config, AdapterFailuresFlow, b.payload) match {
            case Left(p) => Left(update(b, p))
            case Right(p) => Right(update(b, p))
          }
        }
      }
  }
}
