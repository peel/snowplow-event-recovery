/*
 * Copyright (c) 2018-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.BadRow._
import com.snowplowanalytics.snowplow.badrows.Payload
import cats.data._
import cats.implicits._
import config._
import config.{
  AdapterFailures => AdapterFailuresFlow,
  TrackerProtocolViolations => TrackerProtocolViolationsFlow,
  SchemaViolations => SchemaViolationsFlow,
  EnrichmentFailures => EnrichmentFailuresFlow
}
import steps._
import inspectable.Inspectable._

object recoverable {
  trait Recoverable[A <: BadRow, B <: Payload] { self =>
    def recover(a: A)(config: Config): Either[A, A]
    def payload(a: A): Option[B]
  }

  object Recoverable {
    def apply[A <: BadRow, B <: Payload](implicit r: Recoverable[A, B]): Recoverable[A, B] = r

    def step[B <: Payload](config: Config, flow: Flow, payload: B)(mkStep: StepConfig => Step[B]): Either[B, B] =
      config.get(flow)
        .getOrElse(List.empty)
        .map(c => Kleisli(mkStep(c).recover))
        .foldLeft(Kleisli(new Precondition[B]().recover) >>>
                    Kleisli(new PassThrough[B]().recover))(_ >>> _)(payload)

    def recover[A <: BadRow, B <: Payload](a: A)(config: Config)(implicit rs: Recoverable[A, B]) = rs.recover(a)(config)
    object ops {
      implicit class RecoverableOps[A <: BadRow, B <: Payload](a: A)(implicit rec: Recoverable[A, B]) {
        def recover(config: Config) = Recoverable[A, B].recover(a)(config)
        def payload = Recoverable[A, B].payload(a)
      }
    }
    implicit val sizeViolationRecovery: Recoverable[SizeViolation, Payload.RawPayload] = unrecoverable
    implicit val cpFormatViolationRecovery: Recoverable[CPFormatViolation, Payload.RawPayload] = unrecoverable

    // TODO can we generate?
    implicit val adapterFailuresRecovery: Recoverable[AdapterFailures, Payload.CollectorPayload] =
      new Recoverable[AdapterFailures, Payload.CollectorPayload] {
        override def payload(b: AdapterFailures) = b.payload.some
        override def recover(b: AdapterFailures)(config: Config) = {
          def update(b: AdapterFailures)(p: Payload.CollectorPayload) = b.copy(payload = p)
          step(config, AdapterFailuresFlow, b.payload)(new Modify[Payload.CollectorPayload](_)).bimap(update(b), update(b))
        }
      }

    implicit val trackerProtocolViolationsRecovery: Recoverable[TrackerProtocolViolations, Payload.CollectorPayload] =
      new Recoverable[TrackerProtocolViolations, Payload.CollectorPayload] {
        override def payload(b: TrackerProtocolViolations) = b.payload.some
        override def recover(b: TrackerProtocolViolations)(config: Config) = {
          def update(b: TrackerProtocolViolations)(p: Payload.CollectorPayload) = b.copy(payload = p)
          step(config, TrackerProtocolViolationsFlow, b.payload)(new Modify[Payload.CollectorPayload](_)).bimap(update(b), update(b))
        }
      }

    implicit val schemaViolationsRecovery: Recoverable[SchemaViolations, Payload.EnrichmentPayload] =
      new Recoverable[SchemaViolations, Payload.EnrichmentPayload] {
        override def payload(b: SchemaViolations) = b.payload.some
        override def recover(b: SchemaViolations)(config: Config) = {
          def update(b: SchemaViolations)(p: Payload.EnrichmentPayload) = b.copy(payload = p)
          step(config, SchemaViolationsFlow, b.payload)(new Modify[Payload.EnrichmentPayload](_)).bimap(update(b), update(b))
        }
      }

    implicit val enrichmentFailuresRecovery: Recoverable[EnrichmentFailures, Payload.EnrichmentPayload] = new Recoverable[EnrichmentFailures, Payload.EnrichmentPayload] {
      override def payload(b: EnrichmentFailures) = b.payload.some
      override def recover(b: EnrichmentFailures)(config: Config) = {
        def update(b: EnrichmentFailures)(p: Payload.EnrichmentPayload) = b.copy(payload = p)
        step(config, EnrichmentFailuresFlow, b.payload)(new Modify[Payload.EnrichmentPayload](_)).bimap(update(b), update(b))
      }
    }

    private[this] def unrecoverable[A <: BadRow, B <: Payload] = new Recoverable[A, B] {
      override def payload(a: A) = None
      override def recover(a: A)(c: Config) = Left(a)
    }
  }
}
