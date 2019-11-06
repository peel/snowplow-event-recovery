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
package com.snowplowanalytics
package snowplow
package event.recovery

import java.util.{Base64, UUID}
import java.util.concurrent.TimeUnit

import io.circe.Json
import io.circe.literal._
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.ScalacheckShapeless._
import com.fortysevendeg.scalacheck.datetime.joda.ArbitraryJoda._
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._

import CollectorPayload.thrift.model1.CollectorPayload
import iglu.core.{SchemaKey, SchemaVer}
import iglu.client.resolver.registries.Registry
import iglu.client.resolver.Resolver
import iglu.schemaddl.scalacheck.{IgluSchemas, JsonGenSchema}

import com.snowplowanalytics.snowplow.badrows._

import cats.Id
import cats.data.EitherT
import cats.effect.Clock

object gens {
  implicit val idClock: Clock[Id] = new Clock[Id] {
    final def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

    final def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }

  val qs = (json: Json) => {
    val str = json.toString
    val unstruct = s"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1","data":$str}}"""
    val encoded = Base64.getEncoder.encodeToString(unstruct.getBytes)
    s"e=ue&tv=js&ue_px=$encoded"
  }
  val body = (json: Json) => {
    val str = json.toString
    val unstruct = s"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1","data":$str}}"""
    val encoded = Base64.getEncoder.encodeToString(unstruct.getBytes)
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[{"e":"ue","p":"web","tv":"js","ue_px":"$encoded"}]}"""
  }

  val jsonGen: EitherT[Id, String, Gen[Json]] = for {
    r <- EitherT.right(Resolver.init[Id](0, None, Registry.IgluCentral))
    schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "client_session", "jsonschema", SchemaVer.Full(1, 0, 1))
    schemaJson <- EitherT(IgluSchemas.lookup[Id](r, schemaKey))
    schemaObject <- EitherT.fromEither(IgluSchemas.parseSchema(schemaJson))
  } yield JsonGenSchema.json(schemaObject)

  implicit val collectorPayloadArb: Arbitrary[CollectorPayload] = Arbitrary {
    for {
      ts <- Gen.choose(1, Long.MaxValue)
      path = "/com.snowplowanalytics.snowplow/v1"
      post <- Gen.oneOf(true, false)
      contentType = "application/json; charset=UTF-8"
      json <- jsonGen.value.getOrElse(Gen.const(json"""{}"""))
    } yield {
      val collectorPayload = new CollectorPayload()
      collectorPayload.timestamp = ts
      collectorPayload.path = path
      if (post) collectorPayload.body = body(json)
      else collectorPayload.querystring = qs(json)
      collectorPayload
    }
  }

  implicit def arbUUID: Arbitrary[UUID] = Arbitrary {
    Gen.delay(UUID.randomUUID)
  }
  implicit val processorA = implicitly[Arbitrary[Processor]]
  implicit val notJsonA = implicitly[Arbitrary[FailureDetails.AdapterFailure.NotJson]]
  implicit val inputDataA = implicitly[Arbitrary[FailureDetails.AdapterFailure.InputData]]
  implicit val schemaMappingA = implicitly[Arbitrary[FailureDetails.AdapterFailure.SchemaMapping]]
  implicit val adapterFailureA: Arbitrary[FailureDetails.AdapterFailure] = Arbitrary(Gen.oneOf(notJsonA.arbitrary, inputDataA.arbitrary, schemaMappingA.arbitrary))
  implicit val tpvCriterionMismatchA = implicitly[Arbitrary[FailureDetails.TrackerProtocolViolation.CriterionMismatch]]
  implicit val tpvNotJsonA = implicitly[Arbitrary[FailureDetails.TrackerProtocolViolation.NotJson]]
  implicit val tpvInputDataA = implicitly[Arbitrary[FailureDetails.TrackerProtocolViolation.InputData]]
  implicit val trackerProtocolViolationA: Arbitrary[FailureDetails.TrackerProtocolViolation] = Arbitrary(Gen.oneOf(tpvNotJsonA.arbitrary, tpvInputDataA.arbitrary, tpvCriterionMismatchA.arbitrary))
  implicit val sizeViolationA = implicitly[Arbitrary[Failure.SizeViolation]]
  implicit val collectorPayloadA = implicitly[Arbitrary[Payload.CollectorPayload]]
  implicit val badRowAdapterFailuresA = implicitly[Arbitrary[BadRow.AdapterFailures]]
  implicit val badRowTrackerProtocolViolationsA = implicitly[Arbitrary[BadRow.TrackerProtocolViolations]]
  implicit val badRowSizeViolationA = implicitly[Arbitrary[BadRow.SizeViolation]]
  implicit val badRowcpFormatViolationA = implicitly[Arbitrary[BadRow.CPFormatViolation]]
  implicit val uuidGen: Gen[UUID] = Gen.uuid
}
