/*
 * Copyright (c) 2018-2018 Snowplow Analytics Ltd. All rights reserved.
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

import org.scalatest.{FreeSpec, EitherValues}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.ScalacheckShapeless._
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._

import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.functions.ProcessFunction
import typeinfo._
import org.mockito.Mockito._

import com.snowplowanalytics.snowplow.badrows._

class SplitByStatusSpec extends FreeSpec with ScalaCheckPropertyChecks with EitherValues with MockitoSugar {
  implicit val processorA = implicitly[Arbitrary[Processor]]
  implicit val adapterFailureA = implicitly[Arbitrary[FailureDetails.AdapterFailure]]
  implicit val adapterFailuresA = implicitly[Arbitrary[Failure.AdapterFailures]]
  implicit val collectorPayloadA = implicitly[Arbitrary[Payload.CollectorPayload]]
  implicit val badRowAdapterFailuresA = implicitly[Arbitrary[BadRow.AdapterFailures]]
  implicit val eitherBRorPayload = Gen.either(badRowAdapterFailuresA.arbitrary, collectorPayloadA.arbitrary)

  "SplitByStatus" - {
    "should separate recovery success from failure" in {
      forAll { (b: Either[BadRow.AdapterFailures, Payload.CollectorPayload]) =>
        val tag = OutputTag[String]("failed")
        val fn = new SplitByStatus(tag)
        val collector = mock[Collector[Payload]]
        val ctx = mock[ProcessFunction[Either[BadRow, Payload], Payload]#Context]
        fn.processElement(b, ctx, collector)
        if (b.isRight) verify(collector).collect(b.right.get)
      }
    }
  }
}
