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

import org.scalatest.{FreeSpec, EitherValues}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen

import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.functions.ProcessFunction
import typeinfo._
import org.mockito.Mockito._

import com.snowplowanalytics.snowplow.badrows._
import gens._

class SplitByStatusSpec extends FreeSpec with ScalaCheckPropertyChecks with EitherValues with MockitoSugar {
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
