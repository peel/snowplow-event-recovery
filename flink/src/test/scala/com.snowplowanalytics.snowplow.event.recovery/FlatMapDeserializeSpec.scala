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
import org.scalacheck.Arbitrary
import org.scalacheck.ScalacheckShapeless._
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._

import org.apache.flink.util.Collector
import io.circe.syntax._
import org.mockito.Mockito._

import com.snowplowanalytics.snowplow.badrows._

class FlatMapDeserializeSpec extends FreeSpec with ScalaCheckPropertyChecks with EitherValues with MockitoSugar {
  implicit val processorA = implicitly[Arbitrary[Processor]]
  implicit val adapterFailureA = implicitly[Arbitrary[FailureDetails.AdapterFailure]]
  implicit val adapterFailuresA = implicitly[Arbitrary[Failure.AdapterFailures]]
  implicit val collectorPayloadA = implicitly[Arbitrary[Payload.CollectorPayload]]
  implicit val badRowAdapterFailuresA = implicitly[Arbitrary[BadRow.AdapterFailures]]

  "FlatMapDeserialize" - {
    "should deserialize json messages to bad row objects" in {
      forAll { (b: BadRow.AdapterFailures) =>
        val collector = mock[Collector[BadRow]]
        FlatMapDeserialize.flatMap(b.asJson.toString, collector)
        verify(collector).collect(b)
      }
    }
  }
}
