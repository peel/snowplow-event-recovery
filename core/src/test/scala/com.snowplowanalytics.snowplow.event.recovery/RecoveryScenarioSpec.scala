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
package com.snowplowanalytics.snowplow.event.recovery

import cats.implicits._
import org.scalatest.{FreeSpec, Inspectors}
import org.scalatest.Matchers._
import org.scalatest.EitherValues._
import org.scalatestplus.scalacheck._
import org.scalacheck._
import org.scalacheck.ScalacheckShapeless._
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._

import com.snowplowanalytics.snowplow.badrows._
import recoverable.Recoverable.ops._
import config.{Config, Removal, Replacement}

class RecoveryScenarioSpec extends FreeSpec with Inspectors with ScalaCheckPropertyChecks {
  implicit val processorA = implicitly[Arbitrary[Processor]]
  implicit val adapterFailureA = implicitly[Arbitrary[FailureDetails.AdapterFailure]]
  implicit val adapterFailuresA = implicitly[Arbitrary[Failure.AdapterFailures]]
  implicit val collectorPayloadA = implicitly[Arbitrary[Payload.CollectorPayload]]
  implicit val badRowAdapterFailuresA = implicitly[Arbitrary[BadRow.AdapterFailures]]
  implicit val badRowTrackerProtocolViolationsA = implicitly[Arbitrary[BadRow.TrackerProtocolViolations]]

  val anyString = "(?U)^.*$"
  val prefix = "replacement"

  "Recoverable" - {
    "allow matcher-based field content replacement" in {
      forAll { (b: BadRow.AdapterFailures) =>
        val fields = b.payload.getClass.getDeclaredFields.toList.map(_.getName).zipWithIndex.filterNot{case (v, _) => Seq("querystring", "headers").contains(v)}.toMap
        val filteredFieldId = Gen.chooseNum(0, fields.size-1).sample.get
        val fieldId = fields.values.toList(filteredFieldId)
        val field = fields.keys.toList(filteredFieldId)
        val fieldValue = b.payload.productIterator.toList(fieldId)
        val replacement = s"$prefix$field"

        val conf: Config = Map(config.AdapterFailures -> List(Replacement(field, anyString, replacement)))
        val recovered = b.recover(conf)
        recovered should be ('right)
        fieldValue match {
          case Some(_) => recovered.right.value should not equal (b)
          case _: String => recovered.right.value should not equal (b)
          case _ => recovered.right.value should equal (b)
        }

        val revert: Config = fieldValue match {
          case Some(v) => Map(config.AdapterFailures -> List(Replacement(field, anyString, v.toString)))
          case v: String => Map(config.AdapterFailures -> List(Replacement(field, anyString, v)))
          case _ => Map.empty
        }
        val reverted = recovered.right.get.recover(revert)
        reverted should be ('right)
        reverted.right.value should equal (b)
      }
    }
    "allow matcher-based field content removal" in {
      forAll { (b: BadRow.TrackerProtocolViolations) =>
        val field = "vendor" // TODO gen
        val conf: Config = Map(config.TrackerProtocolViolations -> List(Removal(field, anyString)))

        val recovered = b.recover(conf)
        recovered should be ('right)
        recovered.map(_.payload.vendor).right.value shouldEqual ""
      }
    }
    "allow chaining processing steps" in {
      forAll { (b: BadRow.AdapterFailures) =>
        val field = "vendor" // TODO gen
        val replacement = s"$prefix$field"

        val conf: Config = Map(config.AdapterFailures -> List(Replacement(field, anyString, replacement), Removal(field, field)))

        val recovered = b.recover(conf)
        recovered should be ('right)
        recovered.map(_.payload.vendor).right.value shouldEqual prefix
      }
    }
    "mark flows unercoverable" in {
      forAll { (b: BadRow.SizeViolation) =>
        b.recover(Map.empty) should be ('left)
      }
      forAll { (b: BadRow.CPFormatViolation) =>
        b.recover(Map.empty) should be ('left)
      }
    }
  }
}
