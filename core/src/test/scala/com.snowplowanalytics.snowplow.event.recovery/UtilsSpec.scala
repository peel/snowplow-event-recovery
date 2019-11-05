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
import org.scalatest.Matchers._
import org.scalatest.prop.PropertyChecks

import CollectorPayload.thrift.model1.CollectorPayload
import gens._

// import RecoveryScenario._
import utils._

class UtilsSpec extends FreeSpec with PropertyChecks with EitherValues {
  "thriftSerDe" - {
    "should deserialize any collector payload" in {
      forAll { (cp: CollectorPayload) =>
        val oldCp = new CollectorPayload(cp)
        val newCp = (thriftSer andThen thriftDeser)(cp)
        oldCp shouldEqual newCp
      }
    }
  }

  "decodeBase64" - {
    "should successfully decode base64" in {
      decodeBase64("YWJjCg==") shouldEqual Right("abc\n")
    }
    "should send an error message if not base64" in {
      decodeBase64("é").left.value should include("Configuration is not properly base64-encoded")
    }
  }

  val json = """{"schema":"iglu:com.snowplowanalytics/recovery_config/jsonschema/1-0-0","data":{"resolver":{"schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1","data":{"cacheSize":5,"repositories":[{"name":"Iglu Central","priority":0,"vendorPrefixes":["com.snowplowanalytics"],"connection":{"http":{"uri":"http://iglucentral.com"}}}]}},"recovery":{"AdapterFailures":[{"context":"body","matcher":"body","replacement":"new-body"},{"context":"query","matcher":"query","replacement":"new-query"}]}}}"""

  "validateConfiguration" - {
    "should successfully validate a properly schemad json" in {
      validateConfiguration(json).value shouldEqual Right(())
    }
    "should fail when validating something that is not json" in {
      validateConfiguration("abc").value.left.value should include("ParsingFailure: expected json value got 'abc' (line 1, column 1)")
    }
    "should fail when validating something that is not according to schema" in {
      val json = """{"schema":"iglu:com.snowplowanalytics/recovery_config/jsonschema/1-0-0","data":{"resolver":{},"recovery":{"AdapterFailure":[{"context":"body","matcher":"body","replacement":"new-body"},{"context":"query","matcher":"query","replacement":"new-query"}]}}}"""
      validateConfiguration(json).value.left.value should include("DecodingFailure at : schema key is not available")
    }
  }
}
