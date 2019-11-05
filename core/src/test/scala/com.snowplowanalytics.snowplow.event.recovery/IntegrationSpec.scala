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

// import java.util.Base64

// import cats.implicits._
// import cats.syntax.either._
// import io.circe.{Decoder, Json}
// import io.circe.generic.extras.auto._
// import io.circe.generic.extras.Configuration
// import io.circe.syntax._
// import org.apache.thrift.TSerializer
// import scalaz._
// import org.json4s._
// import org.json4s.jackson.JsonMethods._
import org.scalatest.{FreeSpec, Inspectors}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
// import org.scalatest.Matchers._
// import org.scalatest.EitherValues._
// import org.scalatest.prop._

// import com.snowplowanalytics.snowplow.badrows._
// import org.scalacheck._
// import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
// import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
// import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
// import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
// import com.snowplowanalytics.iglu.client.Resolver

// import recoverable.Recoverable.ops._
// import config.{Config, Removal, Replacement}

// import org.scalacheck.ScalacheckShapeless._
// import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._

class IntegrationSpec extends FreeSpec with Inspectors with ScalaCheckPropertyChecks {
//   implicit val processorA = implicitly[Arbitrary[Processor]]
//   implicit val adapterFailureA = implicitly[Arbitrary[FailureDetails.AdapterFailure]]
//   implicit val adapterFailuresA = implicitly[Arbitrary[Failure.AdapterFailures]]
//   implicit val collectorPayloadA = implicitly[Arbitrary[Payload.CollectorPayload]]

//   implicit def eitherDecoder[L, R](implicit l: Decoder[L], r: Decoder[R]): Decoder[Either[L, R]] =
//     l.either(r)
//   implicit val genConfig: Configuration =
//     Configuration.default.withDiscriminator("name")

//   val badRows = {
//     val br = io.circe.parser.parse(getResourceContent("/bad_rows.json"))
//       .flatMap(_.as[List[BadRow]])
//       .fold(f => throw new Exception(s"invalid bad rows: ${f.getMessage}"), identity)
//     println("Decoded bad rows are:")
//     println(br.map(r => r.copy(line = thriftDeser(r.line).toString)).asJson)
//     br
//   }
//   val recoveryScenarios = io.circe.parser.parse(getResourceContent("/recovery_scenarios.json"))
//     .flatMap(_.hcursor.get[List[RecoveryScenario]]("data"))
//     .fold(f => throw new Exception(s"invalid recovery scenarios: ${f.getMessage}"), identity)
//   val expectedPayloads = io.circe.parser.parse(getResourceContent("/expected_payloads.json"))
//     .flatMap(_.as[List[ExpectedPayload]])
//     .fold(f => throw new Exception(s"invalid expected payloads: ${f.getMessage}"), identity)
//     .map { p =>
//       val cp = new CollectorPayload()
//       cp.schema = "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0"
//       cp.encoding = "UTF-8"
//       cp.collector = "c"
//       cp.path = p.path
//       p match {
//         case QueryString(_, qs) => cp.querystring = qs
//         case Body(_, b) =>
//           cp.contentType = "application/json; charset=UTF-8"
//           cp.body = b.noSpaces
//       }
//       cp
//     }

//   val resolver = Resolver.parse(parse(getResourceContent("/resolver.json")))
//     .fold(l => throw new Exception(s"invalid resolver: $l"), identity)
//   val registry = EnrichmentRegistry
//     .parse(parse(getResourceContent("/enrichments.json")), true)(resolver)
//     .fold(l => throw new Exception(s"invalid registry: $l"), identity)

  "IntegrationSpec" in {

//     // filter -> check expected payloads counts
//     val filtered = badRows.filter(_.isAffected(recoveryScenarios))
//     filtered.size shouldEqual expectedPayloads.size

//     // mutate -> check expected payloads
//     val mutated = filtered.map(_.mutateCollectorPayload(recoveryScenarios))
//     mutated.size shouldEqual expectedPayloads.size
//     // check only the modifiable fields
//     mutated.map(cp => (cp.path, cp.querystring, cp.body)) shouldEqual
//       expectedPayloads.map(cp => (cp.path, cp.querystring, cp.body))

//     // check enrich
//     val enriched = mutated
//       .map { payload =>
//         val thriftSerializer = new TSerializer
//         val bytes = thriftSerializer.serialize(payload)
//         val res = Base64.getEncoder.encode(bytes)
//         val r = ThriftLoader.toCollectorPayload(bytes)
//         r
//       }
//       .map(payload => EtlPipeline.processEvents(
//         registry,
//         "etlVersion",
//         org.joda.time.DateTime.now,
//         payload)(resolver)
//       )
//       .flatten
//     forAll (enriched) { r =>
//       val e = r match {
//         case Success(e) => Right(e)
//         case Failure(e) => Left(e)
//       }
//       e should be ('right)
//     }
//   }

//     forAll { (b: BadRow.AdapterFailures) =>
//       val field = "vendor" // gen
//       val prefix = "replacement" // gen
//       val replacement = s"$prefix$field"

//       val conf: Config = Map(config.AdapterFailures -> List(Replacement(field, "^(.*)", replacement), Removal(field, field)))

//       val recovered = b.recover(conf)
//       recovered should be ('right)
//       recovered.map(_.payload.vendor).right.value shouldEqual prefix
//     }

//   private def getResourceContent(resource: String): String = {
//     val f = getClass.getResource(resource).getFile
//     val s = scala.io.Source.fromFile(f)
//     val c = s.mkString
//     s.close()
//     c
  }
}
