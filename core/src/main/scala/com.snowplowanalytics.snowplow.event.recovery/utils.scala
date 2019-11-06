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

import java.util.Base64
import scala.collection.JavaConverters._

import io.circe.Json
import io.circe.parser.{parse => parseJson}
import cats.effect.Clock
import cats._
import cats.data._
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import org.apache.thrift.{TDeserializer, TSerializer}

import config.Config
import config.json._
import com.snowplowanalytics.snowplow.badrows.Payload
import CollectorPayload.thrift.model1.CollectorPayload
import com.snowplowanalytics.iglu.client.Client

object utils {
  /** Deserialize a String into a CollectorPayload after having base64-decoded it. */
  val thriftDeser: String => CollectorPayload = { s =>
    val decoded = Base64.getDecoder.decode(s)
    val thriftDeserializer = new TDeserializer
    val payload = new CollectorPayload
    thriftDeserializer.deserialize(payload, decoded)
    payload
  }

  /** Serialize a CollectorPayload into a byte array and base64-encode it. */
  val thriftSer: CollectorPayload => String = { cp =>
    val thriftSerializer = new TSerializer
    val bytes = thriftSerializer.serialize(cp)
    Base64.getEncoder.encodeToString(bytes)
  }

  /**
   * Decode a base64-encoded string.
   * @param encoded base64-encoded string
   * @return either a successfully decoded string or a failure
   */
  def decodeBase64(encoded: String): Either[String, String] =
    Either.catchNonFatal(new String(Base64.getDecoder.decode(encoded)))
      .leftMap(e => s"Configuration is not properly base64-encoded: ${e.getMessage}")

  def loadConfig(str: String): Either[String, config.Config] =
    parseJson(str)
      .flatMap(_.hcursor.downField("data").get[Config]("recovery"))
      .leftMap(_.show)

  /**
   * Validate that a configuration conforms to its schema.
   * @param configuration in json form
   * @return a failure if the json didn't validate against its schema or a success
    */
  def validateConfiguration[F[_] : Monad : InitSchemaCache : InitListCache : Clock: RegistryLookup](config: String): EitherT[F, String, Unit] = {
    val parse: String => Either[String, Json] = str => parseJson(str).leftMap(_.show)
    val zoomF: (String, String) => EitherT[F, String, Json] = (str, field) => EitherT.fromEither(parse(str).flatMap(_.hcursor.downField("data").downField(field).focus.toRight(s"Missing $field configuration")))
    for {
      resolver <- zoomF(config, "resolver")
      recovery <- zoomF(config, "recovery")
      c <- Client.parseDefault[F](resolver).leftMap(_.show)
      i = SelfDescribingData(SchemaKey("com.snowplowanyltics.snowplow", "recovery_config", "jsonschema", SchemaVer.Full(1,0,0)), recovery)
      _ <- c.check(i).leftMap(_.show)
    } yield ()
  }

  def coerce(payload: Payload): Option[CollectorPayload] = payload match {
    case p: Payload.CollectorPayload => {
      val cp = new CollectorPayload(
        s"iglu:${p.vendor}/CollectorPayload/thrift/${p.version}",
        p.ipAddress.orNull,
        p.timestamp.map(_.getMillis).getOrElse(0),
        p.encoding,
        p.collector
      )
      cp.userAgent = p.useragent.orNull
      cp.refererUri = p.refererUri.orNull
      cp.querystring = Foldable[List].foldMap(p.querystring)(_.value.getOrElse(""))
      cp.body = p.body.orNull
      cp.headers = p.headers.asJava
      cp.contentType = p.contentType.orNull
      cp.hostname = p.hostname.orNull
      cp.networkUserId = p.networkUserId.map(_.toString).orNull
      Some(cp)
    }
    case Payload.EnrichmentPayload(_, p) => {
      val cp = new CollectorPayload(
        s"iglu:${p.vendor}/CollectorPayload/thrift/${p.version}",
        p.ipAddress.orNull,
        p.timestamp.map(_.getMillis).getOrElse(0),
        p.encoding,
        // FIXME
        null
      )
      cp.userAgent = p.useragent.orNull
      cp.refererUri = p.refererUri.orNull
      // FIXME ??? cp.querystring = azFoldable[List].foldMap(p.querystring)(_.value.getOrElse(""))
      // FIXME ??? cp.body = p.body.orNull
      cp.headers = p.headers.asJava
      cp.contentType = p.contentType.orNull
      cp.hostname = p.hostname.orNull
      cp.networkUserId = p.userId.map(_.toString).orNull
      Some(cp)
    }
    case _ => None
  }

}
