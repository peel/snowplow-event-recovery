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

import java.util.UUID
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import com.snowplowanalytics.snowplow.badrows._
import config.{Context, Matcher}
import cats.implicits._
import monocle.function.At.at
import monocle.macros.syntax.lens._

object inspectable {
  object Inspectable {
    trait Inspectable[A <: Payload] {
      def replace(a: A)(ctx: Context, matcher: String, replacement: String): Either[A, A]
      def remove(a: A)(ctx: Context, matcher: String): Either[A, A] = replace(a)(ctx, matcher, "")
    }
    def apply[A <: Payload](implicit i: Inspectable[A]): Inspectable[A] = i
    object ops {
      implicit class InspectableOps[A <: Payload: Inspectable](a: A){
        def replace(ctx: Context, matcher: String, replacement: String) = Inspectable[A].replace(a)(ctx, matcher, replacement)
        def remove(ctx: Context, matcher: String) = Inspectable[A].remove(a)(ctx, matcher)
      }
    }

    implicit val collectorPayloadInspectable: Inspectable[Payload.CollectorPayload] =
      new Inspectable[Payload.CollectorPayload] {
        override def replace(p: Payload.CollectorPayload)(context: Context, matcher: Matcher, replacement: String) = {
          extractContext(context).headOption.toRight(p) flatMap {
            case "vendor" => p.lens(_.vendor).modify(fix(matcher, replacement)).asRight
            case "version" => p.lens(_.version).modify(fix(matcher, replacement)).asRight
            case "querystring" => p.lens(_.querystring).modify(_.map(_.lens(_.value).modify(_.map(fix(matcher, replacement))))).asRight
            case "contentType" => p.lens(_.contentType).modify(_.map(fix(matcher, replacement))).asRight
            case "body" => p.lens(_.body).modify(_.map(fix(matcher, replacement))).asRight
            case "collector" => p.lens(_.collector).modify(fix(matcher, replacement)).asRight
            case "encoding" => p.lens(_.encoding).modify(fix(matcher, replacement)).asRight
            case "hostname" => p.lens(_.hostname).modify(_.map(fix(matcher, replacement))).asRight
            case "timestamp" => p.lens(_.timestamp).modify(_.map(fixDate(matcher, replacement))).asRight
            case "ipAddress" => p.lens(_.ipAddress).modify(_.map(fix(matcher, replacement))).asRight
            case "useragent" => p.lens(_.useragent).modify(_.map(fix(matcher, replacement))).asRight
            case "refererUri" => p.lens(_.refererUri).modify(_.map(fix(matcher, replacement))).asRight
            case "headers" => p.lens(_.headers).modify(_.map(fix(matcher, replacement))).asRight
            case "networkUserId" => p.lens(_.networkUserId).modify(_.map(fixUuid(matcher, replacement))).asRight
            case _ => Left(p)
          }
        }
          
      }

    implicit val enrichmentPayloadInspectable: Inspectable[Payload.EnrichmentPayload] =
      new Inspectable[Payload.EnrichmentPayload] {
        override def replace(p: Payload.EnrichmentPayload)(context: Context, matcher: String, replacement: String) = {
          val failed = Left(p)
          (for {
             top <- extractSegment(context, 1).toRight(p).recoverWith{ case _ => failed}
             bottom <- extractSegment(context, 2).toRight(p).recoverWith{ case _ => failed}
          } yield (top, bottom)).flatMap {
            case ("raw", "vendor") => p.lens(_.raw.vendor).modify(fix(matcher, replacement)).asRight
            case ("raw", "version") => p.lens(_.raw.version).modify(fix(matcher, replacement)).asRight
            case ("raw", "parameters") => extractSegment(context, 3).toRight(p).flatMap { v =>
              p.lens(_.raw.parameters).composeLens(at(v)).modify(_.map(fix(matcher, replacement))).asRight
            }
            case ("raw", "contentType") => p.lens(_.raw.contentType).modify(_.map(fix(matcher, replacement))).asRight
            case ("raw", "loaderName") => p.lens(_.raw.loaderName).modify(fix(matcher, replacement)).asRight
            case ("raw", "encoding") => p.lens(_.raw.encoding).modify(fix(matcher, replacement)).asRight
            case ("raw", "hostname") => p.lens(_.raw.hostname).modify(_.map(fix(matcher, replacement))).asRight
            case ("raw", "timestamp") => p.lens(_.raw.timestamp).modify(_.map(fixDate(matcher, replacement))).asRight
            case ("raw", "ipAddress") => p.lens(_.raw.ipAddress).modify(_.map(fix(matcher, replacement))).asRight
            case ("raw", "useragent") => p.lens(_.raw.useragent).modify(_.map(fix(matcher, replacement))).asRight
            case ("raw", "refererUri") => p.lens(_.raw.refererUri).modify(_.map(fix(matcher, replacement))).asRight
            case ("raw", "headers") => p.lens(_.raw.headers).modify(_.map(fix(matcher, replacement))).asRight
            case ("raw", "userId") => p.lens(_.raw.userId).modify(_.map(fixUuid(matcher, replacement))).asRight
            case _ => failed
          }
        }
      }
  }
  private[this] def extractContext(stringContext: String) = stringContext.split('.')
  private[this] def extractSegment(stringContext: String, n: Int) = Either.catchNonFatal(extractContext(stringContext)(n)).toOption
  def fix(matcher: String, replacement: String)(str: String) = matcher.r.replaceAllIn(str, replacement)
  def fixDate(matcher: String, replacement: String)(dt: DateTime) = {
    val str = dt.toString(ISODateTimeFormat.basicDateTime())
    val replaced = fix(matcher, replacement)(str)
    Either.catchNonFatal(ISODateTimeFormat.dateTimeParser.parseDateTime(replaced)).getOrElse(dt)
  }
  def fixUuid(matcher: String, replacement: String)(u: UUID) = {
    val str = u.toString
    val replaced = fix(matcher, replacement)(str)
    Either.catchNonFatal(UUID.fromString(replaced)).getOrElse(u)
  }

}
