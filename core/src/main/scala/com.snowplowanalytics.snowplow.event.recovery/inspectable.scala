package com.snowplowanalytics.snowplow
package event.recovery

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
          def fix(str: String) = matcher.r.replaceAllIn(str, replacement)
          extractContext(context).headOption.toRight(p) flatMap {
            case "vendor" => p.lens(_.vendor).modify(fix).asRight
            case "version" => p.lens(_.version).modify(fix).asRight
            case "querystring" => p.lens(_.querystring).modify(_.map(_.lens(_.value).modify(_.map(fix)))).asRight
            case "contentType" => p.lens(_.contentType).modify(_.map(fix)).asRight
            case "body" => p.lens(_.body).modify(_.map(fix)).asRight
            case "collector" => p.lens(_.collector).modify(fix).asRight
            case "encoding" => p.lens(_.encoding).modify(fix).asRight
            case "hostname" => p.lens(_.hostname).modify(_.map(fix)).asRight
            case "timestamp" => p.lens(_.timestamp).modify(_.map(fix)).asRight
            case "ipAddress" => p.lens(_.ipAddress).modify(_.map(fix)).asRight
            case "useragent" => p.lens(_.useragent).modify(_.map(fix)).asRight
            case "refererUri" => p.lens(_.refererUri).modify(_.map(fix)).asRight
            case "headers" => p.lens(_.headers).modify(_.map(fix)).asRight
            case "networkUserId" => p.lens(_.networkUserId).modify(_.map(fix)).asRight
            case _ => Left(p)
          }
        }
          
      }

    implicit val enrichmentPayloadInspectable: Inspectable[Payload.EnrichmentPayload] =
      new Inspectable[Payload.EnrichmentPayload] {
        override def replace(p: Payload.EnrichmentPayload)(context: Context, matcher: String, replacement: String) = {
          val failed = Left(p)
          // TODO remove patmat for >2.11
          (for {
           top <- extractSegment(context, 1).toRight(p) match {
             case Right(r) => Right(r)
             case Left(_) => failed
            }
            bottom <- extractSegment(context, 2).toRight(p) match {
             case Right(r) => Right(r)
             case Left(_) => failed
            }
          } yield (top, bottom)).flatMap {
            case ("rawEvent", "vendor") => p.lens(_.rawEvent.vendor).modify(_.replaceAll(matcher, replacement)).asRight
            case ("rawEvent", "version") => p.lens(_.rawEvent.version).modify(_.replaceAll(matcher, replacement)).asRight
            case ("rawEvent", "parameters") => extractSegment(context, 3).toRight(p).flatMap { v =>
              p.lens(_.rawEvent.parameters).composeLens(at(v)).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            }
            case ("rawEvent", "contentType") => p.lens(_.rawEvent.contentType).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case ("rawEvent", "loaderName") => p.lens(_.rawEvent.loaderName).modify(_.replaceAll(matcher, replacement)).asRight
            case ("rawEvent", "encoding") => p.lens(_.rawEvent.encoding).modify(_.replaceAll(matcher, replacement)).asRight
            case ("rawEvent", "hostname") => p.lens(_.rawEvent.hostname).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case ("rawEvent", "timestamp") => p.lens(_.rawEvent.timestamp).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case ("rawEvent", "ipAddress") => p.lens(_.rawEvent.ipAddress).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case ("rawEvent", "useragent") => p.lens(_.rawEvent.useragent).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case ("rawEvent", "refererUri") => p.lens(_.rawEvent.refererUri).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case ("rawEvent", "headers") => p.lens(_.rawEvent.headers).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case ("rawEvent", "userId") => p.lens(_.rawEvent.userId).modify(_.map(_.replaceAll(matcher, replacement))).asRight
            case _ => failed
          }
        }
      }
  }

  private[this] def extractContext(stringContext: String) = stringContext.split('.')
  private[this] def extractSegment(stringContext: String, n: Int) = Either.catchNonFatal(extractContext(stringContext)(n)).toOption

}
