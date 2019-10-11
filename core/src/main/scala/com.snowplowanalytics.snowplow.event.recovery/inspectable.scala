package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows.{NVP, Payload}

import monocle.Lens
import monocle.macros._

object inspectable {
  trait Inspectable[A <: Payload] {
    def body(a: A): Lens[A, Option[String]]
    def query(a: A): Lens[A, List[NVP]]
  }
  object Inspectable {
    def apply[A <: Payload](implicit hs: Inspectable[A]): Inspectable[A] = hs

    object ops {
      implicit class InspectableOps[A <: Payload: Inspectable](a: A){
        def body = Inspectable[A].body(a)
        def query = Inspectable[A].query(a)
      }
    }

    implicit val collectorPayloadInspectable: Inspectable[Payload.CollectorPayload] =
      new Inspectable[Payload.CollectorPayload] {
        override def body(p: Payload.CollectorPayload) = GenLens[Payload.CollectorPayload](_.body)
        override def query(p: Payload.CollectorPayload) = GenLens[Payload.CollectorPayload](_.querystring)
      }
  }
}
