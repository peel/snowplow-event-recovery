package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows.{NVP, Payload}

import monocle.Lens
import monocle.macros._
// import config._

import shapeless._
import shapeless.ops.record._

object modify {
  trait Replace[A <: Payload] {
    def apply(a: A)(ctx: String)(fn: String => String): A
  }

  object Replace {
    def apply[A <: Payload](w: String)(implicit r: Lazy[Replace[A]]): Replace[A] = r.value

    implicit def genericReplace[A <: Payload, ARepr <: HList]
      (w: Witness)(implicit
       gen: LabelledGeneric.Aux[A, ARepr],
       mod: Modifier.Aux[ARepr, w.T, String, String, ARepr]
      ): Replace[A] =
      new Replace[A] {
        def apply(a: A)(ctx: String)(fn: String => String): A = {
          val record: ARepr = gen.to(a)
          val updated: ARepr = mod(record, fn)
          gen.from(updated)
        }
      }

    object ops {
      implicit class ReplaceOps[A <: Payload](a: A){
        def replace(ctx: String)(fn: String => String)(implicit replacer: Replace[A]): A = replacer(a)(ctx)(fn)
      }
    }
  }
}

object inspectable {
  trait Inspectable[A <: Payload] {
    // def replaces(a: A)(implicit replace: Replace[A]): A = replace(a)

    def body(a: A): Lens[A, Option[String]]
    def query(a: A): Lens[A, List[NVP]]

  }
  object Inspectable {
    def apply[A <: Payload](implicit i: Inspectable[A]): Inspectable[A] = i
    object ops {
      implicit class InspectableOps[A <: Payload: Inspectable](a: A){
        def body = Inspectable[A].body(a)
        def query = Inspectable[A].query(a)
        // def replaces(ctx: Witness)(implicit replacer: Replace[A]) = Inspectable[A].replaces(a)
      }
    }

    implicit val collectorPayloadInspectable: Inspectable[Payload.CollectorPayload] =
      new Inspectable[Payload.CollectorPayload] {
        override def body(p: Payload.CollectorPayload) = GenLens[Payload.CollectorPayload](_.body)
        override def query(p: Payload.CollectorPayload) = GenLens[Payload.CollectorPayload](_.querystring)
      }

    implicit val enrichmentPayloadInspectable: Inspectable[Payload.EnrichmentPayload] =
      new Inspectable[Payload.EnrichmentPayload] {
        override def body(p: Payload.EnrichmentPayload) = ???
        override def query(p: Payload.EnrichmentPayload) = ???
      }
  }

}
