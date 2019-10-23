package com.snowplowanalytics.snowplow
package event.recovery

import com.snowplowanalytics.snowplow.badrows.{NVP, Payload}

import monocle.Lens
import monocle.macros._

import shapeless._
import shapeless.record._
import shapeless.labelled._
import shapeless.ops.hlist._
import shapeless.ops.record._

object inspectable {
  implicit class IgnoringOps[T, L <: HList](gen: LabelledGeneric.Aux[T, L]) {
    def ignoring[V, I <: HList, H <: HList, J <: HList](k: Witness)(v: V)(implicit
      rem: Remover.Aux[L, k.T, (V, I)],
      upd: Updater.Aux[I, FieldType[k.T, V], H],
      ali: Align[H, L]
    ): LabelledGeneric[T] = new LabelledGeneric[T] {
      override type Repr = I
      override def to(t: T): I = gen.to(t) - k
      override def from(r: I): T = gen.from(r + field[k.T](v))
    }
  }

  trait Inspectable[A <: Payload] {
    def replace(a: A)(fn: String => String)(implicit ctx: Witness): A
    def body(a: A): Lens[A, Option[String]]
    def query(a: A): Lens[A, List[NVP]]
  }

  object Inspectable {
    def apply[A <: Payload](implicit i: Inspectable[A]): Inspectable[A] = i
    object ops {
      implicit class InspectableOps[A <: Payload: Inspectable](a: A){
        def body = Inspectable[A].body(a)
        def query = Inspectable[A].query(a)
        def replace(fn: String => String)(implicit ctx: Witness) = Inspectable[A].replace(a)(fn)(ctx)
      }
    }

    implicit val collectorPayloadInspectable: Inspectable[Payload.CollectorPayload] =
      new Inspectable[Payload.CollectorPayload] {
        override def body(p: Payload.CollectorPayload) = GenLens[Payload.CollectorPayload](_.body)
        override def query(p: Payload.CollectorPayload) = GenLens[Payload.CollectorPayload](_.querystring)
        override def replace(p: Payload.CollectorPayload)(fn: String => String)(implicit ctx: Witness) = {
          val symbols = 'version :: 'vendor :: Nil
          val s = symbols.find(_.toString == ctx).get
          val gen = LabelledGeneric[Payload.CollectorPayload].ignoring(symbolize(ctx))(fn("new-vendor"))
          gen.from(gen.to(p))
        }

      }

    implicit val enrichmentPayloadInspectable: Inspectable[Payload.EnrichmentPayload] =
      new Inspectable[Payload.EnrichmentPayload] {
        override def body(p: Payload.EnrichmentPayload) = ???
        override def query(p: Payload.EnrichmentPayload) = ???
        override def replace(p: Payload.EnrichmentPayload)(fn: String => String)(implicit ctx: Witness) = ???
      }

  }

}
