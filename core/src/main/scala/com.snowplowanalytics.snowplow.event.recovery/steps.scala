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

import com.snowplowanalytics.snowplow.badrows._

import config._
import inspectable.Inspectable._
import inspectable.Inspectable.ops._

object steps {
  /**
    * A definition of a single recovery process step.
    * Many subsequent steps form a recovery flow.
    * The flows are applied to BadRows in recovery process.
    * @param a type of Payload given Step operates on
    */
  sealed trait Step[A <: Payload] {
    /**
      * Defines a process of application of the `Step` to `A`
      */
    val recover: A => Either[A, A]
  }

  class Precondition[A <: Payload] extends Step[A] {
    // FIXME what should Precondition step do on a flow?
    val recover: A => Either[A, A] = a => Right(a)
  }

  class PassThrough[A <: Payload] extends Step[A] {
    // FIXME what should PassThrough step do on a flow?
    val recover: A => Either[A, A] = a => Right(a)
  }

  class Modify[A <: Payload : Inspectable](config: StepConfig) extends Step[A] {
    val recover: A => Either[A, A] = a => config match {
      case Replacement(context, matcher, replacement) =>
        a.replace(context, matcher, replacement)
      case Removal(context, matcher) =>
        a.remove(context, matcher)
      case _ =>
        Left(a)
    }
  }
}
