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

import org.apache.flink.util.{OutputTag => JOutputTag}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.functions.ProcessFunction
import com.snowplowanalytics.snowplow.badrows._

class SplitByStatus(tag: OutputTag[String]) extends ProcessFunction[Either[BadRow, Payload], Payload] {
  override def processElement(value: Either[BadRow, Payload], ctx: ProcessFunction[Either[BadRow, Payload], Payload]#Context, out: Collector[Payload]): Unit =
    value match {
      case Right(v) => out.collect(v)
      case Left(v) => ctx.output(tag.asInstanceOf[JOutputTag[Object]], v)
    }
}
