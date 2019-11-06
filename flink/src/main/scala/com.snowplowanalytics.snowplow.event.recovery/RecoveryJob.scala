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

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kinesis._
import org.apache.flink.streaming.connectors.kinesis.config._

import org.apache.flink.api.common.typeinfo.TypeInformation

import com.monovore.decline._
import com.monovore.decline.effect._

import cats.effect._
import cats.implicits._
import io.circe.parser.decode

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import com.snowplowanalytics.snowplow.badrows._

import config._
import typeinfo._
import recoverable._, Recoverable.ops._

import cats.Id
import scala.concurrent.duration.{TimeUnit, MILLISECONDS, NANOSECONDS}

object typeinfo {
  implicit val badRowT: TypeInformation[BadRow] = TypeInformation.of(classOf[BadRow])
  implicit val eitherAForAFT: TypeInformation[Either[BadRow, BadRow]] = TypeInformation.of(classOf[Either[BadRow, BadRow]])
  implicit val eitherBBT: TypeInformation[Either[BadRow, BadRow.AdapterFailures]] = TypeInformation.of(classOf[Either[BadRow, BadRow.AdapterFailures]])
  implicit val payloadT: TypeInformation[Payload] = TypeInformation.of(classOf[Payload])
  implicit val payloadCPT: TypeInformation[Payload.CollectorPayload] = TypeInformation.of(classOf[Payload.CollectorPayload])
  implicit val eitherPPT: TypeInformation[Either[BadRow, Payload]] = TypeInformation.of(classOf[Either[BadRow, Payload]])
  implicit val collectorPayloadT: TypeInformation[CollectorPayload] = TypeInformation.of(classOf[CollectorPayload])
  implicit val optionCollectorPayloadT: TypeInformation[Option[CollectorPayload]] = TypeInformation.of(classOf[Option[CollectorPayload]])
  implicit val stringT: TypeInformation[String] = TypeInformation.of(classOf[String])
}

object Main extends CommandIOApp(
  name = "snowplow-event-recovery-job",
  header = "Snowplow event recovery job"
){
  override def main: Opts[IO[ExitCode]] = {
    val input = Opts.option[String]("input", help = "Input S3 path")
    val output = Opts.option[String]("output", help = "Output Kinesis topic")
    val config = Opts.option[String](
      "config",
      help = "Base64 config with schema com.snowplowanalytics.snowplow/recovery_config/jsonschema/1-0-0"
    ).mapValidated(utils.decodeBase64(_).toValidatedNel)
     .mapValidated(json => utils.validateConfiguration[Id](json).toValidatedNel.map(_ => json))
     .mapValidated(utils.loadConfig(_).toValidatedNel)
    (input, output, config).mapN { (i, o, c) => IO(RecoveryJob.run(i, o, c)).as(ExitCode.Success) }
  }

  implicit val catsClockIdInstance: Clock[Id] = new Clock[Id] {
    override def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), NANOSECONDS)
    override def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), MILLISECONDS)
  }
}

object RecoveryJob {
  def run(input: String, output: String, cfg: Config): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = {
      val producerConfig = new Properties()
      producerConfig.put(AWSConfigConstants.AWS_REGION, "eu-central-1")
      producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "access-key")
      producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secret-key")
      producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "https://host.docker.internal:4568")
      producerConfig.put("KinesisEndpoint", "host.docker.internal")
      producerConfig.put("KinesisPort", "4568")
      producerConfig.put("VerifyCertificate", "false")
      producerConfig
    }

    val kinesis = {
      val producer = new FlinkKinesisProducer[String](new SimpleStringSchema, config)
      producer.setFailOnError(true)
      producer.setDefaultStream(output)
      producer.setDefaultPartition("0")
      producer
    }

    def unpack(cfg: Config, b: BadRow): Either[BadRow, Payload] = b match {
      case r: BadRow.AdapterFailures => r.recover(cfg).map(_.payload)
      case r: BadRow.TrackerProtocolViolations => r.recover(cfg).map(_.payload)
      case r: BadRow.SchemaViolations => r.recover(cfg).map(_.payload)
      case r: BadRow.EnrichmentFailures => r.recover(cfg).map(_.payload)
      case l => Left(l)
    }

    val tag = OutputTag[String]("failed")
    def lines = env
      .readFileStream(s"s3://$input")
      .flatMap(decode[BadRow](_).toOption)
      .map(unpack(cfg, _))
      .process(new SplitByStatus(tag))

    lines
      .map(utils.coerce(_))
      .filter(_.isDefined)
      .map(_.get)
      .map(utils.thriftSer)
      .addSink(kinesis)

    env.execute("Event recovery job started.")
    ()
  }
}
