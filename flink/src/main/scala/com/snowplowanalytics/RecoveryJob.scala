package com.snowplowanalytics.snowplow.event.recovery

import java.util.Properties

import org.apache.flink.util.{OutputTag => JOutputTag}

import org.apache.flink.util.Collector
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kinesis._
import org.apache.flink.streaming.connectors.kinesis.config._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation

import cats.syntax.apply._
import cats.syntax.either._
import com.monovore.decline._

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import com.snowplowanalytics.snowplow.badrows._

import config._
import typeinfo._
import recoverable._, Recoverable.ops._

object typeinfo {
  // TODO Flink's macros cause unused import on `package`
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

object Main extends CommandApp(
  name = "snowplow-event-recovery-job",
  header = "Snowplow event recovery job",
  main = {
    val input = Opts.option[String]("input", help = "Input S3 path")
    val output = Opts.option[String]("output", help = "Output Kinesis topic")
    val config = Opts.option[String](
      "config",
      help = "Base64 config with schema com.snowplowanalytics.snowplow/recoveries/jsonschema/1-0-0"
    ).mapValidated(utils.decodeBase64(_).toValidatedNel)
     .mapValidated(utils.loadConfig(_).toValidatedNel)
    (input, output, config).mapN { (i, o, c) => RecoveryJob.run(i, o, c) }
  }
)

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

    // TODO can this be done better with Flink?
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
      .flatMap(FlatMapDeserialize)
      .map(unpack(cfg, _))
      .process(new SplitByStatus(tag))

    lines
      .getSideOutput(tag)
      .map(v => s"Failed: $v")
      .print()

    lines
      .map(utils.coerce(_))
      .filter(_.isDefined)
      .map(_.get)
      .map(utils.thriftSer)
      .addSink(kinesis)

    lines
      .print()

    env.execute("Event recovery job started.")
    ()
  }
}

object FlatMapDeserialize extends RichFlatMapFunction[String, BadRow] {
  override def flatMap(str: String, out: Collector[BadRow]): Unit = {
    badRow(str).foreach(out.collect)
  }

  def badRow(v: String) = io.circe.parser.decode[BadRow](v).toOption
}

class SplitByStatus(tag: OutputTag[String]) extends ProcessFunction[Either[BadRow, Payload], Payload] {
  override def processElement(value: Either[BadRow, Payload], ctx: ProcessFunction[Either[BadRow, Payload], Payload]#Context, out: Collector[Payload]): Unit =
    value match {
      case Right(v) => out.collect(v)
      case Left(v) => ctx.output(tag.asInstanceOf[JOutputTag[Object]], v)
    }
}
