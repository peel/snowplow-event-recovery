package com.snowplowanalytics.snowplow.event.recovery

import java.util.Properties

import org.apache.flink.util.Collector
import org.apache.flink.api.common.serialization._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kinesis._
import org.apache.flink.streaming.connectors.kinesis.config._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import cats.syntax.apply._
import cats.syntax.either._
import com.monovore.decline._
import mymodel._
import mymodel.implicits._

object mymodel {
  case class BadRow(name: String, error: String)
  object implicits {
    implicit val badRowT = TypeInformation.of(classOf[BadRow])
    implicit val optionBadRowT = TypeInformation.of(classOf[Option[BadRow]])
    implicit val stringT = TypeInformation.of(classOf[String])
  }
}

object Main extends CommandApp(
  name = "snowplow-event-recovery-job",
  header = "Snowplow event recovery job",
  main = {
    val input = Opts.option[String]("input", help = "Input S3 path")
    val output = Opts.option[String]("output", help = "Output Kinesis topic")
    val recoveryScenarios = Opts.option[String](
      "config",
      help = "Base64 config with schema com.snowplowanalytics.snowplow/recoveries/jsonschema/1-0-0"
    ).mapValidated(utils.decodeBase64(_).toValidatedNel)
      .mapValidated(json => utils.validateConfiguration(json).toValidatedNel.map(_ => json))
      .mapValidated(utils.parseRecoveryScenarios(_).toValidatedNel)
    (input, output, recoveryScenarios).mapN { (i, o, rss) => RecoveryJob.run(i, o, rss) }
  }
)

object RecoveryJob {
  def run(input: String, output: String, recoveryScenarios: List[RecoveryScenario]): Unit = {
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

    val lines = env
      .readFileStream(s"s3://$input")
      .flatMap(FlatMapDeserialize)
      .filter(_.isDefined)
      .map(_.asJson.noSpaces)

    lines
      .addSink(kinesis)

    lines
      .print()

    val res = env.execute("Event recovery job started.")
  }
}

object FlatMapDeserialize extends RichFlatMapFunction[String, Option[BadRow]] {
  override def flatMap(str: String, out: Collector[Option[BadRow]]): Unit = {
    out.collect(badRow(str))
  }
  //2.11
  def badRow(v: String) = decode[BadRow](v) match {
    case Right(a) => Some(a)
    case Left(err) => None
  }

}
