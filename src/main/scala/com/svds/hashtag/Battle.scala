package com.svds.hashtag

import java.io._
import java.net.Socket
import java.util.Properties
import java.util.concurrent.Future

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try
import scala.util.matching.Regex

object Battle extends LazyLogging {

  def main(args: Array[String]) {

    val options: Options = parseArgs(args.toList)

    val conf = new SparkConf().setAppName("Hashtag Battle")
    val ssc = new StreamingContext(conf, options.duration)

    val stream = TwitterUtils.createStream(ssc, twitterAuth = None, filters = (options.hashtags ++ options.terms).toSeq)
      .repartition(options.partitions)
      .map(tweet => s"@${tweet.getUser.getScreenName}: ${tweet.getText}")

    battle(stream, options)

    sys.ShutdownHookThread {
      logger.info("Attempting to shutdown gracefully")
      ssc.stop()
      options.outputs.foreach(_.close())
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def battle(stream: DStream[String], options: Options): Unit = {
    val searchTerms = getSearchTerms(options)
    val cased = stream.transform(search(searchTerms) _).cache()
    // It would be more efficient to do the following, but it requires checkpointing which we don't want to use.
    // val windowedCounts = cased.countByValueAndWindow(options.window, options.duration, options.partitions)
    val windowedCounts = cased.window(options.window).countByValue(options.partitions)
    windowedCounts.foreachRDD(output(searchTerms, options.outputs) _)
  }

  def getSearchTerms(options: Options): List[String] = options.hashtags.map("#" + _).toList ++ options.terms

  def search(searchTerms: List[String])(tweets: RDD[String]): RDD[String] = {
    val searchRegex = s"(?i)(${searchTerms.mkString("|")})".r
    val filtered = tweets.flatMap(tweet => searchRegex.findAllIn(tweet).map(_.toString).toTraversable)
    val caseMap = searchTerms.map(e => (e.toLowerCase, e)).toMap
    val cased = filtered.map(k => caseMap.getOrElse(k.toLowerCase, k))
    cased
  }

  def output(searchTerms: List[String], outputs: Set[Output])(rddCounts: RDD[(String, Long)], time: Time): Unit = {
    val counts = rddCounts.collectAsMap()
    val withZeros = searchTerms.map(_ -> 0L).toMap ++ counts
    withZeros.foreach { case (key, count) =>
      val formattedKey = key.replaceAll("\\s", "_")
      outputs.foreach(_.write(formattedKey, count, time.milliseconds / 1000))
    }
    outputs.foreach(_.flush())
  }

  @tailrec
  def parseArgs(args: List[String], options: Options = Options()): Options = args match {
    case Nil if (options.hashtags.nonEmpty || options.terms.nonEmpty) && options.outputs.nonEmpty => options
    case "--output-stdout" :: tail =>
      parseArgs(tail, options.copy(outputs = options.outputs + StdoutOutput))
    case "--output-kafka" :: brokers :: topic :: tail =>
      parseArgs(tail, options.copy(outputs = options.outputs + KafkaOutput(brokers, topic)))
    case "--output-socket" :: host :: port :: tail =>
      parseArgs(tail, options.copy(outputs = options.outputs + SocketOutput(host, port.toInt)))
    case "--output-graphite" :: host :: port :: path :: tail =>
      parseArgs(tail, options.copy(outputs = options.outputs + SocketOutput(host, port.toInt, path + ".")))
    case "--output-file" :: path :: tail =>
      parseArgs(tail, options.copy(outputs = options.outputs + LocalFileOutput(path)))
    case "--hashtags" :: hashtags :: tail =>
      val parsedHashtags = regexFilter(hashtags.split(','), """^([a-zA-Z0-9_-]*)$""".r, "hashtag")
      parseArgs(tail, options.copy(hashtags = options.hashtags ++ Set(parsedHashtags: _*)))
    case "--terms" :: terms :: tail =>
      val parsedTerms = regexFilter(terms.split(','), """^([ a-zA-Z0-9_-]*)$""".r, "term")
      parseArgs(tail, options.copy(terms = options.terms ++ Set(parsedTerms: _*)))
    case "--duration" :: duration :: tail =>
      parseArgs(tail, options.copy(duration = Seconds(duration.toInt)))
    case "--window" :: window :: tail =>
      parseArgs(tail, options.copy(window = Seconds(window.toInt)))
    case "--partitions" :: partitions :: tail =>
      parseArgs(tail, options.copy(partitions = partitions.toInt))
    case tail =>
      println(
        """Hashtag Battle with Spark Streaming
          |Usage:
          |  [ --hashtags list,of,hashtags ] [ --terms more,stuff ]
          |  [ --duration sec ] [ --window sec ] [ --partitions num ]
          |  [ --output-stdout ] [ --output-kafka brokers topic ] [ --output-socket host port ]
          |  [ --output-graphite host port path ] [ --output-file path ]
          |
          |Defaults
          | - Duration is 5 sec, window is 10 min (600 sec), and number of partitions is 3.
          |Notes
          | - Requires a twitter4j.properties file in the pwd or classpath.
          | - Multiple outputs can be specified, minimum one.
          | - Hashtags and/or terms are required
        """.stripMargin)
      if (tail.nonEmpty)
        logger.info(s"remaining args $tail and parsed options is $options")
      sys.exit(42)
  }

  def regexFilter(seq: Seq[String], R: Regex, typ: String): Seq[String] = seq flatMap {
    case R(w) => Seq(w)
    case u =>
      logger.warn(s"$u is not a valid $typ, removing from the list. Must match $R")
      Seq()
  }
}

case class Options(outputs: Set[Output] = Set(),
                   hashtags: Set[String] = Set(),
                   terms: Set[String] = Set(),
                   window: Duration = Minutes(10),
                   duration: Duration = Seconds(5),
                   partitions: Int = 3)

trait Output extends Closeable with Flushable with LazyLogging {
  def format: String = "%s %d %d"

  def write(key: String, count: Long, time: Long): Unit = write(format.format(key, count, time))

  def write(s: String): Unit

  /** For super simple exception logging when we don't really care about the result */
  protected def logFailure(expression: => Any, message: => String): Unit = Try(expression) recover {
    case e => logger.error(message, e)
  }
}

object StdoutOutput extends Output {
  def write(s: String): Unit = println(s)

  def close(): Unit = ()

  def flush(): Unit = ()
}

case class KafkaOutput(brokers: String, topic: String) extends Output with Serializable {
  private val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", classOf[StringSerializer])
  props.put("value.serializer", classOf[StringSerializer])

  @transient private lazy val producer = new KafkaProducer[String, String](props)
  @transient private lazy val pending = mutable.Stack[Future[_]]()

  def write(s: String): Unit = {
    val record = new ProducerRecord[String, String](topic, s)
    pending push producer.send(record)
  }

  def close(): Unit = producer.close()

  def flush(): Unit = {
    pending.foreach { f => logFailure(f.get(), "Unable to flush message to kafka") }
    pending.clear()
  }
}

case class SocketOutput(host: String, port: Int, prefix: String = "") extends Output with Serializable {
  override val format = prefix + super.format + "\n"
  // For initial connection error out immediately if unable to connect
  @transient private var out: Writer = connect()

  def write(s: String): Unit = Try(out.write(s)) recover {
    case e => logger.error(s"Unable to write to socket $host:$port, retrying.", e)
      out = connect()
      logFailure(out.write(s), s"Unable to write to socket $host:$port")
  }

  private def connect(): Writer = {
    Try(close()) // Close output if it exists and ignore exceptions
    val socket = new Socket(host, port)
    new OutputStreamWriter(socket.getOutputStream)
  }

  def close(): Unit = logFailure(out.close(), s"Unable to close socket $host:$port")

  def flush(): Unit = Try(out.flush()) recover {
    case e => logger.error(s"Unable to flush socket $host:$port, reconnecting", e)
      out = connect()
  }
}

case class LocalFileOutput(path: String) extends Output {
  override val format = super.format + "\n"
  private val file = new File(path)
  if (file.exists())
    throw new RuntimeException(s"The output file $file already exists")
  private val out = new BufferedWriter(new FileWriter(file))

  def write(s: String): Unit = out.write(s)

  def close(): Unit = logFailure(out.close(), s"Unable to close file $file")

  def flush(): Unit = logFailure(out.flush(), s"Unable to flush file $file")
}
