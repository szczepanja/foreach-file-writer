import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger

import java.io.{File, PrintWriter}
import java.sql.Timestamp

object Rate extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  //as: zamiana Row na para Timestamp, Long
  val rates = spark
    .readStream
    .format("rate")
    .option("header", value = true)
    .load
    .as[(Timestamp, Long)]

  import scala.concurrent.duration._

  var pw: PrintWriter = _

  val fw: ForeachWriter[(Timestamp, Long)] = new ForeachWriter[(Timestamp, Long)] {

    val docs = "target/tmp/docs"

    override def open(partitionId: Long, batchId: Long): Boolean = {
      println(s">>> open ($partitionId, $batchId")
      try {
        pw = new PrintWriter(new File(docs + s"/batch_${partitionId}_${batchId}"))
        true
      } catch {
        case _ =>
          println("ERROR")
          false
      }
    }

    override def process(pair: (Timestamp, Long)): Unit = {
      println(s">>> process $pair")
      pw.write(s"timestamp: ${pair._1} => value: ${pair._2}\n")
    }

    override def close(errorOrNull: Throwable): Unit = {
      println(s">>> error $errorOrNull")
      pw.close()
    }
  }

  val sq = rates
    .writeStream
    .format("console")
    .queryName("hello")
    .trigger(Trigger.ProcessingTime(5.seconds))
    .foreach(fw)
    .start

  sq.awaitTermination(10000)
  sq.stop()
}
