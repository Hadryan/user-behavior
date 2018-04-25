package com.madkudu

import com.madkudu.data.ActionDataModels.ActionData
import kafka.serializer.StringDecoder
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Stream from Kafka
  */
object ActionsDataStream {

  val localLogger = Logger.getLogger("BehaviorDataStream")
  val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")

  def main(args: Array[String]) {

    // update
    // val checkpointDir = "./tmp"

    val sparkConf = new SparkConf().setAppName("Behaviors")
    sparkConf.setIfMissing("spark.master", "local[5]") //"spark://127.0.0.1:7077")
    //    sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)
    sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    println(s"using cassandraDailyViews $cassandraTableDailyViews")

    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)

    localLogger.info(s"connecting to brokers: $kafkaBroker")
    localLogger.info(s"kafkaParams: $kafkaParams")
    localLogger.info(s"topics: $topics")

    val actionsStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val parsedActionsStream: DStream[ActionData] = ingestStream(actionsStream)

    persist(cassandraKeyspace, cassandraTableActions, cassandraTableDailyViews, parsedActionsStream)

    parsedActionsStream.print // for demo purposes only

    //Kick off
    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }

  def persist(CassandraKeyspace: String, CassandraTableRaw: String,
              CassandraTableDailyPrecip: String,
              parsedactionsStream: DStream[ActionData]): Unit = {

    import com.datastax.spark.connector.streaming._

    parsedactionsStream.saveToCassandra(CassandraKeyspace, CassandraTableRaw)

    parsedactionsStream.map { action =>
      (action.uid, fmt.print(new DateTime(action.date)), action.name, 1)
    }.saveToCassandra(CassandraKeyspace, CassandraTableDailyPrecip)
  }

  def ingestStream(rawActionsStream: InputDStream[(String, String)]): DStream[ActionData] = {
    val parsedActionsStream = rawActionsStream.map(_._2.split(","))
      .map(new ActionData(_))
    parsedActionsStream
  }
}
