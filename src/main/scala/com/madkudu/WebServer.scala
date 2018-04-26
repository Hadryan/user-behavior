package com.madkudu

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import com.datastax.spark.connector._
import com.madkudu.data.ActionDataModels.{ActionData, JsonSupport}
import org.apache.commons.lang.StringEscapeUtils.escapeJava
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.json._

import scala.concurrent.Future
import scala.io.StdIn

object WebServer extends Directives with JsonSupport {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBroker)
    props.put("client.id", "UserBehaviorProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)


    val sparkConf = new SparkConf().setAppName("Behaviors")
    sparkConf.setIfMissing("spark.master", "local[5]") //"spark://127.0.0.1:7077")
    //    sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)
    sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    val route =
      pathPrefix("v1" ) {
        path("user" / Segment) { userId =>
          get {
            val userBehavior = sc.cassandraTable(cassandraKeyspace, cassandraTableDailyViews)
              .select("day", "name", "views")
              .as((day: String, name: String, views: Int) => (day, name, views))
              .where("day >= ? AND uid = ?", ISODateTimeFormat.date().print(DateTime.now.minusDays(7)), userId)
              .cache()

            if (!userBehavior.isEmpty()) {

              val views = userBehavior.map(_._3).fold(0)(_ + _)

              val activeDays = userBehavior.distinct().count()

              val mostViewedPage = userBehavior.map(d => (d._2, d._3))
                .foldByKey(0)(_ + _)
                .max()(Ordering[Int].on(_._2))._1

              complete {
                Map(
                  "user_id" -> userId.toJson,
                  "number_pages_viewed_in_the_last_7_days" -> views.toJson,
                  "time_spent_on_site_in_last_7_days" -> 0.toJson,
                  "number_of_days_active_in_last_7_days" -> activeDays.toJson,
                  "most_viewed_page_in_last_7_days" -> mostViewedPage.toJson
                ).toJson
              }
            } else complete((NotFound, s"No data for user $userId"))
          }
        
      }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
