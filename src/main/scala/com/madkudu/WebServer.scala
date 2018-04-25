package com.madkudu

import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives._
import com.madkudu._
import org.apache.spark.rdd._
import akka.stream.ActorMaterializer
import com.madkudu.data.ActionDataModels.{ActionData, JsonSupport}
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.SparkContext._

import scala.io.StdIn
import spray.json._
import org.joda.time.{DateTime, LocalDate}
import org.joda.time.format.ISODateTimeFormat

object WebServer extends Directives with JsonSupport {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val sparkConf = new SparkConf().setAppName("Behaviors")
    sparkConf.setIfMissing("spark.master", "local[5]") //"spark://127.0.0.1:7077")
    //    sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)
    sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(sparkConf)
    val route =
      path("v1" / "user" / Segment) { userId =>
          get {
            val views = sc.cassandraTable(cassandraKeyspace, cassandraTableDailyViews)
              .select("views")
              .as((views: Int) => views)
              .where("day >= ? AND uid = ?", ISODateTimeFormat.date().print(DateTime.now.minusDays(7)), userId)
              .fold(0)(_ + _)

            val activeDays = sc.cassandraTable(cassandraKeyspace, cassandraTableDailyViews)
                .select("day")
                .as((day: String) => day)
                .where("day >= ? AND uid = ?", ISODateTimeFormat.date().print(DateTime.now.minusDays(7)), userId)
                .distinct()
                .count()

            val mostViewedPage = sc.cassandraTable(cassandraKeyspace, cassandraTableDailyViews)
                .select("views", "name")
                .as((views: Int, name: String) => (name, views))
                .where("day >= ? AND uid = ?", ISODateTimeFormat.date().print(DateTime.now.minusDays(7)), userId)
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
