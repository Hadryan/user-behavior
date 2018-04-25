package com.madkudu.data

import java.time.format.DateTimeFormatter

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.json.DefaultJsonProtocol

object ActionDataModels {

  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait BehaviorModel extends Serializable

  final case class ActionData(
                               uid: String,
                               date: Long,
                               name: String
                             ) extends BehaviorModel {
    def this(array: Array[String]) = this(
        uid = array(0),
        date = ISODateTimeFormat.dateTimeParser().parseDateTime(array(2)).getMillis,
        name = array(1)
      )
  }

  trait BehaviorAggregate extends BehaviorModel with Serializable {
    def uid: String
  }

  trait Views extends BehaviorAggregate

  final case class DailyViews(uid: String,
                              day: DateTime,
                              name: Int,
                              views: Int) extends Views

  trait JsonSupport extends SprayJsonSupport with JsonJodaTimeProtocol {
    implicit val actionFormat = jsonFormat3(ActionData)
    implicit val dailyViewsFormat = jsonFormat4(DailyViews)
  }

}
