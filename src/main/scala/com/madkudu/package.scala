package com

package object madkudu {

  val kafkaTopicRaw = "actions"
  val kafkaBroker = "kafka:9092"

  val cassandraKeyspace = "madkudu_behavior_data"
  val cassandraTableActions = "actions"
  val cassandraTableDailyViews = "daily_views"
}
