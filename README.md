# Madkudu behavior service

#### To run on local machine

The only required is to have docker installed on your machine

* Start Zookeeper, Kafka and Cassandra

`docker-compose up kafka cassandra zookeeper`

* Create Kafka topic

`docker-compose up create_test_topic`

* Create Cassandra CQL definitions

`docker-compose up create-timeseries`

* Send actions Data to Kafka (do not hesitate to provide your own, or to just re-run it)

`docker-compose up injest_information`

* Now you can start the WebServer and ActionsDataStream with your favourite IDE and start calling the API

i.e: `http://localhost:8080/v1/user/019mr8mf4r`