DROP KEYSPACE IF EXISTS madkudu_behavior_data;
CREATE KEYSPACE madkudu_behavior_data WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use madkudu_behavior_data;


CREATE TABLE actions (
  uid text,
  date timestamp,
  name text,
  PRIMARY KEY  (uid, date)
) WITH CLUSTERING ORDER BY (date DESC);


CREATE TABLE daily_views (
  uid text,
  day date,
  name text,
  views	counter,
  PRIMARY KEY  (uid, day, name)
) WITH CLUSTERING ORDER BY (day DESC, name DESC);


