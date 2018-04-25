name := "kafka-streaming"

version := "1.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList(pl @ _*) if pl.contains("log4j.properties") => MergeStrategy.concat
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

scalaVersion := "2.11.6"

resolvers += "jitpack" at "https://jitpack.io"

// still want to be able to run in sbt
// https://github.com/sbt/sbt-assembly#-provided-configuration
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")

libraryDependencies ++= Seq(
  "com.groupon.sparklint" %% "sparklint-spark162" % "1.0.4" excludeAll (
    ExclusionRule(organization = "org.apache.spark")
    ),
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "com.typesafe.akka" %% "akka-remote" % "2.5.12",
  "org.apache.spark" %% "spark-sql" % "1.6.2",
  "org.apache.spark" %% "spark-streaming" % "1.6.2",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0",
  "com.typesafe.akka" %% "akka-http"   % "10.1.1",
  "com.typesafe.akka" %% "akka-stream" % "2.5.12",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.1"
)
