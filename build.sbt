name := "hands-on-spark"

version := "1.0.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.4.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M3",
  "org.scalatest" % "scalatest_2.10" % "2.0"
)
