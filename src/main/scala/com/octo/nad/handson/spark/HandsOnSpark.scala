package com.octo.nad.handson.spark

import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.solution.Pipeline
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import _root_.kafka.serializer.StringDecoder


object HandsOnSpark extends App with AppConf{
  /* Création SparkConf */
  val sparkConf =
    new SparkConf()
    .setAppName(AppName)
    .set("spark.cassandra.connection.host", CassandraHostName)
    .setMaster(SparkMaster)

  /* Création des Context */
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, SparkBatchWindow)

  /* Récupe des topics, des kafkaBroker et init des kafkaParams */
  val topics = KafkaTopicSet
  val kafkaBroker = KafkaBroker
  val kafkaParams =
    Map[String, String](
      "metadata.broker.list" -> kafkaBroker
    )
  /* Init du key space et des tables Cassandra */
  prepareCassandraKeySpaceAndTables(sparkConf)
  /* Création et récupération du DStream */
  val ticketStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics).map(_._2)
  Pipeline.processAll(ticketStream)


  ssc.start()
  ssc.awaitTerminationOrTimeout(1000)

  def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraTicketTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue double, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
    }
  }
}
