package com.octo.nad.handson.unit.spark

import java.util

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.{Pipeline, SparkStreamingSpec, AppConf}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.{BeforeAndAfter, GivenWhenThen}

class ProcessAllSpec extends SparkStreamingSpec with GivenWhenThen with BeforeAndAfter with AppConf {
  "La méthode processAll de l'object Pipeline" should "à partir d'un fichier csv renvoyé des tuples (String, Double) => (Section, Ca cumulé)" in {

    Given("Un DStream de ligne csv")
    val inputDstream = generateDStreamObject
    prepareCassandraKeySpaceAndTables(sc.getConf)

    When("On passe dans le pipeline")
    Pipeline.processAll(inputDstream)
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)

    Then("On récupère un DStream de tuples (String, Double) => (Section, Ca cumulé)")
    Thread.sleep(1500)

    eventually{
      var resultList : util.List[Row] = null
      CassandraConnector(ssc.sparkContext.getConf).withSessionDo({
        s =>
          resultList = s.execute(s"select $CassandraColumnSection, $CassandraColumnRevenue from $CassandraKeySpace.$CassandraTicketTable").all()
      })
      resultList.size() should be(2)
      val resultTuples = resultList.toArray(Array[Row]()).map(row => (row.getString(0), row.getDouble(1)))
      resultTuples should contain(("Nettoyage",7.3))
      resultTuples should contain(("Apéro",4.3))
    }
  }

  private def generateDStreamObject: InputDStream[String] = {
    val lines = scala.collection.mutable.Queue[RDD[String]]()
    val dstream = ssc.queueStream(lines)
    lines += sc.makeRDD(Seq(("56;Lessive Mir;Nettoyage;130;1"),("56;Tuc;Apéro;70;3"),("23;Coca;Boisson;120;1"),("56;Balais brosse;Nettoyage;150;4"),("56;Bières;Apéro;110;2")))
    dstream
  }

  private def prepareCassandraKeySpaceAndTables(confCassandra: SparkConf) = {
    CassandraConnector(confCassandra).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeySpace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $CassandraReplicationFactor }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeySpace.$CassandraTicketTable ($CassandraColumnSection text, $CassandraColumnBatchId bigint, $CassandraColumnRevenue double, primary key ($CassandraColumnSection, $CassandraColumnBatchId))")
      session.execute(s"truncate $CassandraKeySpace.$CassandraTicketTable")
    }
  }
}