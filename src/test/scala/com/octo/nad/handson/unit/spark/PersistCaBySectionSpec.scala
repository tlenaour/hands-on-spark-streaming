package com.octo.nad.handson.unit.spark

import java.util

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.octo.nad.handson.spark.{Pipeline, SparkStreamingSpec}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.GivenWhenThen

class PersistCaBySectionSpec extends SparkStreamingSpec with GivenWhenThen{

  "La méthode persistCaBySection de l'object Pipeline" should "enregistrer les tuples dans Cassandra" in {
    Given("Un DStream de tuples(String, Long)")
    val inputDstream = generateDStreamArticle
    prepareCassandraKeySpaceAndTables(sc.getConf)

    When("On apelle la méthode persistCaBySection de l'object Pipeline")
    Pipeline.persistCaBySection(inputDstream)
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)

    Then("On récupère un DStream de tuple (String,Double) => (Section,Ca cumulé)")
    Thread.sleep(1000)

    eventually{
      var resultList : util.List[Row] = null
      CassandraConnector(ssc.sparkContext.getConf).withSessionDo({
        s =>
          resultList = s.execute(s"select $CassandraColumnSection, $CassandraColumnRevenue from $CassandraKeySpace.$CassandraTicketTable").all()
      })
      resultList.size() should be(2)
      val resultTuples = resultList.toArray(Array[Row]()).map(row => (row.getString(0), row.getDouble(1)))
      resultTuples should contain(("Nettoyage", 5.55))
      resultTuples should contain(("Apero", 1.45))
    }
  }

  private def generateDStreamArticle: InputDStream[(String, Double)] = {
    val lines = scala.collection.mutable.Queue[RDD[(String, Double)]]()
    val dstream = ssc.queueStream(lines)
    val res1 = ("Apero", 1.45)
    val res2 = ("Nettoyage", 5.55)
    lines += sc.makeRDD(Seq(res1, res2))
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
