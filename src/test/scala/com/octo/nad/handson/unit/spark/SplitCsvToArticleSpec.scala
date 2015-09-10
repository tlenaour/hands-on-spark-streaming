package com.octo.nad.handson.unit.spark

import com.octo.nad.handson.domain.Article
import com.octo.nad.handson.spark.{Pipeline, SparkStreamingSpec}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.GivenWhenThen

class SplitCsvToArticleSpec extends SparkStreamingSpec with GivenWhenThen{
  "La méthode splitCsvToArticle de l'object Pipeline" should "renvoyer uniquement des Articles à partir de ligne de csv" in {
    Given("Un DStream de ligne csv")
    val inputDstream = generateDStreamCsvLine
    When("On apelle la méthode splitCsvToArticle de l'object Pipeline")
    val result = Pipeline.splitCsvToArticle(inputDstream)
    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[Article]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
    Then("On récupère un DStream d'Article")
    eventually{
      val resultArrayFromRDD = resultsRDD.head
      resultArrayFromRDD.length should be(2)
      resultArrayFromRDD should contain(Article(1,"Lessive Mir","Nettoyage",1.0,1,0))
      resultArrayFromRDD should contain(Article(2,"Tuc","Gateaux Apéro",0.84,3,0))
    }
  }
  private def generateDStreamCsvLine: InputDStream[String] = {
    val lines = scala.collection.mutable.Queue[RDD[String]]()
    val dstream = ssc.queueStream(lines)
    lines += sc.makeRDD(Seq(("1;Lessive Mir;Nettoyage;100;1"),("2;Tuc;Gateaux Apéro;84;3")))
    dstream
  }
}
