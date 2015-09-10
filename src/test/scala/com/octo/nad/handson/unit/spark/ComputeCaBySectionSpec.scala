package com.octo.nad.handson.unit.spark

import com.octo.nad.handson.domain.Article
import com.octo.nad.handson.spark.{Pipeline, SparkStreamingSpec}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.GivenWhenThen

class ComputeCaBySectionSpec extends SparkStreamingSpec with GivenWhenThen{

  "La méthode computeCaBySection de l'object Pipeline" should "renvoyer un tuple (String,Double) => (Section,Ca cumulé)" in {
    Given("Un DStream de ligne csv")
    val inputDstream = generateDStreamArticle
    When("On apelle la méthode splitCsvToArticle de l'object Pipeline")
    val result = Pipeline.computeCaBySection(inputDstream)
    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[(String,Double)]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
    Then("On récupère un DStream de tuple (String,Double) => (Section,Ca cumulé)")
    eventually{
      val resultArrayFromRDD = resultsRDD.head
      resultArrayFromRDD.length should be(2)
      resultArrayFromRDD should contain(("Nettoyage",7.3))
      resultArrayFromRDD should contain(("Apéro",4.6))
    }
  }
  private def generateDStreamArticle: InputDStream[Article] = {
    val lines = scala.collection.mutable.Queue[RDD[Article]]()
    val dstream = ssc.queueStream(lines)
    val art1 = Article(58,"Lessive Mir","Nettoyage",1.3,1,1.3)
    val art2 = Article(58,"Balais brosse","Nettoyage",1.50,4,6)
    val art3 = Article(58,"Tuc","Apéro",0.8,3,2.4)
    val art4 = Article(58,"Bières","Apéro",1.10,2,2.2)
    lines += sc.makeRDD(Seq(art1,art2,art3,art4))
    dstream
  }
}
