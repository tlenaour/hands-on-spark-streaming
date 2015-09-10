package com.octo.nad.handson.unit.spark

import com.octo.nad.handson.domain.Article
import com.octo.nad.handson.spark.{Pipeline, SparkStreamingSpec}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.GivenWhenThen

class ComputeTotalPriceByLineSpec extends SparkStreamingSpec with GivenWhenThen{
  "La méthode computeTotalPriceByLine de l'object Pipeline" should "enrichir l'attribut totalPrice de l'objet Article" in {
    Given("Un DStream d'article")
    val inputDStream = generateDStreamArticle
    When("On apelle la méthode computeTotalPriceByLine de l'object Pipeline")
    val result = Pipeline.computeTotalPriceByLine(inputDStream)
    Then("On récupère un DStream d'Article enrichi du totalPrice")
    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[Article]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
    eventually{
      val resultArrayFromRDD = resultsRDD.head
      resultArrayFromRDD.length should be(2)
      resultArrayFromRDD should contain(Article(58,"Lessive Mir","Nettoyage",1.3,1,1.3))
      resultArrayFromRDD should contain (Article(58,"Tuc","Gateaux Apéro",0.84,3,2.52))
    }
  }
  private def generateDStreamArticle: InputDStream[Article] = {
    val lines = scala.collection.mutable.Queue[RDD[Article]]()
    val dstream = ssc.queueStream(lines)
    val art1 = Article(58,"Lessive Mir","Nettoyage",1.3,1,0)
    val art2 = Article(58,"Tuc","Gateaux Apéro",0.84,3,0)
    lines += sc.makeRDD(Seq(art1,art2))
    dstream
  }
}
