package com.octo.nad.handson.unit.spark

import com.octo.nad.handson.domain.Article
import com.octo.nad.handson.spark.{Pipeline, SparkStreamingSpec}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.GivenWhenThen

class FilterByStoreSpec extends SparkStreamingSpec with GivenWhenThen{
  "La méthode filterByStoreLambdaVersion de l'object Pipeline" should "renvoyer uniquement les Articles du magasin 58" in {
    Given("Un DStream d'article de plusieurs magasins")
    val inputDStream = generateDStreamArticle
    When("On apelle la méthode filterByStoreLambdaVersion de l'object Pipeline")
    val result = Pipeline.filterByStoreLambdaVersion(inputDStream)
    Then("On récupère un DStream d'Article du magasin 58")
    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[Article]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
    eventually{
      val resultArrayFromRDD = resultsRDD.head
      resultArrayFromRDD.length should be(1)
      resultArrayFromRDD.apply(0) should be(Article(56,"Tuc","Gateaux Apéro",0.84,3,0))
    }
  }
  "La méthode filterByStoreCaseVersion de l'object Pipeline" should "renvoyer uniquement les Articles du magasin 58" in {
    Given("Un DStream d'article de plusieurs magasins")
    val inputDStream = generateDStreamArticle
    When("On apelle la méthode filterByStoreLambdaVersion de l'object Pipeline")
    val result = Pipeline.filterByStoreCaseVersion(inputDStream)
    Then("On récupère un DStream d'Article du magasin 58")
    var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[Article]]
    result.foreachRDD(rdd => {
      resultsRDD += rdd.collect()
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000)
    eventually{
      val resultArrayFromRDD = resultsRDD.head
      resultArrayFromRDD.length should be(1)
      resultArrayFromRDD.apply(0) should be(Article(56,"Tuc","Gateaux Apéro",0.84,3,0))
    }
  }
  private def generateDStreamArticle: InputDStream[Article] = {
    val lines = scala.collection.mutable.Queue[RDD[Article]]()
    val dstream = ssc.queueStream(lines)
    val art1 = Article(1,"Lessive Mir","Nettoyage",1,1,0)
    val art2 = Article(56,"Tuc","Gateaux Apéro",0.84,3,0)
    lines += sc.makeRDD(Seq(art1,art2))
    dstream
  }
}