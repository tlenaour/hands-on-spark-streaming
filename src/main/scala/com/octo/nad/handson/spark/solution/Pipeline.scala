package com.octo.nad.handson.spark.solution

import com.datastax.spark.connector._
import com.octo.nad.handson.domain.{Article, Utils}
import com.octo.nad.handson.spark.AppConf
import org.apache.spark.streaming.dstream.DStream

object Pipeline extends AppConf{

  def processAll(rawStream : DStream[String]) = persistCaBySection(computeCaBySection(computeTotalPriceByLine(filterByStoreCaseVersion(splitCsvToArticle(rawStream)))))

  def splitCsvToArticle(stream : DStream[String]) : DStream[Article] = {
    stream.map(
        line => {
          Utils.csvLineToArticle(line.split(";"))
        }
    )
  }

  def filterByStoreCaseVersion(streamArticle : DStream[Article]) : DStream[Article] = {
    streamArticle.filter({
      case a: Article if a.storeId == 56 => true
      case _ => false
  })
  }

  def filterByStoreLambdaVersion(streamArticle : DStream[Article]) : DStream[Article] = {
    streamArticle.filter(
      article => {
        article.storeId == 56
      }
    )
  }

  def computeTotalPriceByLine(streamArticles: DStream[Article]) : DStream[Article] = {
    streamArticles.map(
      article => {
        val totalPrice = article.quantity * article.unitPrice
        Article(article.storeId,article.product,article.section,article.unitPrice,article.quantity, totalPrice)
      }
    )
  }

  def computeCaBySection(streamArticles: DStream[Article]) : DStream[(String,Double)]= {
    streamArticles.map(
      article => (article.section,article.totalPrice)
    )
    .reduceByKey(_ + _)
  }

  def updateInMemoryCaBySection(stream: DStream[(String,Double)]) = {
    stream.updateStateByKey(preUpdateInMemoryCaBySection)
  }

  def preUpdateInMemoryCaBySection(allCa: Seq[Double],caToAdd : Option[Double]) : Option[Double] = {
    val cumul = allCa.sum
    val newCaToAdd = caToAdd.getOrElse(0d)
    val cumulUpdated =  cumul + newCaToAdd
    Some(cumulUpdated)
  }

  def persistCaBySection(stream: DStream[(String,Double)]) = {
    stream
    .foreachRDD(
        (rdd, batchId) =>
        rdd
          .map(tuple => (tuple._1, batchId.milliseconds, tuple._2))
          .saveToCassandra(
        CassandraKeySpace,CassandraTicketTable,SomeColumns(CassandraColumnSection, CassandraColumnBatchId, CassandraColumnRevenue)
        )
      )
  }
}
