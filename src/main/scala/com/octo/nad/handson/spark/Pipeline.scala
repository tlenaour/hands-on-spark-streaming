package com.octo.nad.handson.spark

import com.datastax.spark.connector._
import com.octo.nad.handson.domain.{Article, Utils}
import org.apache.spark.streaming.dstream.DStream

object Pipeline extends AppConf{

  def processAll(rawStream : DStream[String]) =  persistCaBySection(computeCaBySection(computeTotalPriceByLine(filterByStoreCaseVersion(splitCsvToArticle(rawStream)))))

  // Emettre un article à partir d'une ligne de CSV, utiliser la fonction csvLineToArticle de l'object Utils
  def splitCsvToArticle(stream : DStream[String]) : DStream[Article] = { stream.map(line => Utils.csvLineToArticle(line.split(";"))) };

  // Filtrer : on ne veut que le magasin numéro 56. Utiliser les case class scala
  def filterByStoreCaseVersion(streamArticle : DStream[Article]) : DStream[Article] = {streamArticle.filter({
    case a:Article
        if a.storeId == 56 => true
    case _ => false
  })};

  // Idem, mais en utilisant une lambda
  def filterByStoreLambdaVersion(streamArticle : DStream[Article]) : DStream[Article] = { streamArticle.filter(article => article.storeId == 56)}

  // Pour chaque ligne, émettre un article avec le prix total (prix_u * qte
  def computeTotalPriceByLine(streamArticles: DStream[Article]) : DStream[Article] = { streamArticles.map(line => line.copy(totalPrice = line.unitPrice*line.quantity) )}

  // Agréger le CA par catégorie de produits
  def computeCaBySection(streamArticles: DStream[Article]) : DStream[(String,Double)]= { streamArticles.map(article => (article.section, article.totalPrice)).reduceByKey(_+_) }

  // Mettre à jour les compteurs de CA par catégorie (états)
  def updateInMemoryCaBySection(stream: DStream[(String,Double)]) = {
    stream.updateStateByKey(preUpdateInMemoryCaBySection)
  }

  // Fonction de mise à jour des états : récupérer l'état courant et y ajouter la valeur calculée
  def preUpdateInMemoryCaBySection(allCa: Seq[Double], caToAdd : Option[Double]) : Option[Double] = { Some(allCa.sum + caToAdd.getOrElse(0d)) }

  // Ecrire le résultat du calcul dans Cassandra
  def persistCaBySection(stream: DStream[(String,Double)]) = { stream.foreachRDD(rdd => rdd.saveToCassandra('foobar', )) }
}
