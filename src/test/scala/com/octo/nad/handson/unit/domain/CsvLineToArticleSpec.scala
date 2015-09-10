package com.octo.nad.handson.unit.domain

import com.octo.nad.handson.domain.{Article, Utils}
import org.scalatest.{ShouldMatchers, GivenWhenThen, FlatSpec}

class CsvLineToArticleSpec extends FlatSpec with GivenWhenThen with ShouldMatchers{

  "La méthode csvLineToArticle de l'object Utils" should "renvoyer un Article" in{
    Given("un Array de string")
    val line = Array[String]("12","Lessive Mir","Produits Ménager","3520","1")
    When("La méthode csvLineToArticle de l'object Utils est appelé")
    val result = Utils.csvLineToArticle(line)
    Then("On récupère un objet Article")
    result should be (Article(12,"Lessive Mir","Produits Ménager",35.2,1,0))
  }
}
