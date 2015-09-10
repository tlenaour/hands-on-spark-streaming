package com.octo.nad.handson.unit.spark

import com.octo.nad.handson.spark.{Pipeline, SparkStreamingSpec}
import org.scalatest.{ShouldMatchers, GivenWhenThen}

class PreUpdateInMemoryCaBySection extends SparkStreamingSpec with ShouldMatchers with  GivenWhenThen {

  "La méthode preUpdateInMemoryCaBySection de l'object Pipeline" should "calculer le cumul de la seq de double en paramètres et y ajouter le double optionnel" in {
    Given("Une liste de double et un double valorisé à 18.5d")
    val actual = Seq(1.2d,6.7d,8.0d)
    val double1ToAdd = Some(2.6d)
    When("On exécute la méthode preUpdateInMemoryCaBySection")
    val result1 = Pipeline.preUpdateInMemoryCaBySection(actual,double1ToAdd)
    Then("On obient le cumul de la seq + le double")
    result1 should be(Some(18.5d))

    Given("Une liste de double et un double valorisé à None")
    val double2ToAdd = None
    When("On exécute la méthode preUpdateInMemoryCaBySection")
    val result2 = Pipeline.preUpdateInMemoryCaBySection(actual,double2ToAdd)
    Then("On obient uniquement le cumul de la seq")
    result2 should be(Some(15.9d))
  }
}
