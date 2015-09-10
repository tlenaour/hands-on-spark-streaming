package com.octo.nad.handson.domain

//Numéro de magasin, libelle produit, libelle catégorie, prix unitaire, qté
case class Article(storeId: Int, product: String, section: String, unitPrice: Double, quantity: Int,totalPrice : Double)
