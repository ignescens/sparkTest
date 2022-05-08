package com.igor.onedot

import org.specs2.specification.AfterAll

import org.apache.spark.sql.SparkSession

trait SparkTestHelper extends AfterAll {

  implicit val spark: SparkSession = SparkSession.builder
    .appName("onedot-tests")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()
}
