package com.igor.onedot

import org.apache.spark.sql.SparkSession

object SessionHelper {
  def build(name: String): SparkSession = {
    SparkSession
      .builder()
      .master("local[2]")
      .appName(name)
      .getOrCreate()
  }
}
