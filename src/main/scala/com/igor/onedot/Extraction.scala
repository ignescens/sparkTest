package com.igor.onedot

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/*
  3) Extraction
  Some relevant features for the product can be extracted from supplier attributes and stored in new attributes. Please extract at least:
  - The value of the consumption from the supplier attribute “ConsumptionTotalText” into an attribute called: “extracted-value-ConsumptionTotalText”
  - The unit of the consumption from the supplier attribute “ConsumptionTotalText” into an attribute called: “extracted-unit-ConsumptionTotalText”
 */

class Extraction(implicit val spark: SparkSession) {

  import spark.implicits._

  def consumptionData()(df: DataFrame): DataFrame = {
    df
      .withColumn("extracted-value-ConsumptionTotalText", split($"ConsumptionTotalText", " ").getItem(0))
      .withColumn("extracted-unit-ConsumptionTotalText", split($"ConsumptionTotalText", " ").getItem(1))
  }

}
