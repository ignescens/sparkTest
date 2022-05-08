package com.igor.onedot

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.first

/*
  1) Pre-processing
Here you need to load the data into a dataset and transform the supplier data to achieve the same granularity as the target
data. (Hint: how many rows per product do you have in the target data?)
Be aware of character encodings when processing the data.
 */

class LoadSupplierData(path: String)(implicit val spark: SparkSession) {

  def fromJson: DataFrame =
    spark.read
      .option("encoding", "UTF-8")
      .json(path)
      .withColumnRenamed("entity_id", "entityId")
      .withColumnRenamed("Attribute Values", "attributeValues")
      .withColumnRenamed("Attribute Names", "attributeNames")

  def preProcessDf()(df: DataFrame): DataFrame =
    df.groupBy("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull")
      .pivot("attributeNames")
      .agg(first("attributeValues"))
}
