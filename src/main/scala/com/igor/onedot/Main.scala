package com.igor.onedot

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Main extends App {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark SQL Workshop")
    .getOrCreate()

  import spark.implicits._

  // step 1
  object PreProcessing {
    val df = spark.read.json("src/main/resources/supplier_car.json")
      .withColumnRenamed("entity_id", "entityId")
      .withColumnRenamed("Attribute Values", "attributeValues")
      .withColumnRenamed("Attribute Names", "attributeNames")
      .withColumnRenamed("ModelTypeText", "modelTypeText")
      .withColumnRenamed("ModelText", "modelText")
      .withColumnRenamed("TypeNameFull", "typeNameFull")
      .withColumnRenamed("TypeName", "typeName")
      .withColumnRenamed("MakeText", "makeText")
      .withColumnRenamed("ID", "id")
      .as[Supplier]

    val preProcdDf = df
      .groupBy("id", "makeText", "modelText", "modelTypeText", "typeName", "typeNameFull")
      .pivot("attributeNames")
      .agg(first("attributeValues"))
  }

  PreProcessing.preProcdDf.show(10)

  // step 2 1) translate? remove caps

  val normalizedDf = PreProcessing.preProcdDf
    .withColumn("makeText",
      when($"makeText".contains("-"), $"makeText") // do something that ROLLS-ROYCE will be Rolls-Royce doesn't work: split("-").map(_.toLowerCase).map(_.capitalize).mkString("-"))
      when($"makeText" === "BMW", $"makeText")
      when($"makeText" === "VW", $"makeText")
      otherwise(initcap($"makeText"))
    )

  PreProcessing.preProcdDf.select("BodyColorText").distinct().show(100)

  /*
    |  schwarz mét.|
    |        orange|
    |      bordeaux|
    |          grün|
    |       schwarz|
    |    braun mét.|
    |      rot mét.|
    |          grau|
    |          gelb|
    |         braun|
    |         weiss|
    |     gelb mét.|
    |     gold mét.|
    |anthrazit mét.|
    |          blau|
    |          gold|
    |         beige|
    |     grün mét.|
    |    weiss mét.|
    |  violett mét.|
    |     grau mét.|
    |     blau mét.|
    |   silber mét.|
    |   orange mét.|
    |        silber|
    |    beige mét.|
    |     anthrazit|
    | bordeaux mét.|
    |           rot|
    maybe a manual mapping?
   */

  /*
  3) Extraction
  Some relevant features for the product can be extracted from supplier attributes and stored in new attributes. Please extract at least:
  - The value of the consumption from the supplier attribute “ConsumptionTotalText” into an attribute called: “extracted-value-ConsumptionTotalText”
  - The unit of the consumption from the supplier attribute “ConsumptionTotalText” into an attribute called: “extracted-unit-ConsumptionTotalText”
   */


  val extractionDf = normalizedDf
    .withColumn("extracted-value-ConsumptionTotalText", split($"ConsumptionTotalText","/").getItem(0))
    .withColumn("extracted-unit-ConsumptionTotalText", split($"ConsumptionTotalText","/").getItem(1))

  extractionDf.show(10)

  /*
  4) Integration
  Integration is to transform the supplier data with a specific data schema into a new dataset with target data schema, such as to:
  - keep any attributes that can be mapped to the target schema
  - discard attributes not mapped to the target schema
  - keep the number of records from the supplier data as unchanged
  Please integrate the dataset using at least the 5 attributes mapping as follows: (structure is given as “supplier attribute”=> “target attribute”)
  - the normalised “BodyColorText” => “color”
  - the normalised “MakeText” => “make”
  - “ModelText” => “model”
  - “TypeName” => “model_variant”
  - “City” => “city”
   */

  private final val ColumnsToDrop = Seq("id", "typeNameFull", "ConditionTypeText", "Hp", "InteriorColorText")

  val integrationDf = extractionDf
    .withColumnRenamed("BodyColorText", "color")
    .withColumnRenamed("makeText", "make")
    .withColumnRenamed("ModelText", "model")
    .withColumnRenamed("typeName", "model_variant")
    .withColumnRenamed("City", "city")
    .withColumnRenamed("BodyTypeText", "carType")
    .drop(ColumnsToDrop:_*).show(10)



//  normilizeMakeTxt(PreProcessing.preProcdDf).show(100)

//  private def normilizeMakeTxt(df: DataFrame) = {
//    // by default all brands start with a capital letter,
//    df
//      .withColumn("makeText", initcap($"makeText"))
//      .select("makeText").map{value => value.toString().split("-").map(_.capitalize).mkString("-")}
//  }

  private final val CarNamesExceptions = Seq("BMW", "AGM", "VW", "MG")
}