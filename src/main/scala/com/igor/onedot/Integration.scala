package com.igor.onedot

import com.igor.onedot.Integration._
import org.apache.spark.sql.{DataFrame, SparkSession}

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

class Integration(implicit val spark: SparkSession) {

  import spark.implicits._

  def toTargetSchema()(df: DataFrame): DataFrame = {
    ColumnsToRename.foldLeft(df) {
      case (df, (supplierName, targetName)) => df.withColumnRenamed(supplierName, targetName)
    }
      .drop(ColumnsToDrop: _*)
  }

}

object Integration {

  private final val ColumnsToRename = Map(
    "BodyColorText" -> "color",
    "MakeText" -> "make",
    "ModelText" -> "model",
    "TypeName" -> "model_variant",
    "City" -> "city",
    "BodyTypeText" -> "carType",
  )
  private final val ColumnsToDrop = Seq("ID", "TypeNameFull", "ConditionTypeText", "Hp", "InteriorColorText", "ConsumptionTotalText")

}
