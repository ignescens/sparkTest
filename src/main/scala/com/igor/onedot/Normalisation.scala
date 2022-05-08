package com.igor.onedot

import com.igor.onedot.Normalisation._
import org.apache.spark.sql.functions.{coalesce, initcap, lit, split, typedLit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
2) Normalisation

Normalisation is required in case an attribute value is different but actually is the same (different spelling, language, differ-
ent unit used etc.).

Example: if the source encodes Booleans as “Yes”/”No”, but the target as 1/0, then you would need to normalise “Yes” to 1
and “No” to 0.
Please normalise at least the following supplier attribute:
- “BodyColorText”: needs to be translated into English and to match target values in target attribute “color”
- “MakeText”: needs to be normalised to match target values in target attribute “make”
 */

class Normalisation(implicit val spark: SparkSession) {

  import spark.implicits._

  def normalizeMake()(df: DataFrame): DataFrame = {
    df
      .withColumn("MakeText",
        when($"makeText".contains("-"), $"makeText")
          when($"makeText".contains("-"), $"makeText") // do something that ROLLS-ROYCE will be Rolls-Royce doesn't work: split("-").map(_.toLowerCase).map(_.capitalize).mkString("-"))
          when($"makeText".isin(CarNamesExceptions: _*), $"makeText")
          otherwise initcap($"makeText")
      )
  }

  // colors like  rot mét. will be treated like rot/Red etc
  def normalizeColor()(df: DataFrame): DataFrame = {
    df
      .withColumn("BodyColorText", split($"BodyColorText", " ").getItem(0))
      .withColumn("BodyColorText", coalesce(ColorsColumn($"BodyColorText"), lit("Other")))
  }

}

object Normalisation {
  private final val CarNamesExceptions = Seq("BMW", "AGM", "VW", "MG")
  private final val ColorsColumn = typedLit(Map(
    "beige" -> "Beige",
    "schwarz" -> "Black",
    "blau" -> "Blue",
    "braun" -> "Brown",
    "gold" -> "Gold",
    "grau" -> "Grey",
    "grün" -> "Green",
    "orange" -> "Orange",
    "violett" -> "Purple",
    "rot" -> "Red",
    "silber" -> "Silver",
    "weiss" -> "White",
    "gelb" -> "Yellow"
  ))
}