package com.igor.onedot

import com.igor.onedot.Normalisation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}

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

  /*
  we have 3 cases here:
    1) when name consist of two words without space and initcap won't work. e.g. Mercedes-Benz
    2) when name should be upper case
    3) when first letter of each word should be in upper case
   */
  private[onedot] def normalizeMake()(df: DataFrame): DataFrame = {
    df.withColumn(
      "MakeText",
      when($"MakeText".contains("-"), doubleName($"MakeText"))
        when ($"MakeText".isin(CarNamesExceptions: _*), upper($"MakeText"))
        otherwise initcap($"MakeText")
    )
  }

  // colors like  rot mét. will be treated like rot/Red etc
  private[onedot] def normalizeColor()(df: DataFrame): DataFrame = {
    df.withColumn("BodyColorText", split($"BodyColorText", " ").getItem(0))
      .withColumn("BodyColorText", coalesce(ColorsColumn($"BodyColorText"), lit("Other")))
  }

  def normalizeColorAndMake()(df: DataFrame): DataFrame =
    df.transform(normalizeMake())
      .transform(normalizeColor())

  private def doubleName(col: ColumnName) = {
    translate(
      initcap(
        translate(col, "-", " ")
      ),
      " ",
      "-"
    )
  }

}

object Normalisation {
  private final val CarNamesExceptions = Seq("BMW", "AGM", "VW", "MG")
  private final val ColorsColumn = typedLit(
    Map(
      "beige"   -> "Beige",
      "schwarz" -> "Black",
      "blau"    -> "Blue",
      "braun"   -> "Brown",
      "gold"    -> "Gold",
      "grau"    -> "Grey",
      "grün"    -> "Green",
      "orange"  -> "Orange",
      "violett" -> "Purple",
      "rot"     -> "Red",
      "silber"  -> "Silver",
      "weiss"   -> "White",
      "gelb"    -> "Yellow"
    )
  )
}
