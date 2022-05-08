package com.igor.onedot

import org.apache.spark.sql.{DataFrame, SaveMode}

object Utils {
  def exportToCsv(df: DataFrame, outputPath: String): Unit =
    df.write
      .option("encoding", "UTF-8")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(outputPath)
}
