package com.igor.onedot

import org.apache.spark.sql.{AnalysisException, SparkSession}

object Main {
  implicit val spark: SparkSession = SessionHelper.build("onedot-supplier-processing")

  def main(args: Array[String]): Unit = {
    try {
      execPipeline()
    } catch {
      case fileNotFound: AnalysisException => println(s"Input file wasn't found: $fileNotFound")
      case e: Exception                    => println(s"It was an error in the pipeline, please check logs: $e}")
    }

  }

  private def execPipeline(): Unit = {
    val loadSupplierData = new LoadSupplierData("src/main/resources/supplier_car.json")
    val normalisation    = new Normalisation
    val extraction       = new Extraction
    val integration      = new Integration

    val initialDf = loadSupplierData.fromJson

    val preProcessedDf = initialDf
      .transform(loadSupplierData.preProcessDf())

    val normalizedDf = preProcessedDf
      .transform(normalisation.normalizeColor())
      .transform(normalisation.normalizeMake())

    val extractedDf = normalizedDf
      .transform(extraction.consumptionData())

    val integratedDf = extractedDf
      .transform(integration.toTargetSchema())

    Utils.exportToCsv(preProcessedDf, "output/pre-processed")
    Utils.exportToCsv(normalizedDf, "output/normalized")
    Utils.exportToCsv(extractedDf, "output/extracted")
    Utils.exportToCsv(integratedDf, "output/integrated")
  }
}
