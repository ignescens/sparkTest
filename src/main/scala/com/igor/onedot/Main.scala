package com.igor.onedot

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.slf4j.LoggerFactory

object Main {
  implicit val spark: SparkSession = SessionHelper.build("onedot-supplier-processing")

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    try {
      execPipeline()
    } catch {
      case fileNotFound: AnalysisException => logger.error(s"Input file wasn't found: $fileNotFound")
      case e: Exception                    => logger.error(s"It was an error in the pipeline, please check logs: $e}")
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
      .persist()

    val normalizedDf = preProcessedDf
      .transform(normalisation.normalizeColorAndMake())
      .persist()

    val extractedDf = normalizedDf
      .transform(extraction.consumptionData())
      .persist()

    val integratedDf = extractedDf
      .transform(integration.toTargetSchema())
      .persist()

    Utils.exportToCsv(preProcessedDf, "output/pre-processed")
    Utils.exportToCsv(normalizedDf, "output/normalized")
    Utils.exportToCsv(extractedDf, "output/extracted")
    Utils.exportToCsv(integratedDf, "output/integrated")
  }
}
