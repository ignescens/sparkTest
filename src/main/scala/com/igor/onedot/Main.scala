package com.igor.onedot


import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SessionHelper.build("onedot-supplier-processing")

    val loadSupplierData = new LoadSupplierData("src/main/resources/supplier_car.json")
    val normalisation = new Normalisation
    val extraction = new Extraction
    val integration = new Integration

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

    integratedDf.show(10)

    Utils.exportToCsv(preProcessedDf, "output/pre-processed")
    Utils.exportToCsv(normalizedDf, "output/normalized")
    Utils.exportToCsv(extractedDf, "output/extracted")
    Utils.exportToCsv(integratedDf, "output/integrated")
  }
}
