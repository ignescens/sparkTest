package com.igor.onedot

import org.specs2.mutable.Specification

class ExtractionSpec extends Specification with SparkTestHelper {

  sequential

  import spark.sqlContext.implicits._

  "Extraction" should {
    "correctly extract consumption unit and value into corresponding columns" in {
      // given
      val extraction = new Extraction
      val data       = Seq("11.5 l/100km", "5 l/100km", null)
      val df =
        spark.sparkContext
          .parallelize(data)
          .toDF("ConsumptionTotalText")

      //when
      val actualResult = df.transform(extraction.consumptionData()).collect()

      // then
      val expectedResult = spark.sparkContext
        .parallelize(Seq(("11.5 l/100km", "11.5", "l/100km"), ("5 l/100km", "5", "l/100km"), (null, null, null)))
        .toDF("ConsumptionTotalText", "extracted-value-ConsumptionTotalText", "extracted-unit-ConsumptionTotalText")
        .collect()

      actualResult must beEqualTo(expectedResult)
    }
  }
}
