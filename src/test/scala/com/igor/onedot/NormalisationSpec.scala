package com.igor.onedot

import org.specs2.mutable.Specification

class NormalisationSpec extends Specification with SparkTestHelper {

  sequential

  import spark.sqlContext.implicits._

  val normalisation = new Normalisation

  "Normalisation" should {
    "normalize BodyColorText" in {
      // given
      val data = Seq("beige", "grün metalic", "falsche farbe")
      val df =
        spark.sparkContext
          .parallelize(data)
          .toDF("BodyColorText")

      //when
      val actualResult = df.transform(normalisation.normalizeColor()).collect()

      // then
      val expectedResult = spark.sparkContext
        .parallelize(Seq("Beige", "Green", "Other"))
        .toDF("BodyColorText")
        .collect()

      actualResult must beEqualTo(expectedResult)
    }

    "normalize MakeText" in {
      // given
      val data = Seq("BMW", "MERCEDES-BENZ", "ALFA ROMEO")
      val df =
        spark.sparkContext
          .parallelize(data)
          .toDF("MakeText")

      //when
      val actualResult = df.transform(normalisation.normalizeMake()).collect()

      // then
      val expectedResult = spark.sparkContext
        .parallelize(Seq("BMW", "Mercedes-Benz", "Alfa Romeo"))
        .toDF("MakeText")
        .collect()

      actualResult must beEqualTo(expectedResult)
    }
  }
}
