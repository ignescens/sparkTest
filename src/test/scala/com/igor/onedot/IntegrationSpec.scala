package com.igor.onedot

import org.specs2.mutable.Specification

class IntegrationSpec extends Specification with SparkTestHelper {

  sequential

  import spark.sqlContext.implicits._

  "Integration" should {
    "rename and drop some columns" in {
      // given
      val integration = new Integration
      val data = Seq(
        (
          "1.0",
          "Mercedes-Benz",
          "E 320",
          "E 320 Elégance 4-Matic",
          "E 320 Elégance 4-Matic",
          "MERCEDES-BENZ E 320 Elégance 4-Matic",
          "Other",
          "Limousine",
          "3199",
          "Zuzwil",
          "275 g/km",
          "Occasion",
          "10.5"
        )
      )
      val df =
        spark.sparkContext
          .parallelize(data)
          .toDF(
            "BodyColorText",
            "MakeText",
            "ModelText",
            "TypeName",
            "City",
            "BodyTypeText",
            "ID",
            "TypeNameFull",
            "ConditionTypeText",
            "Hp",
            "InteriorColorText",
            "ConsumptionTotalText",
            "extracted-value-ConsumptionTotalText"
          )

      // when
      val actualResult = df.transform(integration.toTargetSchema()).collect()

      // then
      val expectedResult = spark.sparkContext
        .parallelize(
          Seq(
            ("1.0", "Mercedes-Benz", "E 320", "E 320 Elégance 4-Matic", "E 320 Elégance 4-Matic", "MERCEDES-BENZ E 320 Elégance 4-Matic", "10.5")
          )
        )
        .toDF(
          "color",
          "make",
          "model",
          "model_variant",
          "city",
          "carType",
          "extracted-value-ConsumptionTotalText"
        )
        .collect()

      actualResult must beEqualTo(expectedResult)
    }
  }

}
