package com.igor.onedot

import org.specs2.mutable.Specification

class LoadSupplierDataSpec extends Specification with SparkTestHelper {

  sequential

  val loadSupplierData = new LoadSupplierData("src/test/scala/resources/supplier_car.json")

  import spark.sqlContext.implicits._

  "LoadSupplierData" should {
    "correctly load data from Json" in {
      // given loadSupplierData

      // when
      val actualResult = loadSupplierData.fromJson.collect()

      // then
      val expectedResult = spark.sparkContext
        .parallelize(
          Seq(
            ("Hp", "389", "946.0", "CHRYSLER", "VIPER", "Viper GTS", "Viper GTS", "CHRYSLER Viper GTS", "3"),
            ("TransmissionTypeText", "Schaltgetriebe manuell", "946.0", "CHRYSLER", "VIPER", "Viper GTS", "Viper GTS", "CHRYSLER Viper GTS", "2"),
            ("InteriorColorText", "schwarz", "946.0", "CHRYSLER", "VIPER", "Viper GTS", "Viper GTS", "CHRYSLER Viper GTS", "1")
          )
        )
        .toDF("attributeNames", "attributeValues", "ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull", "entityId")
        .collect()

      actualResult must beEqualTo(expectedResult)
    }

    "correctly preprocess/pivot data" in {
      // given
      val data = Seq(
        ("Hp", "389", "946.0", "CHRYSLER", "VIPER", "Viper GTS", "Viper GTS", "CHRYSLER Viper GTS", "3"),
        ("TransmissionTypeText", "Schaltgetriebe manuell", "946.0", "CHRYSLER", "VIPER", "Viper GTS", "Viper GTS", "CHRYSLER Viper GTS", "2"),
        ("InteriorColorText", "schwarz", "946.0", "CHRYSLER", "VIPER", "Viper GTS", "Viper GTS", "CHRYSLER Viper GTS", "1")
      )
      val df = spark.sparkContext
        .parallelize(data)
        .toDF("attributeNames", "attributeValues", "ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull", "entityId")

      // when
      val actualResult = df.transform(loadSupplierData.preProcessDf()).collect()

      // then
      val expectedResult = spark.sparkContext
        .parallelize(
          Seq(
            ("946.0", "CHRYSLER", "VIPER", "Viper GTS", "Viper GTS", "CHRYSLER Viper GTS", "389", "schwarz", "Schaltgetriebe manuell")
          )
        )
        .toDF("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull", "Hp", "InteriorColorText", "TransmissionTypeText")
        .collect()

      actualResult must beEqualTo(expectedResult)
    }
  }
}
