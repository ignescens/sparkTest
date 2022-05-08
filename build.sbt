name := "onedot"

version := "0.1"

scalaVersion := "2.13.8"

val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion,
  "org.apache.spark" %% "spark-sql"       % sparkVersion,
  "org.specs2"       % "specs2-mock_2.13" % "4.10.5" % Test
)
