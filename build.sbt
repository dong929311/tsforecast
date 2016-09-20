name := "tsforecast"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.cloudera.sparkts" % "sparkts" % "0.4.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"
)
    