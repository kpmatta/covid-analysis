
name := "covid-stats"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

libraryDependencies ++= sparkDependencies