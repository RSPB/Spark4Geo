name := "Spark4Geo"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.datasyslab" % "sernetcdf" % "0.1.0",
  "org.datasyslab" % "geospark" % "0.8.0"
)