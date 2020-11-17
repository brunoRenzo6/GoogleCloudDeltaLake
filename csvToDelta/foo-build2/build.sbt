ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "com.example"

lazy val hello = (project in file("."))
  .settings(
    name := "Hello",
	
	libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
	libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7" % "provided",
	libraryDependencies += "io.delta" %% "delta-core" % "0.4.0",

  )