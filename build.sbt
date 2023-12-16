ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "BDAssignment"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

libraryDependencies += "com.lihaoyi" %% "ujson" % "3.1.3"

