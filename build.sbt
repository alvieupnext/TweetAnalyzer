ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "BDAssignment"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

libraryDependencies += "com.lihaoyi" %% "ujson" % "3.1.3"

/* .... your libraryDependencies */

/* Define here your Main Class */
val mainClassName = "MLR"

/* Define here the name of your jar */
val outputJarName = "linreg.jar"

/* Make sure this is the same as scalaVersion defined above in your build.sbt */
val sVer = "2.13.12"

/* The rest of the code prepares your project for using sbt assembly */

Compile / mainClass := Some(mainClassName)

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.example",
  scalaVersion := sVer,
  assembly / test := {}
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    assembly / mainClass := Some(mainClassName)
  )

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard

  case x =>
    MergeStrategy.first
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
assembly / assemblyJarName := outputJarName

/* you need to be able to undo the "provided" annotation on the deps when running your spark
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
Runtime / fullClasspath  := (fullClasspath in (Compile, run)).value

