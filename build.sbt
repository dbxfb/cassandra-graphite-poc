lazy val commonSettings = Seq(
  organization := "com.criteo.sre.storage",
  version := "0.0.1",
  scalaVersion := "2.11.6"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Singularity",

    // Dependencies
    libraryDependencies ++= Seq(
      "org.apache.lucene" % "lucene-core" % "5.1.0",
      "org.apache.lucene" % "lucene-analyzers-common" % "5.1.0",
      "org.apache.lucene" % "lucene-facet" % "5.1.0"
    ),

    // Testing
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.2.4" % "test"
    ),

    // Remove locally copied JARs from assembly output
    assemblyExcludedJars in assembly ++= (unmanagedJars in Compile).value
  )
