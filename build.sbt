name := "quasar-spark-connector"

version := "1.0"

scalaVersion := "2.10.4"

exportJars := true

unmanagedBase := baseDirectory.value / "lib"

unmanagedJars in Compile += file("lib/rados-1.0-SNAPSHOT.jar")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.scala-lang" % "scala-library" % "2.10.4" % "provided",
  "org.mongodb" % "mongo-java-driver" % "3.0.1"
)