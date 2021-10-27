name := "spark-pancake-db-connector"

version := "0.0.0-alpha.0"

scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java-util" % "3.18.1",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.pancakedb" %% "pancake-db-client" % "0.0.0-alpha.0-full",

  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
)
