name := "spark-pancake-db-connector"

version := "0.2.1"

scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.pancakedb" % "pancake-db-idl" % "0.2.0",
  "com.pancakedb" %% "pancake-db-client" % "0.2.1",

  "io.netty" % "netty-all" % "4.1.72.Final", // conflict resolution

  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
)
