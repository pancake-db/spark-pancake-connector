name := "spark-pancake-db-connector-examples"

version := "0.0.0"

scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"
val awsVersion = "1.11.563"
val hadoopVersion = "3.2.2"

ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadepb.@1").inAll,
  ShadeRule.rename("com.google.gson.**" -> "shadegson.@1").inAll,
)

val assemblyIncludedJars = Array[String](
  "aws-java-sdk-core",
  "aws-java-sdk-dynamodb", //frustratingly an import needed in hadoop aws
  "aws-java-sdk-s3",
  "gson",
  "hadoop-aws",
  "idl",
  "protobuf-java",
  "sbt-jni-core",
  "spark-pancake",
)
assemblyExcludedJars in assembly := {
  val cp = (assembly / fullClasspath).value
  println("ASSEMBLY JARS:")
  cp filter {classPath =>
    val include = assemblyIncludedJars.exists(pattern =>
      classPath.data.getName.contains(pattern)
    )
    if (include) {
      println(s"\t${classPath.data.getName}")
    }
    !include
  }
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.google.protobuf" % "protobuf-java-util" % "3.18.1",
  "com.github.sbt" %% "sbt-jni-core" % "1.5.3",
  "com.pancakedb" %% "pancake-db-client" % "0.0.0-alpha.0",

  "com.github.sbt" %% "sbt-jni-core" % "1.5.3",
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk-core" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % awsVersion, //frustratingly an import needed in hadoop aws
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
)
