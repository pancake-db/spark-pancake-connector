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
  "protobuf-java",
  "sbt-jni-core",
  "pancake",
)
assembly / assemblyExcludedJars := {
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
assembly / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "com.pancakedb" %% "spark-pancake-db-connector" % "0.1.0",

  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk-core" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % awsVersion, //frustratingly an import needed in hadoop aws
  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
)
