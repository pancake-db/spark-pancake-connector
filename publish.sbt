credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

ThisBuild / organization := "com.pancakedb"
ThisBuild / organizationName := "PancakeDB"
ThisBuild / organizationHomepage := Some(url("https://github.com/pancake-db/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/pancake-db/"),
    "scm:git@github.com:pancake-db/spark-pancake-connector.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "mwlon",
    name  = "Martin Loncaric",
    email = "martin@pancakedb.com",
    url   = url("https://github.com/mwlon")
  )
)

ThisBuild / description := "PancakeDB scala client"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/pancake-db/spark-pancake-connector"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
