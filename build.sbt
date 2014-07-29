scalaVersion := "2.11.1"

resolvers ++= Seq(
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
  "Anormcypher Repository" at "http://repo.anormcypher.org/"
)

libraryDependencies ++= Seq(
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.1",
  "com.github.tototoshi" %% "scala-csv" % "1.0.0",
  "org.anormcypher" %% "anormcypher" % "0.5.1"
)
