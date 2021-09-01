name := "FindAllPathsForPairsGraphX"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-graphx" % "3.0.1" % "provided",
  "com.lihaoyi" %% "upickle" % "1.4.0",
  "com.lihaoyi" %% "os-lib" % "0.7.8"
)

conflictManager := ConflictManager.latestRevision

mainClass in assembly := Some("FindPathsMain")

resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", _*) => MergeStrategy.last
  case PathList("javax", "inject", _*) => MergeStrategy.last
  case PathList("javax", "servlet", _*) => MergeStrategy.last
  case PathList("javax", "activation", _*) => MergeStrategy.last
  case PathList("org", "apache", _*) => MergeStrategy.last
  case PathList("com", "google", _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", _*) => MergeStrategy.last
  case PathList("com", "codahale", _*) => MergeStrategy.last
  case PathList("com", "yammer", _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt" => MergeStrategy.discard
  case "git.properties" => MergeStrategy.first
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName := "FindAllPathsForPairsGraphX-assembly-3.0.1-1.3.4.jar"