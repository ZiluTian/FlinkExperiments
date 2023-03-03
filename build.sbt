ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "benchmark"

version := "0.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

// 1.16.1, 1.15.3, 1.17-snapshot
val flinkVersion = "1.16.1"
val breezeVersion = "0.13.2"

val flinkDependencies = Seq(
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" % "flink-gelly" % flinkVersion,
  "org.apache.flink" % "flink-connector-files" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies,
    libraryDependencies += "org.scalanlp" %% "breeze" % breezeVersion,
    libraryDependencies += "org.scalanlp" %% "breeze-natives" % breezeVersion,
  )

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
