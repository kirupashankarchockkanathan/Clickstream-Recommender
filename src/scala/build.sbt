name := "clickstream-recs"
version := "0.1.0"
scalaVersion := "2.12.18"
scalacOptions ++= Seq("-deprecation", "-feature", "-target:jvm-1.8")

val sparkV = "3.5.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"   % sparkV % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkV % Provided
)
// Kafka + JDBC connectors are supplied at submit time with --jars (see scripts/run_job.sh)
