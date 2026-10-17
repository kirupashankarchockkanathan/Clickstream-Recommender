# Compile the Scala project inside a Java 8 + sbt container (nothing to install on Windows).
# Output: src/scala/target/scala-2.12/clickstream-recs_2.12-0.1.0.jar
$root = Split-Path -Parent $PSScriptRoot
docker run --rm -v "${root}\src\scala:/work" -v bda-sbt-cache:/root/.cache -v bda-ivy-cache:/root/.ivy2 -w /work `
  sbtscala/scala-sbt:eclipse-temurin-jammy-8u402-b06_1.9.9_2.12.18 sbt -batch package
