import sbt.Keys._

name := "spark-ashley-madison-ml"

version := "1.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= deps

scalaVersion := "2.10.4"

lazy val deps = {
  val akkaV = "2.3.9"
  val playV = "2.3.9"
  val sparkV = "1.5.1"
  val akkaStreamV = "1.0-RC3"
  Seq(
    "mysql"                   % "mysql-connector-java"      % "5.1.+" % "compile",
    "com.typesafe.play"       %% "play-ws" 		    % playV,
    "com.lihaoyi"             %% "upickle"                  % "0.3.6",
    "com.databricks"          %% "spark-csv"                % "1.2.0",
    "org.scalamacros"         %% s"quasiquotes"             % "2.0.0" % "provided",
    "org.apache.spark"        %% "spark-core"               % sparkV,
    "org.apache.spark"        %% "spark-mllib"              % sparkV,
    "org.apache.spark"        %% "spark-sql"                % sparkV
  )
}

addCommandAlias("scraper",  "run-main io.surfkit.driver.Scraper")

addCommandAlias("house",  "run-main io.surfkit.driver.HousingPrices")

addCommandAlias("fake",  "run-main io.surfkit.driver.Fake")
 
addCommandAlias("gender",  "run-main io.surfkit.driver.Gender")


