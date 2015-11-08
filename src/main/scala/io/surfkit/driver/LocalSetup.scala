package io.surfkit.driver

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by suroot on 27/08/15.
 */
trait LocalSetup extends Setup{

  val conf = new SparkConf()
    .setAppName("Spark Ashley Madison ML")
    .setMaster("local[4]")
    .set("spark.executor.memory", "8g")

  println("loading spark conf")
  val sc = new SparkContext(conf)
  // Read the data from MySql (JDBC)
  // Load the driver
  Class.forName("com.mysql.jdbc.Driver")

  println("get sql context")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val df = sqlContext.load("jdbc", Map(
    "url" -> config.getString("database"),
    //"dbtable" -> "am_am_member",
    //"dbtable" -> "am_tmp",
    "dbtable" -> "am_am_member2",            // small subset (1M) records.
    "user" -> config.getString("dbuser"),
    "password" -> config.getString("password") ))
    .registerTempTable("members")


}
