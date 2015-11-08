package io.surfkit.driver

import java.io.InputStream

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

import scala.Predef._

/**
 *
 * Created by Corey Auger
 */

object Fake extends App with LocalSetup{

  override def main(args: Array[String]) {

    // 0 = Man       // mapped from 2
    // 1 = Woman

    val df = sqlContext.sql(
      """
        |SELECT profile_caption
        |FROM members
        |WHERE gender = 1
      """.stripMargin
    )

    /*val df = sqlContext.sql(
      """
        |SELECT gender, pref_lookingfor_abstract
        |FROM members
        |WHERE pref_lookingfor_abstract <> ''
      """.stripMargin
    )
*/
    //pref_lookingfor_abstract

    df.take(10).foreach(println)

    val stream : InputStream = getClass.getResourceAsStream("/stopwords.txt")
    val stopWords = scala.io.Source.fromInputStream( stream )
      .getLines
      .map(_.trim.toLowerCase.replaceAll("[^\\w\\s]"," "))      // remove punctuation
      .toSet


    def tokenizeAndClean(s:String):Seq[String] =
      (s.toLowerCase.replaceAll("[^\\w\\s]","").split(" "))
      .map(_.trim).filter(_ != "")
        .filter(w => !stopWords.contains(w))
        .toSeq


    val profiles = df
      .map(r =>
        ( // tokenize, convert to lowercase, and remove stop words.
            tokenizeAndClean( (if(r.isNullAt(0))"" else r.getString(0))  )    // profile_caption
        )
      )

    profiles.take(10).foreach(println)

    val hashingTF = new HashingTF(10000)
    val tf: RDD[Vector] = hashingTF.transform(profiles)

    println("done tf")

    tf.cache()
    val idf = new IDF(minDocFreq = 1).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    println("done idf")


    // Cluster the data into two classes using KMeans
    val numClusters = 4
    val numIterations = 25
    val clusters = KMeans.train(tfidf, numClusters, numIterations)

    val profileClusters = profiles
      .zip(tfidf)
      .groupBy(p => clusters.predict(p._2))

    var total = 0
    profileClusters.collect.map{ c =>
      val sorted = c._2.map(_._1.mkString(" ")).toSeq.sorted
      total = total + sorted.length
      println(s"Cluster ${c._1} of size ${sorted.length}")
      sorted.take(15).foreach(println)
      println("")
    }

    println(s"\nTOTAL ${total}")

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(tfidf)
    println("Within Set Sum of Squared Errors = " + WSSSE)


    sc.stop()

  }



}
