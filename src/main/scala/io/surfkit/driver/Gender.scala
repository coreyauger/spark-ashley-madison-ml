package io.surfkit.driver

import java.io.InputStream

import io.surfkit.data.Data
import io.surfkit.data.Data.NGramStats
import org.apache.spark.mllib.classification.{NaiveBayes, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Vector}


import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.IDF

import scala.Predef._
import io.surfkit.data._
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.stat. {MultivariateStatisticalSummary, Statistics}
/**
 *
 * Created by Corey Auger
 */

object Gender extends App with LocalSetup{

  override def main(args: Array[String]) {

    // 0 = Man       // mapped from 2
    // 1 = Woman

  //  val p = new java.io.PrintWriter("./output/gender.json")


    val df = sqlContext.sql(
      """
        |SELECT gender, profile_caption
        |FROM members
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
      .map(_.trim.toLowerCase.replaceAll("[^\\w\\s]",""))      // remove punctuation
      .toSet

    import sqlContext.implicits._


    def tokenizeAndClean(s:String):Seq[String] =
      (s.toLowerCase.replaceAll("[^\\w\\s]","").split(" "))
      .map(_.trim).filter(_ != "")
        .filter(w => !stopWords.contains(w))
        .toSeq


    val profiles = df
      .map(r =>
        ( // tokenize, convert to lowercase, and remove stop words.
          (r.getInt(0),
            tokenizeAndClean( (if(r.isNullAt(1))"" else r.getString(1)) )  )    // profile_caption
        )
      )

    profiles.take(10).foreach(println)

    val hashingTF = new HashingTF(10000)
    val tf: RDD[Vector] = hashingTF.transform(profiles.map(_._2))

    println("done tf")

    tf.cache()
    val idf = new IDF(minDocFreq = 1).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    println("done idf")

    val dataFull = profiles
      .map(s => if(s._1 == 2) 0.0 else 1.0 ).zip(tfidf)
      .map(p => LabeledPoint(p._1, p._2))

    val data = dataFull.zipWithIndex.filter(l => l._1.label == 1.0 || l._2 % 8 == 0 ).map(_._1)

    val total = data.count().toDouble
    val men = data.filter(_.label == 0.0).count().toDouble / total
    println(s"Total ${total}")
    println(s"% men     ${men}")
    println(s"% women   ${(1.0-men)}")

    //val summary = Statistics.colStats(tfidf)
    //print("Max :");summary.max.toArray.foreach(m => print("%5.1f |     ".format(m)));println
    //print("Min :");summary.min.toArray.foreach(m => print("%5.1f |     ".format(m)));println
    //print("Mean :");summary.mean.toArray.foreach(m => print("%5.1f     | ".format(m)));println

    println("got data..")

    data.take(10).foreach(println)

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Building the model
    val nbModel = NaiveBayes.train(training)

    val predictionAndLabel = test.map{p =>
      (nbModel.predict(p.features), p.label)
    }
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(s"NB ${accuracy}")

    predictionAndLabel.take(20).foreach(println)

    // simple women
    val women = sc.parallelize( Seq(
      "Preferably someone local. No drama or unreasonable relationship expectations from me or you. Mutual respect and absolute discretion a must.",
      "I am looking for someone who can handle a casual encounter with no strings.  I am very busy and am looking to have some fun. If you think you can handle it...lets connect",
      "looking goodfeeling greatnow communicate",
      "I want a women that knows",
      "i may be spoken 4 but i speak 4 myself",
      "I need a guy who can take control."
    ))
    val womenVec = idf.transform(hashingTF.transform(women.map(tokenizeAndClean)))
    womenVec.foreach(println)
    nbModel.predict(womenVec).foreach(println)


    sc.stop()

  }



}
