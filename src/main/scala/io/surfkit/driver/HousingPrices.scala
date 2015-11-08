package io.surfkit.driver

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SQLContext}
import scala.Predef._

/**
 *
 * Created by Corey Auger
 */

object HousingPrices extends App{

  override def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark HousingPrices ML")
      .setMaster("local[4]")
      .set("spark.executor.memory", "8g")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //val normalizer1 = new Normalizer()

    def extractFeatureVector(r:Row) =
      LabeledPoint(r.getString(0).toDouble,
        //normalizer1.transform(Vectors.dense( (1 to (r.size-1)).map(i => r.getString(i).toDouble).toArray ))
        Vectors.dense( (1 to (r.size-1)).map(i => r.getString(i).toDouble).toArray )
      )

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/home_data.csv")

    val simple = df.select("price", "sqft_living")
                    .map(extractFeatureVector)

    val better = df.select("price","bedrooms","bathrooms","sqft_living","sqft_lot","floors","zipcode")
                    .map(extractFeatureVector)

    val advanced = df.select(
      "price", "bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "zipcode",
      "condition","grade","waterfront","view","sqft_above","sqft_basement","yr_built",
      "yr_renovated","lat", "long","sqft_living15","sqft_lot15")
        .map(extractFeatureVector)

    advanced.take(5).foreach(println)


    Seq( (simple,0.00000001), (better,0.000000001), (advanced,0.000000001)).foreach {
      case (data, step) =>
        val splits = data.randomSplit(Array(0.8, 0.2), seed = 0L)
        val training = splits(0).cache()
        val test = splits(1)

        // Building the model
        val numIterations = 500
        val model = LinearRegressionWithSGD.train(training, numIterations, step)

        // Evaluate model on training examples and compute training error
        val valuesAndPreds = training.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction)
        }
        valuesAndPreds.take(10).foreach(println)

        val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
        println("training Mean Squared Error = " + MSE)
        println("training Root Mean Squared Error = " + Math.sqrt(MSE))
    }
    sc.stop()

  }


}
