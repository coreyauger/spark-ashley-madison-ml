package io.surfkit.driver

import play.api.libs.ws.DefaultWSClientConfig
import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder
import play.api.libs.ws.ning.NingWSClient
import com.ning.http.client.AsyncHttpClientConfig
import scala.concurrent.ExecutionContext.Implicits.global

import scala.Predef._

/**
 *
 * Created by Corey Auger
 */

object Scraper extends App with LocalSetup{

  override def main(args: Array[String]) {

    import sqlContext.implicits._

    val config = new NingAsyncHttpClientConfigBuilder(DefaultWSClientConfig()).build
    val builder = new AsyncHttpClientConfig.Builder(config)
    val WS = new NingWSClient(builder.build)

    val p = new java.io.PrintWriter("./output/opento.json")

    WS.url(nytimesEndpoint).get.map{ res =>
      println(res.json)

    }


    p.close()
    sc.stop()

  }



}
