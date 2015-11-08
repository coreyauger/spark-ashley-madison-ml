package io.surfkit.driver

import com.typesafe.config.ConfigFactory

/**
 * Created by suroot on 27/08/15.
 */
trait Setup {

  val config = ConfigFactory.load()

  lazy val nytimesEndpoint = "http://api.nytimes.com/svc/search/v2/articlesearch.json?fq=news_desk:(\"Arts\")&page=0&api-key=84ac7a74714a4c5a7c0c17751a48e204:10:72963944"

}
