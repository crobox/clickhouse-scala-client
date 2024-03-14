package com.crobox.clickhouse

import com.typesafe.config.Config
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object TestCase extends App {


  //val config: Config
  val queryDatabase: String = "default"
  //implicit val system: ActorSystem

  val client = new ClickhouseClient()
  val res = client.query("SELECT version()").map(result => {
    println(s"Got query result $result")
  })
  Await.result(res, 10.seconds)
}
