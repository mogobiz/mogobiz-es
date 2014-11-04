package com.mogobiz.es

import java.io.File

import com.sksamuel.elastic4s.ElasticDsl._
import spray.client.pipelining._
import spray.http._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object Mapping {
  def clear = Await.result(EsClient.client.execute(delete index com.mogobiz.es.Settings.ElasticSearch.Index), Duration.Inf)

  def set() {
    def route(url: String) = "http://" + Settings.ElasticSearch.FullUrl + url
    def mappingFor(name: String) = new File(this.getClass.getClassLoader.getResource(s"es/mappings/$name.json").toURI)

    implicit val system = akka.actor.ActorSystem("mogopay-boot")
    val pipeline: HttpRequest => scala.concurrent.Future[HttpResponse] = sendReceive

    EsClient.client.sync.execute(create index com.mogobiz.es.Settings.ElasticSearch.Index)

    mappingFiles foreach { name =>
      val url = s"/${Settings.ElasticSearch.Index}/$name/_mapping"
      val mapping = scala.io.Source.fromFile(mappingFor(name)).mkString
      val x: Future[Any] = pipeline(Post(route(url), mapping)) map { response: HttpResponse =>
        response.status match {
          case StatusCodes.OK => System.err.println(s"The mapping for `$name` was successfully set.")
          case _ => System.err.println(s"Error while setting the mapping for `$name`: ${response.entity.asString}")
        }
      }
      Await.result(x, 10 seconds)
    }

    system.shutdown
  }

  private def mappingFiles = {
    val dir = new File(this.getClass.getClassLoader.getResource(s"es/mappings").toURI)
    dir.listFiles.map(_.getName.split('.')(0))
  }
}
