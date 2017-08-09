package org.apache.flink.quickstart

import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncCollector, AsyncFunction}

import scala.concurrent.{ExecutionContext, Future}

class AsyncDatabaseRequest extends AsyncFunction[String, (String, String)] {

  /** The database specific client that can issue concurrent requests with callbacks */
//  lazy val client: DatabaseClient = new DatabaseClient(host, post, credentials)

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())


  override def asyncInvoke(str: String, asyncCollector: AsyncCollector[(String, String)]): Unit = {

    // issue the asynchronous request, receive a future for the result
//    val resultFuture: Future[String] = client.query(str)

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the collector
//    resultFuture.onSuccess {
//      case result: String => asyncCollector.collect(Iterable((str, result)));
//    }
  }
}
