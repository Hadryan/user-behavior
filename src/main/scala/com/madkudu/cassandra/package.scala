package com.madkudu

import com.datastax.driver.core.{PreparedStatement, ResultSet, Session, SimpleStatement}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions

package object cassandra {

  implicit def listenableFutureToFuture[T](
                                            listenableFuture: ListenableFuture[T]
                                          ): Future[T] = {
    val promise = Promise[T]()
    Futures.addCallback(listenableFuture, new FutureCallback[T] {
      def onFailure(error: Throwable): Unit = {
        promise.failure(error)
        ()
      }
      def onSuccess(result: T): Unit = {
        promise.success(result)
        ()
      }
    })
    promise.future
  }

  implicit class CqlStrings(val context: StringContext) extends AnyVal {
    def cql(args: Any*)(implicit session: Session): Future[PreparedStatement] = {
      val statement = new SimpleStatement(context.raw(args: _*))
      session.prepareAsync(statement)
    }
  }

  def execute(statement: Future[PreparedStatement], params: Any*)(
    implicit executionContext: ExecutionContext, session: Session
  ): Future[ResultSet] =
    statement
      .map(_.bind(params.map(_.asInstanceOf[Object])))
      .flatMap(session.executeAsync(_))
}
