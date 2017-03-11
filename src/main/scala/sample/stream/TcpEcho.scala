package sample.stream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.util.ByteString
import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

object TcpEcho {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      implicit val system = ActorSystem("ClientAndServer")
      val (address, port) = ("localhost", 6000)
      server(address, port)
      client(address, port)
    } else {
      val (address, port) = if (args.length == 3) (args(1), args(2).toInt)
                            else ("localhost", 6000)
      if (args(0) == "server") {
        server(address, port)(ActorSystem("Server"))
      } else if (args(0) == "client") {
        client(address, port)(ActorSystem("Client"))
      } else {
        Console.err.println(
          """Use without parameters to start both client and server.
            |Use parameters `server 0.0.0.0 6001` to start server listening on port 6001.
            |Use parameters `client localhost 6001` to start client connecting to server on localhost:6001.
            |""".stripMargin)
        System.exit(-1)
      }
    }
  }

  def server(address: String, port: Int)(implicit system: ActorSystem): Unit = {
    import system.dispatcher
    implicit val materializer = ActorMaterializer()

    val handler: Sink[IncomingConnection, Future[Done]] = Sink.foreach[Tcp.IncomingConnection] { conn =>
      println("Client connected from: " + conn.remoteAddress)
      conn handleWith Flow[ByteString]
    }

    Tcp()
      .bind(address, port)
      .to(handler)
      .run()
      .onComplete {
        case Success(b) =>
          println("Server started, listening on: " + b.localAddress)

        case Failure(e) =>
          println(s"Server could not bind to $address:$port: ${e.getMessage}")
          system.terminate()
      }
  }

  def client(address: String, port: Int)(implicit system: ActorSystem): Unit = {
    import system.dispatcher
    implicit val materializer = ActorMaterializer()

    val testInput: immutable.Iterable[ByteString] = ('a' to 'z').map(ByteString(_))

    Source(testInput)
      .via(Tcp().outgoingConnection(address, port))
      .runFold(ByteString.empty) { (acc, in) ⇒ acc ++ in }
      .onComplete {
        case Success(successResult) =>
          println(s"Result: " + successResult.utf8String)
          println("Shutting down client")
          system.terminate()

        case Failure(e) =>
          println("Failure: " + e.getMessage)
          system.terminate()
      }
  }
}
