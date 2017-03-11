package sample.stream

import java.nio.file.Paths
import java.util.concurrent.ThreadLocalRandom.{current => ramdomizer}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.util.{Failure, Success}
import java.nio.file.StandardOpenOption._

/** Writes out prime numbers to `target/primes.txt` until terminated with Ctrl-C */
object WritePrimes {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher
    implicit val materializer = ActorMaterializer()

    // generate random numbers
    val maxRandomNumberSize = 1000000
    val primeSource: Source[Int, NotUsed] =
      Source
        .fromIterator(() => Iterator.continually(ramdomizer.nextInt(maxRandomNumberSize)))
        .filter(rnd => isPrime(rnd))
        .filter(prime => isPrime(prime + 2)) // ensure neighbor +2 is also prime

    val fileSink = FileIO.toPath(Paths.get("target/primes.txt"), Set(WRITE, TRUNCATE_EXISTING))

    val slowSink = Flow[Int] // act as if processing is really slow
      .map { i =>
        Thread.sleep(1000)
        ByteString(i.toString + "\n")
      }
      .toMat(fileSink)((_, bytesWritten) => bytesWritten)

    val consoleSink = Sink.foreach[Int](println)

    // send primes to both slow file sink and console sink using graph API
    val graph = GraphDSL.create(slowSink, consoleSink)((slow, _) => slow) { implicit builder =>
      (slow, console) =>
        import akka.stream.scaladsl.GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[Int](2)) // the splitter - like a Unix tee
        primeSource ~> broadcast ~> slow // connect primes to splitter, and one side to file
        broadcast ~> console // connect other side of splitter to console
        ClosedShape
    }
    val materialized = RunnableGraph.fromGraph(graph).run()

    // ensure the output file is closed and the system shutdown upon completion
    materialized.onComplete {
      case Success(_) =>
        system.terminate()

      case Failure(e) =>
        println(s"Failure: ${ e.getMessage }")
        system.terminate()
    }
  }

  protected[stream] def isPrime(n: Int): Boolean = {
    if (n <= 1) false
    else if (n == 2) true
    else !(2 until n).exists(x => n % x == 0)
  }
}
