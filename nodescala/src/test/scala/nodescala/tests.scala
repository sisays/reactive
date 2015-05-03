package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("A delayed future should not complete before its duration") {
    val delayed = Future.delay(200 millis)
    try {
      Await.result(delayed, 199 millis)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("A delayed future should complete at its duration") {
    val delayed2 = Future.delay(200 millis)
    try {
      Await.result(delayed2, 205 millis)
    } catch {
      case t: TimeoutException => assert(false)
    }
  }

  test("Any future will return the first to complete") {
    val delayed1 = Future.delay(200 millis, () => 1)
    val delayed2 = Future.delay(100 millis, () => 2)
    val delayed3 = Future.delay(300 millis, () => 3)
    val test = Future.any(List(delayed1, delayed2, delayed3))
    test onComplete {
      case Success(v) => assert(v == 2)
      case Failure(ex) => assert(false)
    }
  }

  test("all futures will return once the last has complete") {
    val delayed1 = Future.delay(200 millis, () => 1)
    val delayed2 = Future.delay(100 millis, () => 2)
    val delayed3 = Future.delay(300 millis, () => 3)
    val test = Future.all(List(delayed1, delayed2, delayed3))

    test onComplete {
      case Success(v) => assert(v contains(1, 2, 3))
      case Failure(ex) => assert(false)
    }

    try {
      Await.result(test, 305 millis)
    } catch {
      case t: TimeoutException => assert(false)
    }
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




