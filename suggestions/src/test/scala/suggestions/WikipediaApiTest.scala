package suggestions



import language.postfixOps
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import rx.lang.scala._
import org.scalatest._
import gui._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class WikipediaApiTest extends FunSuite {

  object mockApi extends WikipediaApi {
    def wikipediaSuggestion(term: String) = Future {
      if (term.head.isLetter) {
        for (suffix <- List(" (Computer Scientist)", " (Footballer)")) yield term + suffix
      } else {
        List(term)
      }
    }
    def wikipediaPage(term: String) = Future {
      "Title: " + term
    }
  }

  import mockApi._

  test("WikipediaApi should make the stream valid using sanitized") {
    val notvalid = Observable.just("erik", "erik meijer", "martin")
    val valid = notvalid.sanitized

    var count = 0
    var completed = false

    val sub = valid.subscribe(
      term => {
        assert(term.forall(_ != ' '))
        count += 1
      },
      t => assert(false, s"stream error $t"),
      () => completed = true
    )
    assert(completed && count == 3, "completed: " + completed + ", event count: " + count)
  }

  test("Observable(1, 2, 3).zip(Observable.interval(300 millis)).timedOut(1L) should return the 3 values, and complete without errors"){
    val values: Observable[(Int,Long)] = Observable.just[Int](1, 2, 3).zip(Observable.interval(300 millis)).timedOut(1L)
    val observed = mutable.Buffer[Int]()
    var hasCompleted = false
    val sub = values subscribe (
      n => observed += n._1,
      t => assert(false, "The observable failed!"),
      () => hasCompleted = true
      )
    Thread.sleep(1500)

    assert(observed == Seq(1, 2, 3), observed)
    assert(hasCompleted)
  }

  test("Observable(1, 2, 3).zip(Observable.interval(700 millis)).timedOut(1L) should return the first value, and complete without errors"){
    val values: Observable[(Int,Long)] = Observable.just[Int](1, 2, 3).zip(Observable.interval(700 millis)).timedOut(1L)
    val observed = mutable.Buffer[Int]()
    var hasCompleted = false
    val sub = values subscribe (
      n => observed += n._1,
      t => assert(false, "The observable failed!"),
      () => hasCompleted = true
      )
    Thread.sleep(3000)

    assert(observed == Seq(1), observed)
    assert(hasCompleted)
  }

  test("WikipediaApi should correctly use concatRecovered") {
    val requests = Observable.just(1, 2, 3)
    val remoteComputation = (n: Int) => Observable.just(0 to n : _*)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 2 + 1 + 2 + 3), s"Sum: $total")
  }

}
