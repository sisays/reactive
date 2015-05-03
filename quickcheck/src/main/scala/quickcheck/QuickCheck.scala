package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }

  property("findMin of 2-elements heap is the min of the 2 elements") = forAll { (a: Int, b: Int) =>
    val h = insert(a, empty)
    val h2 = insert(b, h)
    findMin(h2) == Math.min(a, b)
  }

  property("deleteMin of 1-element heap is empty") = forAll { a: Int =>
    val h = insert(a, empty)
    isEmpty(deleteMin(h))
  }

  property("'deleteMin till heap is empty' gives sorted seq") = forAll { (h: H) =>
    def deleteMinAndAccumulate(h: H, l: List[Int]): List[Int] = {
      if(isEmpty(h)) l
      else {
        val m = findMin(h)
        val l2 = l:+m
        deleteMinAndAccumulate(deleteMin(h), l2)
      }
    }

    def isOrdered(l: List[Int]) = (l == l.sorted)

    isOrdered(deleteMinAndAccumulate(h, List.empty))
  }

  property("findMin of 2 melded heaps is findMin of one or the other") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == findMin(h1) || findMin(meld(h1, h2)) == findMin(h2)
  }

  property("findMin of 2 melded heaps is min of the two findMin's") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == Math.min(findMin(h1), findMin(h2))
  }

  property("meld left then findMin") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, empty)) == findMin(h1)
  }

  property("meld right then findMin") = forAll { (h1: H, h2: H) =>
    findMin(meld(empty, h2)) == findMin(h2)
  }

  property("meld then deleteMin") = forAll { (h1: H, h2: H) =>
    deleteMin(meld(h1, h2)) == deleteMin(meld(h2, h1))
  }

  property("last tricky link stuff!") = forAll { (h1: H, h2: H) =>

    def heapEqual(h1: H, h2: H): Boolean = {
      if (isEmpty(h1) && isEmpty(h2)) true
      else {
        val m1 = findMin(h1)
        val m2 = findMin(h2)
        m1 == m2 && heapEqual(deleteMin(h1), deleteMin(h2))
      }
    }

    val m1 = findMin(h1)
    heapEqual(meld(h1, h2), meld(deleteMin(h1), insert(m1, h2)))
  }

  lazy val genHeap: Gen[H] =  for {
    m <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(m, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
