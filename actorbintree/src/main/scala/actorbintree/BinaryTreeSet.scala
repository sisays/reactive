/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply
}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context become garbageCollecting(newRoot)
    }
    case op: Operation => root ! op
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => None
    case CopyFinished => {
      //kill the old tree actors
      root ! PoisonPill

      //replay all enqueued operations
      pendingQueue map (op => newRoot ! op)
      pendingQueue = Queue.empty[Operation]

      //update the root ref
      root = newRoot

      // switch back to normal mode
      context become normal
    }
    case op: Operation => pendingQueue = pendingQueue.enqueue(op)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case CopyTo(treeNode) => {
      val children = Set.empty[ActorRef] ++ subtrees.values

      if(removed){
        if (children.isEmpty){
          context.parent ! CopyFinished
        }
      } else {
        treeNode ! Insert(self, 10000, this.elem)
      }
      children foreach { _ ! CopyTo(treeNode) }
      context become copying(children, removed)
    }

    case op: Operation if (op.elem < elem && subtrees.isDefinedAt(Left)) => subtrees(Left) ! op
    case op: Operation if (op.elem > elem && subtrees.isDefinedAt(Right)) => subtrees(Right) ! op

    case Insert(requester, id, elem) =>
      if(elem == this.elem){
        removed = false
        requester ! OperationFinished(id)
      } else {
        val child = context.actorOf(props(elem, false))
        if (elem < this.elem) subtrees += Left -> child
        else subtrees += Right -> child
        requester ! OperationFinished(id)
      }

    case Contains(requester, id, elem) =>
      if(elem == this.elem) requester ! ContainsResult(id, !removed)
      else requester ! ContainsResult(id, false)

    case Remove(requester, id, elem) =>
      if(elem == this.elem) {
        removed = true
        requester ! OperationFinished(id)
      } else requester ! OperationFinished(id)
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {

    case OperationFinished(10000) => {
      if(expected.isEmpty) context.parent ! CopyFinished
      else context become copying(expected, true)
    }

    case CopyFinished => {
      if (insertConfirmed && expected.size == 1) context.parent ! CopyFinished
      else context become copying(expected - sender, insertConfirmed)
    }
  }
}
