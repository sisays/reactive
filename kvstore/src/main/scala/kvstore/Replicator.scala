package kvstore

import akka.actor._
import akka.util.Timeout
import kvstore.Replicator.{Replicate, Snapshot, SnapshotAck}
import kvstore.SnapshotSender.{SnapshotSenderFail, SnapshotSenderAck}
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.pattern.{AskTimeoutException, after}
import akka.pattern.ask

import scala.util.{Failure, Success}

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  case class NotReplicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case r:Replicate =>
      val seq = nextSeq
      acks += seq ->(sender, r)
      context.actorOf(SnapshotSender.props(replica, r, seq))

    case SnapshotSenderAck(snapshotAck) =>
      //notify the primary replica
      acks.get(snapshotAck.seq).map{ pair => pair._1 ! Replicated(snapshotAck.key, pair._2.id) }

      //remove the seq from the map
      acks = acks - snapshotAck.seq

    case SnapshotSenderFail(replicate) =>
      //notify the primary replica
      acks.get(replicate.id).map{ pair => pair._1 ! NotReplicated(replicate.key, pair._2.id) }

      //remove the seq from the map
      acks = acks - replicate.id
  }

}

object SnapshotSender {
  case class SnapshotSenderAck(ack: SnapshotAck)
  case class SnapshotSenderFail(replicate: Replicate)
  def props(replica: ActorRef, replicate: Replicate, seq: Long): Props = Props(new SnapshotSender(replica, replicate, seq))
}

class SnapshotSender(replica: ActorRef, replicate: Replicate, seq: Long) extends Actor {
  import Replicator._

  val maxTries = 10

  var _tryCounter = 0
  def nextTry = {
    val ret = _tryCounter
    _tryCounter += 1
    ret
  }

  replica ! Snapshot(replicate.key, replicate.valueOption, seq)
  context.setReceiveTimeout(100.milliseconds)

  def receive = {
    case ack: SnapshotAck =>
      context.parent ! SnapshotSenderAck(ack)
      self ! PoisonPill

    case ReceiveTimeout =>
      if(nextTry > maxTries) {
        context.parent ! SnapshotSenderFail(replicate)
        self ! PoisonPill
      }
      else replica ! Snapshot(replicate.key, replicate.valueOption, seq)
  }
}
