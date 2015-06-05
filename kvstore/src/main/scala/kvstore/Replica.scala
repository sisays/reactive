package kvstore

import java.util.UUID

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence._
import kvstore.PersistenceSender._
import kvstore.Replicator.SnapshotAck
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // seq -> list of actors for which we are expecting a reply
  var unAckOperations = Map.empty[Long, Set[ActorRef]]

  // Register against the arbiter once created
  arbiter ! Join

  var persistenceActor = context.actorOf(persistenceProps)

  var _expectedSeq = 0L
  def nextSeq = {
    val ret = _expectedSeq
    _expectedSeq += 1
    ret
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {

    case Replicas(replicas: Set[ActorRef]) =>
      // For new replicas
      replicas.filter(r => !replicators.contains(r)).zip(kv.toList).map {
        case (r, (k, v)) =>
          val replicator = context.actorOf(Replicator.props(r))
          replicators += replicator
          secondaries += r -> replicator
          replicator ! Replicate(k, Some(v), System.currentTimeMillis())
      }
      // For replica that is removed
      secondaries.map {
        case (replica, replicator) =>
          if(!replicas.contains(replica)){
            replicator ! PoisonPill
            replicators = replicators - replicator
            secondaries = secondaries - replica
          }
      }

    case Insert(key: String, value: String, id: Long) =>
      kv += key -> value
      val persistenceSender = context.actorOf(PersistenceSender.props(persistenceActor, sender, Persist(key, Some(value), id)))
      unAckOperations(id) += persistenceSender
      replicators map { r =>
        r ! Replicate(key, Some(value), id)
        unAckOperations(id) += r
      }

    case Remove(key: String, id: Long) =>
      kv = kv - key
      val persistenceSender = context.actorOf(PersistenceSender.props(persistenceActor, sender, Persist(key, None, id)))
      unAckOperations(id) += persistenceSender
      replicators map {
        r => r ! Replicate(key, None, id)
        unAckOperations(id) += r
      }

    case PersistAck(p: Persisted, origin: ActorRef) =>
      unAckOperations(p.id) -= sender
      if(unAckOperations(p.id).isEmpty){
        unAckOperations -= p.id
        origin ! OperationAck(p.id)
      }

    case PersistFail(p: Persist, origin: ActorRef) =>
      unAckOperations -= p.id
      origin ! OperationFailed(p.id)

    case Get(key: String, id: Long) => sender ! GetResult(key, kv.get(key), id)

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key: String, id: Long) => sender ! GetResult(key, kv.get(key), id)
    case op:Operation => sender ! OperationFailed(op.id)

    case Snapshot(key: String, valueOption: Option[String], seq: Long) if(seq > _expectedSeq) =>
    case Snapshot(key: String, valueOption: Option[String], seq: Long) if(seq < _expectedSeq) =>
      sender ! SnapshotAck(key, seq)
    case Snapshot(key: String, valueOption: Option[String], seq: Long) if(seq == _expectedSeq) =>
      kv = valueOption match {
        case Some(v) => kv + (key -> v)
        case None => kv - key
      }
      context.actorOf(PersistenceSender.props(persistenceActor, sender, Persist(key, valueOption, seq)))

    case PersistAck(p: Persisted, origin: ActorRef) =>
      origin ! SnapshotAck(p.key, p.id)
  }

}


object PersistenceSender {
  case class PersistAck(persisted: Persisted, sender: ActorRef)
  case class PersistFail(persist: Persist, sender: ActorRef)
  def props(persistence: ActorRef, origin: ActorRef, persist: Persist): Props = Props(new PersistenceSender(persistence, origin, persist))

}

class PersistenceSender(persistence: ActorRef, origin: ActorRef, persist: Persist) extends Actor {
  val maxTries = 9
  var _tryCounter = 0
  def nextTry = {
    val ret = _tryCounter
    _tryCounter += 1
    ret
  }
  persistence ! persist
  context.setReceiveTimeout(100.milliseconds)

  def receive = {
    case p: Persisted =>
      context.parent ! PersistAck(p, origin)
      self ! PoisonPill

    case ReceiveTimeout =>
      if (nextTry > maxTries) {
        context.parent ! PersistFail(persist, origin)
        self ! PoisonPill
      }
      else persistence ! persist
  }
}