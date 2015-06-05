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
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.util.Timeout
import akka.event.Logging
import akka.pattern.after

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
  val log = Logging(context.system, this)
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]

  // a map from id to requester
  var senders = Map.empty[Long, ActorRef]

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
      replicas.filter(r => !secondaries.contains(r) && r != self).map {
        newReplica =>
          log.info("Creating replicator for new replica {}", newReplica)
          val newReplicator = context.actorOf(Replicator.props(newReplica))
          replicators += newReplicator
          secondaries += newReplica -> newReplicator
          kv.foreach {
            case (k, v) =>
              log.info("Sending all {}={} to replicator {}", k, v, newReplicator)
              newReplicator ! Replicate(k, Some(v), System.currentTimeMillis())
          }
      }
      // For replica that is removed
      secondaries.foreach {
        case (replica, replicator) =>
          if(!replicas.contains(replica)){

            unAckOperations.foreach {
              case (id, operators) =>
                if(operators.contains(replicator)) {
                  log.info("Acknowledge operation() for this operator", id, replicator)
                  ackReception(id, replicator)
                  if (!unAckOperations.contains(id)) {
                    senders.get(id) match {
                      case Some(origin) =>
                        log.info("Primary sends OperationAck({}) back to {}", id, origin)
                        origin ! OperationAck(id)
                        senders -= id
                      case None =>
                    }
                  }
                }
            }
            log.info("Remove replicator {} for replica {}", replicator, replica)
            replicator ! PoisonPill
            replicators = replicators - replicator
            secondaries = secondaries - replica
          }
      }

    case Insert(key: String, value: String, id: Long) =>
      log.info("Insert command {} ({} = {}) received from {}", id, key, value, sender)
      kv += key -> value

      log.info("Storing {} -> {}", id, sender)
      val caller = sender
      senders += id -> sender

      log.info("Create persistenceSender to send {}, {}={} to {}", id, key, value, sender)
      val persistenceSender = context.actorOf(PersistenceSender.props(persistenceActor, sender, key, Some(value), id))
      recordExpectation(id, persistenceSender)

      replicators map { r =>
        log.info("Primary sends Replicate({}, {}, {}) to replicator {}", key, value, id, r)
        r ! Replicate(key, Some(value), id)
        recordExpectation(id, r)
      }
      after(1000.millis, using = context.system.scheduler)(Future.apply {
        if(unAckOperations.contains(id)){
          log.info("Too late, primary sends a failure ({}) back to the sender", id)
          caller ! OperationFailed(id)
          senders -= id
          unAckOperations -= id

        }

      })

    case Remove(key: String, id: Long) =>
      log.info("Remove command {} ({}) received from {}", id, key, sender)
      kv = kv - key
      senders += id -> sender
      val caller = sender
      val persistenceSender = context.actorOf(PersistenceSender.props(persistenceActor, sender, key, None, id))
      recordExpectation(id, persistenceSender)
      replicators map {
        r => r ! Replicate(key, None, id)
        recordExpectation(id, r)
      }
      after(1000.millis, using = context.system.scheduler)(Future.apply {
        log.info("Too late, primary sends a failure ({}) back to the sender", id)
        caller ! OperationFailed(id)
        senders -= id
        unAckOperations -= id
      })

    case PersistAck(p: Persisted, origin: ActorRef) =>
      log.info("Primary received PersistAck({}, {})", p, origin)
      ackReception(p.id, sender)
      if(!unAckOperations.contains(p.id)){
        log.info("Primary sends OperationAck({}) back to {}", p.id, origin)
        origin ! OperationAck(p.id)
        senders -= p.id
      }

    case PersistFail(key: String, id: Long, origin: ActorRef) =>
      log.info("Primary received PersistFail({}, {}, {}) from PersistenceSender", key, id, origin)
      origin ! OperationFailed(id)
      unAckOperations -= id
      senders -= id

    case Replicated(key: String, id: Long) =>
      log.info("Primary received Replicated({}, {}) from replicator {}", key, id, sender)
      ackReception(id, sender)
      if(!unAckOperations.contains(id)){
        senders.get(id) match {
          case Some(origin) =>
            origin ! OperationAck(id)
            senders -= id
          case None =>
        }
      }

    case Get(key: String, id: Long) => sender ! GetResult(key, kv.get(key), id)

  }

  def recordExpectation(id: Long, operator: ActorRef) = {
    log.info("Expect a result to {} from {}", id, operator)
    unAckOperations.get(id) match {
      case Some(operators) =>
        val operators2 = operators + operator
        unAckOperations = unAckOperations.updated(id , operators2)
      case None =>
        unAckOperations += id -> Set(operator)
    }
  }

  def ackReception(id: Long, operator: ActorRef) = {
    log.info("Acknowledge a result to {} from {}", id, operator)
    unAckOperations.get(id) match {
      case Some(operators) =>
        val operators2 = operators - operator
        if(operators2.isEmpty) unAckOperations -= id
        else unAckOperations = unAckOperations.updated(id, operators2)
      case None =>
    }
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
      context.actorOf(PersistenceSender.props(persistenceActor, sender, key, valueOption, nextSeq))

    case PersistAck(p: Persisted, origin: ActorRef) =>
      origin ! SnapshotAck(p.key, p.id)
  }

}


object PersistenceSender {
  case class PersistAck(persisted: Persisted, sender: ActorRef)
  case class PersistFail(key: String, id: Long, sender: ActorRef)
  def props(persistence: ActorRef, origin: ActorRef, key: String, valueOption: Option[String], id: Long): Props = Props(new PersistenceSender(persistence, origin, key, valueOption, id))

}

class PersistenceSender(persistence: ActorRef, origin: ActorRef, key: String, valueOption: Option[String], id: Long) extends Actor {
  val log = Logging(context.system, this)
  val maxTries = 9

  var _tryCounter = 0
  def nextTry = {
    val ret = _tryCounter
    _tryCounter += 1
    ret
  }

  log.info("PersistenceSender sends Persist({}, {} ,{}) ... try {}", key, valueOption, id, _tryCounter)
  persistence ! Persist(key, valueOption, id)
  context.setReceiveTimeout(100.milliseconds)

  def receive = {
    case p: Persisted =>
      log.info("PersistenceSender received Persisted({}, {})", p.key, p.id)
      log.info("PersistenceSender sends back to primary a PersistAck({}, {})", p, origin)
      context.parent ! PersistAck(p, origin)
      self ! PoisonPill

    case ReceiveTimeout =>

      if (nextTry >= maxTries) {
        log.info("PersistenceSender ({}) received a time-out for the last time", id)
        log.info("PersistenceSender sends a PersistFail({}, {}, {}) to its parent", key, id, origin)
        context.parent ! PersistFail(key, id, origin)
        self ! PoisonPill
      }
      else {
        log.info("PersistenceSender ({}) received a time-out", id)
        log.info("PersistenceSender sends Persist({}, {} ,{}) ... try {}", key, valueOption, id, _tryCounter)
        persistence ! Persist(key, valueOption, id)
      }
  }
}