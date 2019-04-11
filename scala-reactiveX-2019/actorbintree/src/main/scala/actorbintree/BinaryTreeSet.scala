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

  /** Request to perform garbage collection */
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
    case m: Operation => root ! m
    case m @ GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    case m @ _ => println(s"unsupport msg return: ${m}")
  }

  def dequeue(newRoot: ActorRef): Unit = {
    val (h, tail) = pendingQueue.dequeue
    pendingQueue = tail
    newRoot ! h
    if (pendingQueue.isEmpty) {
      self ! CopyFinished
    } else {
      dequeue(newRoot)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished =>
      println("[debug] garbageCollecting copy finished")
      if (pendingQueue.size == 0) {
        println("[debug] garbageCollecting copy finished and queue finished")
        root = newRoot
        context.become(normal)
      } else {
        dequeue(newRoot)
      }
    case GC => //ignore because is gcing
    case m: Operation =>
      //      println(s"in garbageCollecting: ${m}, root: ${newRoot}")
      //      newRoot ! m
//      println(s"[debug] enqueue: ${m}, self: ${self}")
      pendingQueue = pendingQueue.enqueue(m)
  }
}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def insertRightOrLeft(m: Insert) = {
//    println(s"[debug] INSERT: $m")

    def insertElement(p: Position) = {
      subtrees.get(p) match {
        case None =>
          subtrees = subtrees + (p -> context.actorOf(BinaryTreeNode.props(m.elem, false)))
          m.requester ! OperationFinished(m.id)
        case Some(node) => node ! m
      }
    }

    if (m.id == -1) println(s"[debug] copy ${m.elem}, INSERT: ${m}, sender:${sender()}")

    if (m.elem > this.elem) {
      insertElement(Right)
    } else if (m.elem < this.elem) {
      insertElement(Left)
    } else {
      removed = false
      m.requester ! OperationFinished(m.id)
    }
  }

  def isExistedElement(m: Contains) = {
    def isExisted(p: Position) = {
      subtrees.get(p) match {
        case None => m.requester ! ContainsResult(m.id, false)
        case Some(node) => node ! m
      }
    }

    if (m.elem > this.elem) {
      isExisted(Right)
    } else if (m.elem < this.elem) {
      isExisted(Left)
    } else {
      m.requester ! ContainsResult(m.id, !removed)
    }
  }

  def removeElement(m: Remove) = {
    def remove(p: Position) = {
      subtrees.get(p) match {
        case None => m.requester ! OperationFinished(m.id)
        case Some(n) => n ! m
      }
    }

    if (m.elem > this.elem) {
      remove(Right)
    } else if (m.elem < this.elem) {
      remove(Left)
    } else {
      removed = true
      m.requester ! OperationFinished(m.id)
    }
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case m: Insert => insertRightOrLeft(m)
    case m: Remove => removeElement(m)
    case m: Contains => isExistedElement(m)
    case m @ CopyTo(newRoot) =>
      if (!removed) {
        newRoot ! Insert(self, -1, this.elem)
      } else {
        newRoot ! Remove(self, -1, this.elem)
      }

      context.become(copying(subtrees.values.toSet, false))
    case x @ _ =>
      println(s"[debug] received somethin else: ${x}, sender:${sender}")
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) =>
      if (expected.isEmpty) {
        println(
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        expected.foreach(_ ! CopyTo(sender))
        context.become(copying(expected, true))
      }
    case CopyFinished => // receive CopyFinished from child node
      val expectLeft = expected.tail // once received CopyFinished from child, then remove one expect child
      if (insertConfirmed && expectLeft.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(expectLeft, insertConfirmed))
      }
    case x @ _ =>
      println(s"[debug] copying else: ${x}")

  }

}
