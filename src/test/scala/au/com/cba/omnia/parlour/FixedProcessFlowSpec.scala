//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.parlour

import java.util.{Collection => JCollection, Collections => JCollections}

import cascading.flow.{Flow, FlowListener}
import cascading.stats.CascadingStats.Status
import cascading.util.Util

import riffle.process._

import org.specs2.Specification
import org.specs2.matcher.MatchResult

object FixedProcessFlowSpec extends Specification { def is = s2"""
FixedProcessFlowSpec
====================
  complete runs synchronously and calls onComplete $completeCallbacks
  start runs asynchronously and calls onComplete   $startCallbacks
  exception causes onThrowable than onComplete     $exceptionCallbacks
  stop causes onStopped than onComplete            $stopCallbacks
  onThrowable exception does not stop onComplete   $onThrowableExceptionCallbacks
  complete throws exceptions                       $completeException
  complete throws listener exceptions              $completeListenerException
  flowStats status marks success                   $flowStatsSuccess
  flowStats status marks stop                      $flowStatsStop
  flowStats status marks throwable                 $flowStatsFail
"""

  def completeCallbacks = {
    val flow = new TestFixedProcessFlow("test", new NullRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.complete
    listener.calls must_== List(OnStarting, OnCompleted)
  }

  def startCallbacks = {
    val flow = new TestFixedProcessFlow("test", new NullRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.start
    flow.waitForProcess
    listener.calls must_== List(OnStarting, OnCompleted)
  }

  def exceptionCallbacks = {
    val flow = new TestFixedProcessFlow("test", new ExceptionalRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.start
    flow.waitForProcess
    listener.calls must_== List(OnStarting, OnThrowable, OnCompleted)
  }

  def stopCallbacks = {
    val flow = new TestFixedProcessFlow("test", new SleepyRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.start
    flow.stop
    flow.waitForProcess
    // three invariants:
    // OnStopping and OnCompleted both fire
    // if OnStarting fires, both OnStarting and OnStopping fire before OnCompleted
    // onThrowing doesn't fire
    (listener.calls must_== List(OnStarting, OnStopping, OnCompleted)) or
    (listener.calls must_== List(OnStopping, OnStarting, OnCompleted)) or
    (listener.calls must_== List(OnStopping, OnCompleted)) or
    (listener.calls must_== List(OnCompleted, OnStopping))
  }

  def onThrowableExceptionCallbacks = {
    val flow = new TestFixedProcessFlow("test", new ExceptionalRiffle)
    val listener = new FailOnThrowableListener
    flow.addListener(listener)
    flow.start
    flow.waitForProcess
    // the throwable exception stops the flow, and onCompleted always fires
    listener.calls must_== List(OnStarting, OnThrowable, OnStopping, OnCompleted)
  }

  def completeException = {
    val flow = new TestFixedProcessFlow("test", new ExceptionalRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.start
    flow.complete must throwAn[Exception].like { case ex => causedBy(ex, cause =>
      cause.getMessage must_== Message.exceptionalRiffle
    )}
  }

  def completeListenerException = {
    val flow = new TestFixedProcessFlow("test", new NullRiffle)
    val listener = new FailOnCompletedListener
    flow.addListener(listener)
    flow.start
    flow.complete must throwAn[Exception].like { case ex => causedBy(ex, cause =>
      cause.getMessage must_== Message.failOnCompleted
    )}
  }

  def flowStatsSuccess = {
    val flow = new TestFixedProcessFlow("test", new NullRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.start
    flow.waitForProcess
    flow.getFlowStats.getStatus must_== Status.SUCCESSFUL
  }

  def flowStatsStop = {
    val flow = new TestFixedProcessFlow("test", new SleepyRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.start
    flow.stop
    flow.waitForProcess
    flow.getFlowStats.getStatus must_== Status.STOPPED
  }

  def flowStatsFail = {
    val flow = new TestFixedProcessFlow("test", new ExceptionalRiffle)
    val listener = new RecordingListener
    flow.addListener(listener)
    flow.start
    flow.waitForProcess
    flow.getFlowStats.getStatus must_== Status.FAILED
  }

  def causedBy(ex: Throwable, cond: Throwable => MatchResult[_]): MatchResult[_] =
    Option(ex.getCause) match {
      case Some(cause) => causedBy(cause, cond)
      case None        => cond(ex)
    }
}

sealed trait Record
case object OnStarting extends Record
case object OnStopping extends Record
case object OnCompleted extends Record
case object OnThrowable extends Record

class RecordingListener extends FlowListener {
  var calls: List[Record] = List.empty[Record]
  def record(r: Record) { this.synchronized { calls = calls :+ r} }

  def onStarting(flow: Flow[_]) { record(OnStarting) }
  def onStopping(flow: Flow[_]) { record(OnStopping) }
  def onCompleted(flow: Flow[_]) { record(OnCompleted) }
  def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = {
    record(OnThrowable)
    false
  }
}

class FailOnThrowableListener extends RecordingListener {
  override def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = {
    super.onThrowable(flow, throwable)
    throw new Exception(Message.failOnThrowable)
  }
}

class FailOnCompletedListener extends RecordingListener {
  override def onCompleted(flow: Flow[_]) {
    super.onCompleted(flow)
    throw new Exception(Message.failOnCompleted)
  }
}

@Process
class NullRiffle {
  @ProcessStart
  def start() { throw new Exception("Not expecting this to be called")}
  @ProcessStop
  def stop() {}
  @ProcessComplete
  def complete() {}
  @DependencyIncoming
  def getIncoming(): JCollection[_] = JCollections.EMPTY_LIST
  @DependencyOutgoing
  def getOutgoing(): JCollection[_] = JCollections.EMPTY_LIST
}

@Process
class ExceptionalRiffle extends NullRiffle {
  @ProcessComplete
  override def complete() { throw new Exception(Message.exceptionalRiffle) }
}

@Process
class SleepyRiffle extends NullRiffle {
  var sleepy = true
  @ProcessStop
  override def stop() { sleepy = false }
  @ProcessComplete
  override def complete() { while (sleepy) Thread.sleep(10) }
}

object Message {
  val failOnCompleted = "onCompleted exception"
  val failOnThrowable = "onThrowable exception"
  val exceptionalRiffle = "ExceptionalRiffle complete exception"
}

class TestFixedProcessFlow[P](
  name: String, process: P
) extends FixedProcessFlow[P](name, process) {
  /** Called after start, will wait for process kicked off by start to finish */
  def waitForProcess() {
    this.synchronized {
      while (thread == null && !stopped) Util.safeSleep(10)
    }
    if (thread != null) thread.join
  }
}
