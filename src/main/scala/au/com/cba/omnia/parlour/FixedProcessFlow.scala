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

import java.util.concurrent.locks.ReentrantLock
import java.util.{List => JList, Map => JMap, Properties}

import scala.collection.JavaConverters._

import cascading.CascadingException
import cascading.flow.{BaseFlow, Flow, FlowException, FlowListener}
import cascading.flow.hadoop.ProcessFlow
import cascading.util.{Update, Util, Version}

import riffle.process.scheduler.{ProcessException, ProcessWrapper}

/**
  * Patches [[cascading.flow.hadoop.ProcessFlow]] so it calls onComplete and
  * onThrowable
  */
class FixedProcessFlow[P](
  properties: JMap[AnyRef, AnyRef], name: String, process: P
) extends ProcessFlow[P](properties, name, process) {
 /*
  * The overall strategy is:
  *  - Override start, stop, complete, and replace run.
  *  - Need to reimplement the small ecosystem around these methods too.
  *  - Use replacement fields for these methods and their ecosystem.
  *  - Use reflection hacks to avoid reimplementing listeners and friends.
  *  - fail fast in constructor if reflection hacks are out of date
  *
  * FixedProcessFlow relates to ProcessFlow as follows:
  *  - process, getProcess: process accessed via getProcess
  *  - processWrapper: FPF has its own instance. They share process.
  *  - isStarted: not used
  *  - setTapFromProcess: not changed
  *  - prepare: not changed
  *  - cleanup: not changed
  *  - start, stop, and complete: not used
  *  - sources and sink ecosystem: not changed
  *
  * FixedProcessFlow relates to HadoopFlow as follows:
  *  - we pretend HadoopFlow doesn't exist
  *
  * FixedProcessFlow relates to BaseFlow as follows:
  *  - thread: use directly
  *  - flowStats: use directly
  *  - listeners and friends: use reflection to read
  *  - throwable: replaced
  *  - stopLock: replaced
  *  - stop field: replaced (pain to use directly because of ambiguity with stop method)
  *  - spawnStrategy: not used
  *  - LOG and log methods: not used, don't have the same loggers in project
  *  - sources, sinks, traps, checkpoints: no need to change
  *  - flowStepGraph, steps, jobsMap: not sure if relevant, but ProcessFlow
  *      leaves these alone so I will too
  *  - start, stop, complete, run, and friends: replaced
  */

  // fail fast if reflection code is out of date
  testFlowListenerThrowable
  testFlowListeners

  /**
    * Patches [[cascading.flow.hadoop.ProcessFlow]] so it calls onComplete and
    * onThrowable
    */
  def this(name: String, process: P) { this(new Properties, name, process) }

  // replacements for selected super-type fields
  protected val processWrapper: ProcessWrapper = new ProcessWrapper(process)
  protected var throwable: Option[Throwable] = None
  protected var stopped: Boolean = false
  @transient protected val stopLock: ReentrantLock = new ReentrantLock(true)

  // override start to mimic BaseFlow
  // like ProcessWrapper, we do not call internalStart, and do not register
  // shutdown hook as process should run in the same JVM
  override def start: Unit = wrap("start on process", this.synchronized {
    if (thread != null || stopped) return

    val threadName = "flow " + Util.toNull(getName).trim
    thread = new Thread(new ProcessFlowRunnable, threadName)
    thread.start
  })

  protected class ProcessFlowRunnable extends Runnable {
    override def run { FixedProcessFlow.this.run }
  }

  // override stop to mostly mimic BaseFlow
  // like ProcessWrapper, we do not call internalClean
  override def stop: Unit = wrap("stop on process", this.synchronized {
    stopLock.lock

    try {
      if (stopped) return
      stopped = true

      fireOnStopping
      if(!flowStats.isFinished) flowStats.markStopped

      processWrapper.stop
    }
    finally {
      flowStats.cleanup
      stopLock.unlock
    }
  })

  // override complete to mostly mimic BaseFlow
  override def complete: Unit = wrap("complete on process", {
    start

    try {
      // wait for start to finish
      try {
        // prevent NPE on quick stop() & complete() after start()
        this.synchronized {
          while (thread == null && !stopped) Util.safeSleep(10)
        }
        if (thread != null) thread.join
      }
      catch {
        case ex: InterruptedException =>
          throw new FlowException(getName, "thread interrupted", ex)
      }

      // if in #stop and stopping, lets wait till its done in this thread
      try  { stopLock.lock } finally { stopLock.unlock }

      // throw any exceptions in throwable
      throwable.foreach( ex => throw ex match {
        case e: FlowException      => e // can't set flow name from here
        case e: CascadingException => e
        case e: OutOfMemoryError   => e
        case e                     => new FlowException(getName, "unhandled exception", e)
      })

      // throw any exceptions from listeners
      flowListeners.foreach( listener => {
        getFlowListenerThrowable(listener).foreach( ex =>
          throw new FlowException(getName, "unhandled listener exception", ex)
        )
      })
    }

    // clean up after finishing and handling exceptions
    finally {
      try {
        thread = null
        throwable = None
        // not doing commitTraps as ProcessFlow uses empty list of traps anyway
        flowListeners.foreach(clearFlowListenerThrowable(_))
      }
      finally {
        flowStats.cleanup
      }
    }
  })

  // reimplement run, will have the same structure as BaseFlow but avoid all the
  // multi-threading and spawn logic
  protected def run() {
    if (thread == null)
      throw new IllegalStateException("to start a Flow call start() or complete(), not Runnable#run()")

    try {
      if (stopped) return

      flowStats.markStartedThenRunning
      fireOnStarting

      processWrapper.complete
      if (!flowStats.isFinished && !stopped) flowStats.markSuccessful
    }
    catch { case ex: Throwable => {
      // the !stopped condition here fixes what I think is a bug in BaseFlow
      // flowStats have already been stopped, can't mark them as failed as well
      if (!stopped) flowStats.markFailed(ex)
      if (!tryHandleThrowable(ex)) throwable = Some(ex)
    }}
    finally {
      try {
        fireOnCompleted
      }
      finally {
        flowStats.cleanup
      }
    }
  }

  protected def tryHandleThrowable(ex: Throwable): Boolean =
    flowListeners.exists( listener  =>
      listener.onThrowable(this, ex)
    )

  // fail fast if the horrible reflection hacks are out of date
  // called in constructor so assumes no need for thread safety
  // using java reflection as I'm not sure how to get throwable field with scala reflection?

  protected def testFlowListenerThrowable() {
    val l = new NullFlowListener
    addListener(l)
    try {
      val listener = flowListeners.head
      val klazz    = listener.getClass
      val similar  = klazz.getDeclaredFields.filter(f =>
        f.getName == "throwable" || f.getType.isAssignableFrom(classOf[Throwable])
      )
      val expected = similar.filter(f =>
        f.getName == "throwable" && f.getType.isAssignableFrom(classOf[Throwable])
      )
      getFlowListenerThrowable(listener) // throws if wrong
      clearFlowListenerThrowable(listener) // throws if wrong
      if (klazz.getName != "cascading.flow.BaseFlow$SafeFlowListener" ||
          similar.size != 1 || expected.size != 1) {
        throw new Exception("SafeFlowListener has changed, check that throwable reflection hack still works")
      }
    }
    finally {
      if (!removeListener(l)) {
        throw new Exception("failed to remove test listener")
      }
    }
  }

  protected def testFlowListeners() {
    val similar = classOf[BaseFlow[_]].getDeclaredMethods.filter(m => m.getName == "getListeners")
    if (similar.size != 1) {
      throw new Exception("BaseFlow has changed, check that getListener reflection hack still works")
    }
    flowListeners // throws if wrong
  }

  // horrible reflection hacks to access listeners and SafeFlowListener throwable
  // assumes reflection works, as we deliberately failed earlier if it didn't

  protected def getFlowListenerThrowable(listener: FlowListener): Option[Throwable] = {
    val field = listener.getClass.getDeclaredField("throwable")
    field.setAccessible(true)
    Option(field.get(listener).asInstanceOf[Throwable])
  }

  protected def clearFlowListenerThrowable(listener: FlowListener) {
    val field = listener.getClass.getDeclaredField("throwable")
    field.setAccessible(true)
    field.set(listener, null)
  }

  protected def flowListeners: Iterable[FlowListener] = {
    val method = classOf[BaseFlow[_]].getDeclaredMethod("getListeners")
    method.setAccessible(true)
    method.invoke(this).asInstanceOf[JList[FlowListener]].asScala
  }

  // mimic standard exception handling in ProcessBase
  protected def wrap[A](desc: String, action: => A): A = {
    try {
      action
    }
    catch {
      case ex: ProcessException => ex.getCause match {
        case cause: RuntimeException => throw cause
        case cause => throw new FlowException(s"could not $desc", cause)
      }
    }
  }
}

class NullFlowListener extends FlowListener {
  def onStarting(flow: Flow[_]) {}
  def onStopping(flow: Flow[_]) {}
  def onCompleted(flow: Flow[_]) {}
  def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = false
}
