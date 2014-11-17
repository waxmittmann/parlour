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

package au.com.cba.omnia.parlour.flow

import scala.collection.JavaConverters.seqAsJavaListConverter

import java.util.{Collection => JCollection}

import au.com.cba.omnia.parlour.FixedProcessFlow
import au.com.cba.omnia.parlour.flow.SqoopFlow.SqoopLogic
import cascading.tap.Tap
import riffle.process._

/** Implements a Cascading Flow that wraps the Sqoop Process */
case class SqoopFlow(name: String, source: Option[Tap[_, _, _]], sink: Option[Tap[_, _, _]])(logic: SqoopLogic)
  extends FixedProcessFlow[SqoopRiffle](name, new SqoopRiffle(source, sink)(logic))

object SqoopFlow {
  type SqoopLogic = (Option[Tap[_, _, _]], Option[Tap[_, _, _]]) => Unit
}

/** Implements a Riffle for a Sqoop Flow */
@Process
class SqoopRiffle(source: Option[Tap[_, _, _]], sink: Option[Tap[_, _, _]])(logic: SqoopLogic) {
  @ProcessStart
  def start(): Unit = ()

  @ProcessStop
  def stop(): Unit = ()

  @ProcessComplete
  def complete(): Unit = logic(source, sink)

  @DependencyIncoming
  def getIncoming(): JCollection[_] = source.toList.asJava

  @DependencyOutgoing
  def getOutgoing(): JCollection[_] = sink.toList.asJava
}