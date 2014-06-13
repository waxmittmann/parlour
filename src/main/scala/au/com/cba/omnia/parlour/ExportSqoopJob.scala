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

import cascading.tap.Tap

import com.cloudera.sqoop.SqoopOptions

import com.twitter.scalding._


/**
 * Sqoop Job that can be embedded within a Cascade
 *
 * See [[SqoopSyntax]] for an easy way to create a [[SqoopOptions]]
 */
class ExportSqoopJob(options: SqoopOptions,
                     source: Tap[_, _, _],
                     sink: Tap[_, _, _] = new NullTap())
                    (args: Args) extends Job(args) {

  /** Helper constructor that allows easy usage from Scalding */
  def this(options: SqoopOptions, source: Source, sink: Source)(args: Args)(implicit mode: Mode) =
    this(options, source.createTap(Read), sink.createTap(Write))(args)

  override def buildFlow =
    new ExportSqoopFlow("exporting to sqoop", options, source, sink)

  /** Can't validate anything because this doesn't use a Hadoop FlowDef. */
  override def validate = ()

  /**
   * Overriden to return success (true) - it will throw an exception on any failures.
   */
  override def run: Boolean = {
    val flow = buildFlow
    flow.complete
    true
  }
}