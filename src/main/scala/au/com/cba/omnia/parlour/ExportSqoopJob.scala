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