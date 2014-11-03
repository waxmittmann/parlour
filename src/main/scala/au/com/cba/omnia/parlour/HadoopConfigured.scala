package au.com.cba.omnia.parlour

import com.twitter.scalding.{HadoopMode, Job}
import org.apache.hadoop.conf.Configuration

trait HadoopConfigured {
  self: Job =>

  /**
   * Retrieves underlying [[Configuration]] from Scalding [[Job]].
   * @return Some in --hdfs mode, None in --local mode.
   */
  def getHadoopConf: Option[Configuration] = mode match {
    case h: HadoopMode => Some(h.jobConf)
    case _ => None
  }
}
