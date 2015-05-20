package kr.re.keti.spark.qconnector.rdd.partitioner

import org.apache.spark.Partition

/**
 * Created by almightykim on 5/5/15.
 */
class QuasarPartition private[qconnector](
    var rddId: Int,
    var idx: Int)
  extends Partition  {

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx

}
