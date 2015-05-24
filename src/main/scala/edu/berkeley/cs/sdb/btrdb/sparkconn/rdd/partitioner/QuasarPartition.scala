package edu.berkeley.cs.sdb.btrdb.sparkconn.rdd.partitioner

import org.apache.spark.Partition

/**
 * Created by almightykim on 5/5/15.
 */
class QuasarPartition private[sparkconn](
    var rddId: Int,
    var idx: Int)
  extends Partition  {

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx

}
