package edu.berkeley.eecs.btrdb.sparkconn.rdd.partitioner

import java.net.InetAddress

import org.apache.spark.Partition

case class QuasarPartition private[sparkconn](
  val partitionSize:Long,
  index: Int,
  val startTime:Long,
  val endTime:Long,
  val node: String)
  extends Partition  {

  override def hashCode(): Int = (index + startTime.toInt + endTime.toInt)

}
