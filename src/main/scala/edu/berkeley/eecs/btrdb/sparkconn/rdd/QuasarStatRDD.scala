package edu.berkeley.eecs.btrdb.sparkconn.rdd

import java.util.UUID

import edu.berkeley.eecs.btrdb.sparkconn.cephprovider._
import edu.berkeley.eecs.btrdb.sparkconn.quasar._
import edu.berkeley.eecs.btrdb.sparkconn.quasar.types.StatRecord
import edu.berkeley.eecs.btrdb.sparkconn.rdd.partitioner.QuasarPartition

import scala.language.existentials
import scala.reflect.ClassTag

//import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.{Partition, SparkContext, TaskContext}

class QuasarStatRDD private[sparkconn] (
    @transient val sc: SparkContext,
    val uid: String,
    val startTime: Long,
    val endTime: Long,
    val unitOfTime: String,
    val pointWidth: Int)
  (implicit
   val classTag: ClassTag[StatRecord])
  extends QuasarRDD[StatRecord](sc, Seq.empty) {

  override type Self = QuasarStatRDD

  override def toVoidQuasarRDD: EmptyQuasarRDD[StatRecord] = {
    new EmptyQuasarRDD[StatRecord](
      sc = sc,
      uid = uid,
      startTime = startTime,
      endTime = endTime,
      unitOfTime = unitOfTime,
      pointWidth = pointWidth
    )
  }

  override protected def copy(
    uid: String = uid,
    startTime: Long = startTime,
    endTime: Long = endTime,
    unitOfTime: String = unitOfTime,
    pointWidth:Int = pointWidth): Self = {

      require(sc != null,
        "RDD transformation requires a non-null SparkContext. " +
          "Unfortunately SparkContext in this QuasarQueryRDD is null. " +
          "This can happen after QuasarQueryRDD has been deserialized. " +
          "SparkContext is not Serializable, therefore it deserializes to null." +
          "RDD transformations are not allowed inside lambdas used in other RDD transformations.")

      new QuasarStatRDD(
        sc = sc,
        uid = uid,
        startTime = startTime,
        endTime = endTime,
        unitOfTime = unitOfTime,
        pointWidth = pointWidth
      )
  }

  override def getPartitions: Array[Partition] = {
    println("getPartitions")

    // TOOD : 1. fix this with config file. 2. size of data &  # of available spark, OSD nodes, and etc has to be taken
    // into account
    val numPartitions:Int = 6

    val partitions:Array[Partition] = new Array[Partition](numPartitions)
    for (i <- 0 until numPartitions) {
      partitions(i) = new QuasarPartition(numPartitions, i, startTime, endTime, "rasp-worker-%d".format(i))
    }
    partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Array(split.asInstanceOf[QuasarPartition].node).toSeq

  override def compute(split: Partition, context: TaskContext): Iterator[StatRecord] = {

    val partition = split.asInstanceOf[QuasarPartition]

    //TOOD : pointWidth overlap between nodes, dividend inaccuracy need to be considered
    val diff = ((endTime - startTime).toDouble / partition.partitionSize.toDouble).toLong

    // local node start time
    val lst = startTime + partition.index * diff

    // local node end time
    val led = lst + diff

    println("compute:: partition [%d] uid %s, startTime [%d] endTime [%d] pointWidth <%d>".format(partition.index, uid, lst, led, pointWidth))

    OpenRadosConn()

    val rv:Iterator[StatRecord] = QueryStatisticalValues(UUID.fromString(uid), lst, led, LatestGeneration, pointWidth)

    CloseRadosConn()

    rv
  }

}


object QuasarStatRDD {

}