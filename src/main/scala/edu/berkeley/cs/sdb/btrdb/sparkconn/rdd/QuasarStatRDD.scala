package edu.berkeley.cs.sdb.btrdb.sparkconn.rdd

import java.util.UUID

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar._
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types.StatRecord
import edu.berkeley.cs.sdb.btrdb.sparkconn.rdd.partitioner.QuasarPartition

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
    logDebug("getPartitions")

    //TODO : fix this with config file
    val numPartitions:Int = 6

    val partitions:Array[Partition] = new Array[Partition](numPartitions)
    for (i <- 0 until numPartitions) {
      partitions(i) = new QuasarPartition(i, startTime, endTime, "rasp-worker-%d".format(i))
    }
    partitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Array(split.asInstanceOf[QuasarPartition].node).toSeq

  override def compute(split: Partition, context: TaskContext): Iterator[StatRecord] = {
    val partition = split.asInstanceOf[QuasarPartition]
    logDebug("compute:: partition Index %d".format(partition.index))
    QueryStatisticalValues(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"), 1364823796L, 1398437046L, LatestGeneration, 16)
  }

}


object QuasarStatRDD {

}