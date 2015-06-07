package edu.berkeley.eecs.btrdb.sparkconn.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class EmptyQuasarRDD[R : ClassTag](
    @transient val sc: SparkContext,
    val uid: String,
    val startTime: Long,
    val endTime: Long,
    val unitOfTime: String,
    val pointWidth:Int)
  extends QuasarRDD[R](sc, Seq.empty) {

  override type Self = EmptyQuasarRDD[R]

  override protected def copy(
    uid: String = uid,
    startTime: Long = startTime,
    endTime: Long = endTime,
    unitOfTime: String = unitOfTime,
    pointWidth: Int = pointWidth): Self = {

    new EmptyQuasarRDD[R](
      sc = sc,
      uid = uid,
      startTime = startTime,
      endTime = endTime,
      unitOfTime = unitOfTime,
      pointWidth = pointWidth
    )
  }

  override protected def getPartitions: Array[Partition] = Array.empty

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[R] =
    throw new UnsupportedOperationException("Cannot call compute on an VoidQuasarRDD")

  override def toVoidQuasarRDD: EmptyQuasarRDD[R] = copy()

}
