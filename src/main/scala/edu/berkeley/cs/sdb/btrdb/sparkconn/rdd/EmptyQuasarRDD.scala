package edu.berkeley.cs.sdb.btrdb.sparkconn.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class EmptyQuasarRDD[R : ClassTag](
    @transient val sc: SparkContext,
    var connection: com.ceph.rados.Rados,
    var uid: String,
    var startTime: Long,
    var endTime: Long,
    var unitOfTime: String)
  extends QuasarRDD[R](sc, Seq.empty) {

  override type Self = EmptyQuasarRDD[R]

  override protected def copy(
    uid: String = uid,
    startTime: Long = startTime,
    endTime: Long = endTime,
    unitOfTime: String = unitOfTime,
    connection: com.ceph.rados.Rados = connection): Self = {

    new EmptyQuasarRDD[R](
      sc = sc,
      connection = connection,
      uid = uid,
      startTime = startTime,
      endTime = endTime,
      unitOfTime = unitOfTime
    )
  }

  override protected def getPartitions: Array[Partition] = Array.empty

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[R] =
    throw new UnsupportedOperationException("Cannot call compute on an VoidQuasarRDD")

  //override protected def connection: com.ceph.rados.Rados = throw new UnsupportedOperationException("Void Quasar RDD don't have context")

  override def toVoidQuasarRDD: EmptyQuasarRDD[R] = copy()

}
