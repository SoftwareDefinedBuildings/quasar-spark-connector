package edu.berkeley.cs.sdb.btrdb.sparkconn.rdd

import edu.berkeley.cs.sdb.btrdb.sparkconn.rdd.partitioner.QuasarPartition

import scala.language.existentials
import scala.reflect.ClassTag

//import org.apache.spark.metrics.InputMetricsUpdater
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * Created by almightykim on 5/2/15.
 */

class QuasarQueryRDD[R] private[sparkconn] (
    @transient val sc: SparkContext,
    var connection: com.ceph.rados.Rados,
    var uid: String,
    var startTime: Long,
    var endTime: Long,
    var unitOfTime: String,
    //TODO : should read this from config file and automatically decide to pull from where
    var numPartitions: Int)
  (implicit
   val classTag: ClassTag[R])
  extends QuasarRDD[R](sc, Seq.empty) {

  override type Self = QuasarQueryRDD[R]

  protected def numOfPartitions:Int = numPartitions

  override def toVoidQuasarRDD: EmptyQuasarRDD[R] = {
    new EmptyQuasarRDD[R](
      sc = sc,
      connection = connection,
      uid = uid,
      startTime = startTime,
      endTime = endTime,
      unitOfTime = unitOfTime
    )
  }

  override protected def copy(
    uid: String = uid,
    startTime: Long = startTime,
    endTime: Long = endTime,
    unitOfTime: String = unitOfTime,
    connection: com.ceph.rados.Rados = connection): Self = {

      require(sc != null,
        "RDD transformation requires a non-null SparkContext. " +
          "Unfortunately SparkContext in this QuasarQueryRDD is null. " +
          "This can happen after QuasarQueryRDD has been deserialized. " +
          "SparkContext is not Serializable, therefore it deserializes to null." +
          "RDD transformations are not allowed inside lambdas used in other RDD transformations.")

      new QuasarQueryRDD[R](
        sc = sc,
        connection = connection,
        uid = uid,
        startTime = startTime,
        endTime = endTime,
        unitOfTime = unitOfTime,
        //TODO : should read this from config file and automatically decide to pull from where
        numPartitions = numPartitions
      )
  }

  override def getPartitions: Array[Partition] = {
    logDebug("getPartitions")

    val array = new Array[Partition](numPartitions)
    for (i <- 0 until numPartitions) {
      array(i) = new QuasarPartition(id, i)
    }
    array
  }

  //TODO : fix this with config file
  override def getPreferredLocations(split: Partition): Seq[String] = {
    //split.asInstanceOf[QuasarPartition].endpoints.map(_.getHostName).toSeq

    var hosts = new Array[String](numPartitions)
    hosts(0) = "rasp-worker-1"
    hosts(1) = "rasp-worker-2"
    hosts(2) = "rasp-worker-3"
    hosts(3) = "rasp-worker-4"
    hosts(4) = "rasp-worker-5"
    hosts(5) = "rasp-worker-6"

    hosts.toSeq
  }


  override def compute(split: Partition, context: TaskContext): Iterator[R] = {



    null
  }

}


object QuasarQueryRDD {

}