package edu.berkeley.eecs.btrdb.sparkconn.rdd

import java.util.UUID

import edu.berkeley.eecs.btrdb.sparkconn.cephprovider._
import edu.berkeley.eecs.btrdb.sparkconn.quasar._
import edu.berkeley.eecs.btrdb.sparkconn.quasar.types.StatRecord
import edu.berkeley.eecs.btrdb.sparkconn.rdd.partitioner.QuasarPartition
import edu.berkeley.eecs.btrdb.sparkconn.util.BTrDBConf

import scala.language.existentials
import scala.reflect.ClassTag
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.util.Properties

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

  private val qconf = new BTrDBConf("/etc/quasar/quasar.conf", "/etc/ceph/ceph.conf")

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


  private def getActiveSlaves(sc: SparkContext): Set[String] = {

    val drvhost: String = sc.getConf.get("spark.driver.host")
    var slaves = Set[String]()
    var nhFound: Boolean = true
    val st = System.currentTimeMillis
    var timeout = false
    while (nhFound && !timeout) {
      Thread.sleep(3000)
      val old = slaves
      val allnodes = sc.getExecutorMemoryStatus.map(_._1.replace(" ", "").split(":")(0)).toSet
      slaves = allnodes.diff(Set(drvhost))
      nhFound = slaves.diff(old).nonEmpty
      if (System.currentTimeMillis - st >= 30000)
        timeout = true
    }
    slaves
  }

  override def getPartitions: Array[Partition] = {
    //TOOD : 1. size of data. 2 # of available spark have to be taken into account
/*
    this whole part does not work due to spark 1.1.1 issue. :(

    // spark executors
    val slaves = getActiveSlaves(sc)

    // ceph monitors
    val cmons = qconf.getCephMons

    val parts = slaves.intersect(cmons).toList
*/

    // this is temp. measure assuming that all the ceph nodes are spark nodes at the same time. :[
    val drvhost: String = sc.getConf.get("spark.driver.host")
    val cmons = qconf.getCephMons
    val parts = cmons.diff(Set(drvhost)).toList
    val numParts:Int = parts.size

    // for Spark 1.2.0 we can ask if this much executors are available in the cluster
    //sc.requestExecutors(numParts);

    val partitions:Array[Partition] = new Array[Partition](numParts)
    for (i <- 0 until numParts) {
      partitions(i) = new QuasarPartition(numParts, i, startTime, endTime, parts(i))
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

    val rv:Iterator[StatRecord] = QueryStatisticalValues(qconf, UUID.fromString(uid), lst, led, LatestGeneration, pointWidth)

    CloseRadosConn()

    rv
  }

}
