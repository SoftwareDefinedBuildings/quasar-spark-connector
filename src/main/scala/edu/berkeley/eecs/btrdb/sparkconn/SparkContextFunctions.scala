package edu.berkeley.eecs.btrdb.sparkconn

import edu.berkeley.eecs.btrdb.sparkconn.rdd.{EmptyQuasarRDD, QuasarStatRDD}
import org.apache.spark.SparkContext

class SparkContextFunctions (@transient val sc: SparkContext) extends Serializable {
  def quasarStatQuery (uid:String, startTime:Long, endTime:Long, unitOfTime:String, pointWidth:Int) =
    new QuasarStatRDD(sc, uid, startTime, endTime, unitOfTime, pointWidth)
}
