package edu.berkeley.eecs.btrdb.sparkconn.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.language.existentials
import scala.reflect.ClassTag

abstract class QuasarRDD[R : ClassTag]  (
    sc: SparkContext,
    dep: Seq[Dependency[_]])
  extends RDD[R](sc, dep) {

  /** This is slightly different than Scala this.type.
    * this.type is the unique singleton type of an object which is not compatible with other
    * instances of the same type, so returning anything other than `this` is not really possible
    * without lying to the compiler by explicit casts.
    * Here SelfType is used to return a copy of the object - a different instance of the same type */
  type Self <: QuasarRDD[R]

  def uid: String

  def startTime: Long

  def endTime: Long

  def unitOfTime: String

  def pointWidth: Int

  def toVoidQuasarRDD: EmptyQuasarRDD[R]

  /** Allows to copy this RDD with changing some of the properties */
  protected def copy(
    uid: String = uid,
    startTime: Long = startTime,
    endTime: Long = endTime,
    unitOfTime: String = unitOfTime,
    pointWidth: Int = pointWidth): Self

  def withQueryParams(uid: String, startTime: Long, endTime: Long, unitOfTime: String, pointWidth:Int) : Self = {
    copy(uid = uid, startTime = startTime, endTime = endTime, unitOfTime = unitOfTime, pointWidth = pointWidth )
  }

}
