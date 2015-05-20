package kr.re.keti.spark.qonnector.rdd

import kr.re.keti.spark.qconnector.rdd.EmptyQuasarRDD
import org.apache.spark.Dependency
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import com.ceph.rados.Rados

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

  protected def uid: String

  protected def startTime: Long

  protected def endTime: Long

  protected def unitOfTime: String

  protected def connection: com.ceph.rados.Rados

  def toVoidQuasarRDD: EmptyQuasarRDD[R]

  /** Allows to copy this RDD with changing some of the properties */
  protected def copy(
    uid: String = uid,
    startTime: Long = startTime,
    endTime: Long = endTime,
    unitOfTime: String = unitOfTime,
    connection: com.ceph.rados.Rados = connection): Self

  /** Returns a copy of this Quasar RDD with specified context */
  def withConnection(connection: com.ceph.rados.Rados): Self = {
    copy(connection = connection)
  }

  def withQueryParams(uid: String, startTime: Long, endTime: Long, unitOfTime: String ) : Self = {
    copy(uid = uid, startTime = startTime, endTime = endTime, unitOfTime = unitOfTime )
  }

}
