package kr.re.keti.spark.qconnector.quasar.types

import com.google.common.primitives.{UnsignedInteger, UnsignedLong}

/**
 * Created by almightykim on 5/10/15.
 */
class Coreblock(
  //Metadata, not copied
  val Identifier: UnsignedLong,
  val Generation: UnsignedLong,

  //Payload, copied
  val PointWidth: UnsignedInteger,
  val StartTime: Long
) {

  var Addr: Array[UnsignedLong]
  var Count: Array[UnsignedLong]
  var Min: Array[Double]
  var Mean: Array[Double]
  var Max: Array[Double]
  var CGeneration: Array[UnsignedLong]

}
