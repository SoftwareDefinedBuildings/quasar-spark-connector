package kr.re.keti.spark.qconnector.quasar.types

import com.google.common.primitives.{UnsignedInteger, UnsignedLong}

// The leaf datablock type. The tags allow unit tests
// to work out if clone / serdes are working properly
// metadata is not copied when a node is cloned
// implicit is not serialised

class Vectorblock (
  //Metadata, not copied on clone
  val Identifier: UnsignedLong,
  val Generation: UnsignedLong,

  //Payload, copied on clone
  val Len: UnsignedInteger,
  val PointWidth: UnsignedInteger,
  val StartTime: Long,
  val Time: Array[Long],
  val Value: Array[Double]
){



}
