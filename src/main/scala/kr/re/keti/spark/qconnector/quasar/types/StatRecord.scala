package kr.re.keti.spark.qconnector.quasar.types

import com.google.common.primitives.UnsignedLong
/**
 * Created by almightykim on 5/10/15.
 */
class StatRecord(
  val Time: UnsignedLong, //This is at the start of the record
  val Count: UnsignedLong,
  val Min: Double,
  val Mean: Double,
  val Max: Double
) {

}
