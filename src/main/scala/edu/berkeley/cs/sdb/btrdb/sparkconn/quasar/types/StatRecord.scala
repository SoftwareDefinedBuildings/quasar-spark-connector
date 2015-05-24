package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types

/**
 * Created by almightykim on 5/10/15.
 */
class StatRecord(
  val Time: Long, //This is at the start of the record
  val Count: Long,
  val Min: Double,
  val Mean: Double,
  val Max: Double
) {

}
