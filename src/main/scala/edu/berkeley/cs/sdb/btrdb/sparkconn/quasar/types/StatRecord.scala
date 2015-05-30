package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types

class StatRecord(
  val Time: Long, //This is at the start of the record
  val Count: Long,
  val Min: Double,
  val Mean: Double,
  val Max: Double
) {

  override def toString: String = " Time [" + this.Time.toString + "] Count (" + this.Count.toString + ") Min <" + this.Min.toString + "> Mean {" + this.Mean.toString + "} Max [" + this.Max.toString + "]"

}
