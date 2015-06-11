package edu.berkeley.eecs.btrdb.sparkconn.quasar.types

class StatRecord(
  val Time: Long, //This is at the start of the record
  val Count: Long,
  val Min: Double,
  val Mean: Double,
  val Max: Double
) {

  override def toString: String = " Time [" + this.Time.toString + "] Count (" + this.Count.toString + ") Min <" + this.Min.toString + "> Mean {" + this.Mean.toString + "} Max [" + this.Max.toString + "]"

  def getTime:Long = Time

  def getCount:Long = Count

  def getMin:Double = Min

  def getMean:Double = Mean

  def getMax:Double = Max
}
