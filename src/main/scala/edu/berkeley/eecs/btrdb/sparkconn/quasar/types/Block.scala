package edu.berkeley.eecs.btrdb.sparkconn.quasar.types

abstract class Block(
  //Metadata, not copied on clone
  val Identifier: Long,
  val Generation: Long,
  val PointWidth: Int,
  val StartTime: Long) {

  def GetDatablockType() : Long
  def Deserialize(src:Array[Byte]): Unit
}
