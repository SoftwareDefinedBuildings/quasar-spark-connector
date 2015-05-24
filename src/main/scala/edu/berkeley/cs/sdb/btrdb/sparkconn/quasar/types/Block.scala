package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types

/**
 * Created by almightykim on 5/23/15.
 */
abstract class Block(
  //Metadata, not copied on clone
  val Identifier: Long,
  val Generation: Long,
  val PointWidth: Int,
  val StartTime: Long) {

  def GetDatablockType() : Long
  def Deserialize(src:Array[Byte]): Unit
}
