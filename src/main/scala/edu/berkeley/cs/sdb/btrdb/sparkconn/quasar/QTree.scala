package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar

import edu.berkeley.cs.sdb.btrdb.sparkconn.cephreader.ReadDatablock
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types._

/**
 * Created by almightykim on 5/10/15.
 */
class QTree (
  val sb:Superblock,
  val commited:Boolean) {

  def this(sb:Superblock){
    this(sb, sb.unlinked)
  }

  var root:QTreeNode = null
  var gen:Generation = null

  @throws(classOf[Exception])
  def LoadNode(addr:Long , impl_Generation:Long , impl_Pointwidth:Int, impl_StartTime:Long) : QTreeNode = {

    //println("LoadNode() addr " + addr.toHexString + " | impl_Generation " + impl_Generation.toString() +  " | impl_Pointwidth " + impl_Pointwidth.toString +  " | impl_StartTime " + impl_Pointwidth.toString)

    val n = new QTreeNode(this)
    ReadDatablock(n, sb.uuid, addr, impl_Generation, impl_Pointwidth, impl_StartTime)

    if (n.ThisAddr() == 0) {
      println("Node has zero address")
    }

    n
  }

  def QueryStatisticalValues(start:Long, end:Long, pw:Integer) = {
    //Remember end is inclusive for QSV
    if (this.root != null) {
      this.root.QueryStatisticalValues(null, start, end, pw)
    }
  }

  @throws(classOf[Exception])
  def QueryStatisticalValuesBlock(start:Long, end:Long, pw:Int) : Array[StatRecord] = {
    val rv = new Array[StatRecord](256)
    this.QueryStatisticalValues(start, end, pw)
    rv
  }

}
