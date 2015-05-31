package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.qtree

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.blockstore.ReadDatablock
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types._

import scala.collection.mutable.ListBuffer

class QTree (
  val sb:Superblock,
  val commited:Boolean) {

  def this(sb:Superblock){
    this(sb, sb.unlinked)
  }

  var root:QTreeNode = null

  def Generation() : Long = {
      return this.sb.Gen()
  }

  @throws(classOf[Exception])
  def LoadNode(addr:Long , impl_Generation:Long , impl_Pointwidth:Int, impl_StartTime:Long) : QTreeNode = {

    val n = new QTreeNode(this)
    ReadDatablock(n, sb.uuid, addr, impl_Generation, impl_Pointwidth, impl_StartTime)

    if (n.ThisAddr() == 0) {
      println("Node has zero address")
    }

    n
  }

  def QueryStatisticalValues(rv:ListBuffer[StatRecord], start:Long, end:Long, pw:Integer) = {

    //println("QTree::QueryStatisticalValues start <" + start.toString + "> end (" + end.toString + ") pw [" + pw.toString + "]")

    //Remember end is inclusive for QSV
    if (this.root != null) {
      this.root.QueryStatisticalValues(rv, start, end, pw)
    }
  }

  @throws(classOf[Exception])
  def QueryStatisticalValuesBlock(start:Long, end:Long, pw:Int) : ListBuffer[StatRecord] = {

    val rv:ListBuffer[StatRecord] = new ListBuffer[StatRecord]()
    this.QueryStatisticalValues(rv, start, end, pw)
    rv
  }

}
