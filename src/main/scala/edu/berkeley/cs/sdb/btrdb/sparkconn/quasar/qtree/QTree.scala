package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.qtree

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.blockstore.ReadDatablock
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types._

import scala.collection.mutable.ListBuffer

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

  def Generation() : Long = {
      return this.sb.Gen()
  }

  @throws(classOf[Exception])
  def LoadNode(addr:Long , impl_Generation:Long , impl_Pointwidth:Int, impl_StartTime:Long) : QTreeNode = {

    println("\n\nQTree::LoadNode() addr " + addr.toHexString + " | impl_Generation " + impl_Generation.toString() +  " | impl_Pointwidth " + impl_Pointwidth.toString +  " | impl_StartTime " + impl_Pointwidth.toString)

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

    println("QTree::QueryStatisticalValuesBlock start <" + start.toString + "> end (" + end.toString + ") pw [" + pw.toString + "]")

    val rv:ListBuffer[StatRecord] = new ListBuffer[StatRecord]()
    this.QueryStatisticalValues(rv, start, end, pw)
    rv
  }

}
