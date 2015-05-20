package kr.re.keti.spark.qconnector.quasar

import com.google.common.primitives.{UnsignedInteger, UnsignedLong}
import kr.re.keti.spark.qconnector.quasar.types.{Generation, Superblock,StatRecord}
import kr.re.keti.spark.qconnector.cephreader.ReadDatablock

/**
 * Created by almightykim on 5/10/15.
 */
class QTree (
  val sb:Superblock,
  val gen:Generation,
  val root:QTreeNode,
  val commited:Boolean) {

  //TODO : fix this
  def this(sb:Superblock){
    //this(sb, sb.gen, sb.root, sb.unlinked)
    this(sb, null, null, sb.unlinked)
  }

  protected def copy(
    sb:Superblock = sb,
    gen:Generation = gen,
    root:QTreeNode = root,
    commited:Boolean = commited): QTree = {
    new QTree(sb, gen, root, commited)
  }

  def withNewRoot(newRoot:QTreeNode): QTree = {
    copy(sb, gen, newRoot, commited)
  }

  @throws(classOf[Exception])
  def LoadNode(addr:UnsignedLong , impl_Generation:UnsignedLong , impl_Pointwidth:UnsignedInteger, impl_StartTime:Long) : QTreeNode = {

    println("LoadNode() addr " + addr.toString(16) + " | impl_Generation " + impl_Generation.toString() +  " | impl_Pointwidth " + impl_Pointwidth.toString +  " | impl_StartTime " + impl_Pointwidth.toString(16))

    val db = ReadDatablock(sb.uuid, addr, impl_Generation, impl_Pointwidth, impl_StartTime)

    val n = new QTreeNode(this)



/*
    switch db.GetDatablockType() {
      case bstore.Vector:
      n.vector_block = db.(*bstore.Vectorblock)
      n.isLeaf = true
      case bstore.Core:
        n.core_block = db.(*bstore.Coreblock)
      n.isLeaf = false
      default:
        log.Panicf("What kind of type is this? %+v", db.GetDatablockType())
    }
    if n.ThisAddr() == 0 {
      log.Panicf("Node has zero address")
    }
    return n, nil
*/
    n
  }

  @throws(classOf[Exception])
  def QueryStatisticalValuesBlock(start:Long, end:Long, gen:UnsignedLong, pointwidth:UnsignedInteger) : Array[StatRecord] = {

    val rv = new Array[StatRecord](0)


    rv
  }


}