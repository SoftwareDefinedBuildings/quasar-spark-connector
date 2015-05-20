package kr.re.keti.spark.qconnector.quasar

import kr.re.keti.spark.qconnector.quasar.types.{Coreblock, Vectorblock}


/**
 * Created by almightykim on 5/10/15.
 */
class QTreeNode(
  val tr: QTree,
  var vector_block: Vectorblock,
  var core_block: Coreblock,
  var isLeaf: Boolean,
  var child_cache: Array[QTreeNode],
  var parent: QTreeNode,
  var isNew: Boolean) {

  def this(qtree:QTree) = {
    this(qtree,null, null, false, null, null, true)
  }


}