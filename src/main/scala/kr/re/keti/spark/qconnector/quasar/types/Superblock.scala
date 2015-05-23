package kr.re.keti.spark.qconnector.quasar.types

import java.util.UUID

import org.bson.Document

/**
 * Created by almightykim on 5/10/15.
 */
class Superblock(
  val uuid:UUID,
  val gen:Long,
  val root:Long,
  val unlinked:Boolean) {

  def this(document:Document) = {
    this( UUID.fromString(document.get("uuid").toString),
          document.get("gen").toString.toLong,
          document.get("root").toString.toLong,
          document.get("unlinked").toString.toBoolean)
  }

  def uid: String = uuid.toString

  override def toString: String = " UUID [" + this.uid + "] GEN (" + gen.toString + ") ROOT <" + root.toString + "> UNLINKED {" + unlinked.toString + "} "
}
