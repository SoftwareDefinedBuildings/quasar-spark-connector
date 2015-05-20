package kr.re.keti.spark.qconnector.quasar.types

import java.util.UUID

import com.google.common.primitives.UnsignedLong
import org.bson.Document

/**
 * Created by almightykim on 5/10/15.
 */
class Superblock(
  val uuid:UUID,
  val gen:UnsignedLong,
  val root:UnsignedLong,
  val unlinked:Boolean) {

  def this(document:Document) = {
    this( UUID.fromString(document.get("uuid").toString),
          UnsignedLong.valueOf(document.get("gen").toString),
          UnsignedLong.valueOf(document.get("root").toString),
          document.get("unlinked").toString.toBoolean)
  }

  def uid: String = uuid.toString

  override def toString: String = " UUID [" + this.uid + "] GEN (" + gen.toString + ") ROOT <" + root.toString + "> UNLINKED {" + unlinked.toString + "} "
}
