package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types

import java.util.UUID

import org.bson.Document

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

  def Gen() : Long = {
    this.gen
  }

  def Root() : Long =  {
    this.root
  }

  def Uuid() : UUID  = {
    return this.uuid
  }

  def Unlinked() : Boolean = {
    this.unlinked
  }

  override def toString: String = " UUID [" + this.uuid.toString + "] GEN (" + gen.toString + ") ROOT < 0x" + root.toHexString + "> UNLINKED {" + unlinked.toString + "} "
}
