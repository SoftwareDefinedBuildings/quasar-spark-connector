package kr.re.keti.spark.qconnector.quasar.types

/**
 * Created by almightykim on 5/10/15.
 */
class Generation(
  val Cur_SB: Superblock,
  val New_SB: Superblock,
  val cblocks: Array[Coreblock],
  val vblocks: Array[Vectorblock],
  val flushed: Boolean
) {

}
