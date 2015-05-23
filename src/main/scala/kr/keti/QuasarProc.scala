package kr.keti

import java.util.UUID

import kr.re.keti.spark.qconnector.quasar.types.{Superblock, StatRecord}
import kr.re.keti.spark.qconnector.quasar.{ LoadSuperblock, ROOTPW, ROOTSTART}
import kr.re.keti.spark.qconnector.cephreader.{make_object_id, handle_read, R_CHUNKSIZE}
import kr.re.keti.spark.qconnector.quasar.NewReadQTree
/**
 * Created by almightykim on 5/10/15.
 */
class QuasarProc {


  //def QueryStatisticalValues(id:UUID, start:Long, end:Long, gen:UnsignedLong, pointwidth:UnsignedInteger) : {val rv: Array[StatRecord]; val tr: UnsignedLong} = {
  def QueryStatisticalValues(id:UUID, start:Long, end:Long, gen:Long, pointwidth:Int) = {

    println("QueryStatisticalValues")

    try{
      //var rv = null

      val bclear = ~((1<<pointwidth.intValue) - 1)
      val st = start & bclear
      val ed = (end & bclear) - 1

      val tr = NewReadQTree(id, gen)


      val rv = tr.QueryStatisticalValuesBlock(st, ed, gen, pointwidth)


      (tr,rv)
/*
      return (rv, tr.Generation(), nil)


      val sb:Superblock = LoadSuperblock(id, gen)

      //println("id : " + id.toString +  " bclear : " + bclear.toBinaryString + " st : " + st.toBinaryString + " ed : " + ed.toBinaryString )
      println("sb " + sb.toString)

      val address = 206243164491L

      val offset = address & 0xFFFFFFL
      //val id = address >> 24

      //ReadDatablock(id, sb.root, sb.gen, ROOTPW, ROOTSTART)
      val oid = make_object_id(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"),UnsignedLong.valueOf(address >> 24))
      println(oid)

      val ret = handle_read(UUID.fromString("2e43475f-5048-4153-4531-5f504f574552"), UnsignedLong.valueOf(address + R_CHUNKSIZE), R_CHUNKSIZE)
*/

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

  }



}
