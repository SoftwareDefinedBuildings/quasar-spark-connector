import java.util.UUID

import com.google.common.primitives.{UnsignedLong, UnsignedInteger}
import kr.re.keti.spark.qconnector.quasar.LatestGeneration
import kr.keti._

import scala.annotation.tailrec

object Hello {

  def hex2bytes(hex: String): Array[Byte] = {
    hex.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
  }

  def main(args: Array[String]) = {
    println("Rados connectivity test!")

/*
    val rtest: RadosTest = new RadosTest()
    //rtest.testConnectivity()

    var mtest: MongoTest = new MongoTest()
    mtest.testConnectivity()
*/

    //http://el-peso:9000/data/uuid/2e43475f-5359-5354-454d-5f5245414354?starttime=1364823796&endtime=1398437046&unitoftime=ns&pw=16
    //http://el-peso:9000/data/uuid/2e43475f-5359-5354-454d-5f5245414354?starttime=1364823796&endtime=1398437046&unitoftime=ns
    val qtest: QuasarProc = new QuasarProc()
    val (rv, tr) = qtest.QueryStatisticalValues(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"), 1364823796L, 1398437046L, LatestGeneration , 16)


/*
    val src:Array[Byte] = hex2bytes("02fd00fd00fd00fd00fd00fd00fd00fd00fd00fd00fd00fd00fd00fd00fd00fd00f060102f2dd4d018d59a04a0029022fb99b9501d14b40e9014c14026901cfe")

    //var rv:Long = 0
    //var i = 1


    @tailrec
    def do_rest(rv:Long, i:Int, n:Int) : (Long,Int) = {
      if (n == 0) {
        return (rv, i)
      }
      val r = (rv << 8) | (src(i) & 0xFF).toLong
      println ("r (" + r.toHexString + ") i <" + i + ">")

      return do_rest(r, (i + 1), (n - 1))
    }

//    val (rv, i) = do_rest(0, 1, 8)

    def do_rest(iv:Long, i:Int, n:Int) : (Long, Int) = {
      var rv = iv
      var itr = i

      for (cnt <- 0 until n){
        rv = (rv << 8) | (src(itr) & 0xFF)
        itr += 1
      }

      return (rv, itr)
    }

    val (r, i) = do_rest(0, 1, 8)
    println ("r (" + r.toHexString.toUpperCase + ") i <" + i + ">")
*/


  }
}




