import java.util.UUID

import com.google.common.primitives.{UnsignedLong, UnsignedInteger}
import kr.re.keti.spark.qconnector.quasar.LatestGeneration
import kr.keti._

object Hello {
  def main(args: Array[String]) = {
    println("Rados connectivity test!")

    val rtest: RadosTest = new RadosTest()
    //rtest.testConnectivity()

    var mtest: MongoTest = new MongoTest()
    //mtest.testConnectivity()

    //http://el-peso:9000/data/uuid/2e43475f-5359-5354-454d-5f5245414354?starttime=1364823796&endtime=1398437046&unitoftime=ns
    val qtest: QuasarProc = new QuasarProc()
    val rv, tr = qtest.QueryStatisticalValues(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"), 1364823796L, 1398437046L, LatestGeneration , UnsignedInteger.valueOf(8))
  }
}




