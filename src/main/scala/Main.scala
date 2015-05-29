import java.util.UUID

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.{LatestGeneration, QueryStatisticalValues}

object Main {

  def main(args: Array[String]) = {

    println("Quasar - Spark Test")
    //http://el-peso:9000/data/uuid/2e43475f-5359-5354-454d-5f5245414354?starttime=1364823796&endtime=1398437046&unitoftime=ns&pw=16
    val (rv, tr) = QueryStatisticalValues(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"), 1364823796L, 1398437046L, LatestGeneration , 16)

    //http://el-peso:9000/data/uuid/2e43475f-5359-5354-454d-5f5245414354?starttime=1396310400&endtime=1396396800&unitoftime=ns&pw=16
    //val (rv, tr) = QueryStatisticalValues(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"), 1396310400L, 1396396800L, LatestGeneration , 16)
  }
}




