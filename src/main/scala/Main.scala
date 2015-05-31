import java.util.UUID

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types.StatRecord
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.{LatestGeneration, QueryStatisticalValues}

object Main {

  def main(args: Array[String]) = {

    //http://el-peso:9000/data/uuid/2e43475f-5359-5354-454d-5f5245414354?starttime=1364823796&endtime=1398437046&unitoftime=ns&pw=16
    val (rv, tr) = QueryStatisticalValues(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"), 1364823796L, 1398437046L, LatestGeneration , 16)

    for (st:StatRecord <- rv){
      println("[" + st.Time.toString + ",0," + st.Min.toString + "," + st.Mean.toString + "," + st.Max.toString + "," + st.Count.toString + "]")
    }

  }
}




