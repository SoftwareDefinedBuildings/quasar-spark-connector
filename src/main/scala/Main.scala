import java.util.UUID

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.LatestGeneration
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.blockstore._
import edu.berkeley.cs.sdb.btrdb.test.QuasarTest

object Main {
  def main(args: Array[String]) = {
    val qtest: QuasarTest = new QuasarTest()
    val (rv, tr) = qtest.QueryStatisticalValues(UUID.fromString("2e43475f-5359-5354-454d-5f5245414354"), 1396310400L, 1396396800L, LatestGeneration , 16)
  }
}




