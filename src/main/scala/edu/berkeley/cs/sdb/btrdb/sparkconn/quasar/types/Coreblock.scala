package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.Core
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.blockstore.{ABSZERO, FULLZERO, KFACTOR, readSignedHuff, recompose}

import scala.util.control._

/**
 * Created by almightykim on 5/10/15.
 */
class Coreblock(
  //Metadata, not copied
  val Identifier: Long,
  val Generation: Long,

  //Payload, copied
  val PointWidth: Int,
  val StartTime: Long
) {

  val Addr: Array[Long] = new Array[Long](KFACTOR)
  val Count: Array[Long] = new Array[Long](KFACTOR)
  val Min: Array[Double] = new Array[Double](KFACTOR)
  val Mean: Array[Double] = new Array[Double](KFACTOR)
  val Max: Array[Double] = new Array[Double](KFACTOR)
  val CGeneration: Array[Long] = new Array[Long](KFACTOR)

  def Deserialize(src:Array[Byte]): Unit = {

    if (src(0).toInt != Core) {
      println("This is not a core block")
    }

    var idx:Int = 1

    def dedeltadeltarizer(maxdepth:Int) : (Long) => Long = {
      val hist_delta:Array[Long] = new Array[Long](maxdepth)
      var depth = 0
      var insidx = 0
      var last_value:Long = 0

      return (dd:Long) => {
        var total_dt:Long = 0

        for (i <- 0 until depth){
          total_dt += hist_delta(i)
        }

        var avg_dt:Long = 0

        if (depth > 0) {
          avg_dt = total_dt / depth
        }

        val curdelta = avg_dt + dd
        val curvalue = last_value + curdelta
        last_value = curvalue
        hist_delta(insidx) = curdelta
        insidx = (insidx + 1) % maxdepth
        depth += 1

        if (depth > maxdepth) {
          depth = maxdepth
        }

        last_value
      }
    }

    val delta_depth = 3
    val dd_addr = dedeltadeltarizer(delta_depth)
    val dd_cgen = dedeltadeltarizer(delta_depth)
    val dd_count = dedeltadeltarizer(delta_depth)
    val dd_mean_m = dedeltadeltarizer(delta_depth)
    val dd_mean_e  = dedeltadeltarizer(delta_depth)
    val dd_min_m = dedeltadeltarizer(delta_depth)
    val dd_min_e = dedeltadeltarizer(delta_depth)
    val dd_max_m = dedeltadeltarizer(delta_depth)
    val dd_max_e = dedeltadeltarizer(delta_depth)

    var itr = 0
    val lp = new Breaks

    lp.breakable{
      for (i <- 0 until KFACTOR) {

        val (addr_dd, used_0, bottom) = readSignedHuff(src.slice(idx,src.length))
        idx += used_0

        if (bottom == ABSZERO) {

          Addr(i) = 0
          Count(i) = 0
          //min/mean/max are undefined
          //Still have to decode cgen

          val (cgen_dd, used_1, _) = readSignedHuff(src.slice(idx,src.length))
          idx += used_1
          CGeneration(i) = dd_cgen(cgen_dd)

        } else if (bottom == FULLZERO) {
          lp.break
        } else {
          //Real value
          Addr(i) = dd_addr(addr_dd)

          var (cnt_dd, used_2, _) = readSignedHuff(src.slice(idx,src.length))
          idx += used_2

          var cgen_dd:Long = 0
          if ((cnt_dd & 1) != 0)
          {
            val (cgen_dd_t, used_3, _) = readSignedHuff(src.slice(idx,src.length))
            cgen_dd = cgen_dd_t
            idx += used_3
          }
          cnt_dd >>>= 1
          CGeneration(i) = dd_cgen(cgen_dd)
          Count(i) = dd_count(cnt_dd)

          var (min_m_dd, used_4, _) = readSignedHuff(src.slice(idx,src.length))
          idx += used_4

          var min_e_dd:Long = 0
          if ((min_m_dd & 1) != 0)
          {
            val (min_e_dd_t, used_5, _) = readSignedHuff(src.slice(idx,src.length))
            min_e_dd = min_e_dd_t
            idx += used_5
          } else {
            min_e_dd = 0
          }
          min_m_dd >>>= 1
          Min(i) = recompose(dd_min_e(min_e_dd), dd_min_m(min_m_dd))

          var (mean_m_dd, used_5, _) = readSignedHuff(src.slice(idx,src.length))
          idx += used_5

          var mean_e_dd:Long = 0
          if ((mean_m_dd & 1) != 0)
          {
            val (mean_e_dd_t, used_6, _) = readSignedHuff(src.slice(idx,src.length))
            mean_e_dd = mean_e_dd_t
            idx += used_6
          } else {
            mean_e_dd = 0
          }
          mean_m_dd >>>= 1
          Mean(i) = recompose(dd_mean_e(mean_e_dd), dd_mean_m(mean_m_dd))

          var (max_m_dd, used_7, _) = readSignedHuff(src.slice(idx,src.length))
          idx += used_7
          var max_e_dd:Long = 0
          if ((max_m_dd & 1) != 0)
          {
            val (max_e_dd_t, used_8, _) = readSignedHuff(src.slice(idx,src.length))
            max_e_dd = max_e_dd_t
            idx += used_8
          } else {
            max_e_dd = 0
          }
          max_m_dd >>>= 1
          Max(i) = recompose(dd_max_e(max_e_dd), dd_max_m(max_m_dd))
        }

        itr = i
      }
    }

    for (i <- (itr + 1) until KFACTOR){
      Addr(i) = 0
      Count(i) = 0
      CGeneration(i) = 0
    }

  }


}
