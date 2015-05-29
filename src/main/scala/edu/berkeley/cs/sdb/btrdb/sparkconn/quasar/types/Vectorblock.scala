package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types

import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.blockstore.{VSIZE, Vector, readSignedHuff, readUnsignedHuff, recompose}

// The leaf datablock type. The tags allow unit tests
// to work out if clone / serdes are working properly
// metadata is not copied when a node is cloned
// implicit is not serialised

class Vectorblock (
  //Metadata, not copied on clone
  Identifier: Long,
  Generation: Long,
  PointWidth: Int,
  StartTime: Long
) extends Block(Identifier, Generation, PointWidth, StartTime) {

  //Payload, copied on clone
  var Len:Int = 0
  val Time: Array[Long] = new Array[Long](VSIZE)
  val Value: Array[Double] = new Array[Double](VSIZE)


  override def GetDatablockType() : Long = {
    Vector
  }

  def Deserialize(src:Array[Byte]): Unit = {
    val blocktype = src(0).toInt & 0xFF
    if (blocktype != Vector) {
      println("This is not a vector block")
    }

    this.Len = (src(1) & 0xFF) + ((src(2) & 0xFF) << 8)
    val length = this.Len
    var idx = 3

    val (m, l_0, _) = readUnsignedHuff(src.slice(idx,src.length))
    idx += l_0
    val (e, l_1, _) = readUnsignedHuff(src.slice(idx,src.length))
    idx += l_1
    val (t, l_2, _) = readUnsignedHuff(src.slice(idx,src.length))
    idx += l_2
    this.Time(0) = t
    this.Value(0) = recompose(e, m)

    //Keep delta history
    val delta_depth = 3
    val hist_deltas_t:Array[Long] = new Array[Long](delta_depth)
    val hist_deltas_e:Array[Long] = new Array[Long](delta_depth)
    val hist_deltas_m:Array[Long] = new Array[Long](delta_depth)
    var delta_idx = 0
    var num_deltas = 0

    var mm1 = m
    var em1 = e
    var tm1 = t

    for (i <- 1 until length) {
      //How many deltas do we have
      var deltas:Int = 0
      if (num_deltas > delta_depth) {
        deltas = delta_depth
      } else {
        deltas = num_deltas
      }

      //Calculate average deltas
      var dt_total:Long = 0
      var dm_total:Long = 0
      var de_total:Long = 0
      for (d <- 0 until deltas) {
        dt_total += hist_deltas_t(d)
        dm_total += hist_deltas_m(d)
        de_total += hist_deltas_e(d)
      }
      var adt:Long = 0
      var ade:Long = 0
      var adm:Long = 0
      if (deltas != 0)
      {
        adt = dt_total / deltas.toLong
        ade = de_total / deltas.toLong
        adm = dm_total / deltas.toLong
      }

      //Read the dd's
      var (ddm, l_3, _) = readSignedHuff(src.slice(idx,src.length))
      idx += l_3
      val dde:Long = 0
      val ddt:Long = 0

      if ((ddm & 2) != 0)
      {
        //log.Warning("re")
        var (dde, l_4, _) = readSignedHuff(src.slice(idx,src.length))
        idx += l_4
      }
      if ((ddm & 1) != 0) {
        //log.Warning("rt")
        val (ddt, l_5, _) = readSignedHuff(src.slice(idx,src.length))
        idx += l_5
      }
      ddm >>= 2
      //Convert dd's to d's
      val dm = ddm + adm
      val dt = ddt + adt
      val de = dde + ade

      //Save the deltas in the history
      hist_deltas_t(delta_idx) = dt
      hist_deltas_m(delta_idx) = dm
      hist_deltas_e(delta_idx) = de
      delta_idx += 1
      if (delta_idx == delta_depth) {
        delta_idx = 0
      }
      num_deltas += 1

      //Save values
      val e = em1 + de
      val m = mm1 + dm
      this.Time(i) = tm1 + dt
      this.Value(i) = recompose(e, m)
      em1 += de
      mm1 += dm
      tm1 += dt
    }

  }

}
