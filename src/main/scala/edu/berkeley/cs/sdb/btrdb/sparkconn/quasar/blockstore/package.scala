package edu.berkeley.cs.sdb.btrdb.sparkconn.quasar

package object blockstore {

  val VALUE           = 0
  val ABSZERO         = 1
  val FULLZERO        = 2

  val VSIZE           = 1024
  val KFACTOR         = 64
  val VBSIZE          = 2 + 9*VSIZE + 9*VSIZE + 2*VSIZE //Worst case with huffman
  val CBSIZE          = 1 + KFACTOR*9*6
  val DBSIZE          = VBSIZE
  val PWFACTOR        = 6
  val RELOCATION_BASE = 0xFF00000000000000L



  def readUnsignedHuff(src:Array[Byte]) : (Long, Int, Int) = {
    var rv:Long = 0
    var idx = 1


    //println("src " + src.length + " : " + src.map("%02x" format _).mkString)

    def do_rest(iv:Long, itr:Int, n:Int) : (Long, Int) = {
      var r = iv
      var i = itr

      for (cnt <- 0 until n){
        r = (r << 8) | (src(i) & 0xFF)
        i += 1
      }

      return (r, i)
    }

    val iv:Int = src(0).toInt & 0xFF

    if (iv > 0xFE){
      println("This huffman symbol is reserved: +v", iv)

    } else if( iv == 0xFD ) {
      return (0, 1, ABSZERO)

    } else if( iv == 0xFE ) {
      return (0, 1, FULLZERO)

    } else if( iv == 0xFC) {
      val (r, i) = do_rest(0, idx, 8)
      rv = r
      idx = i
    } else if( iv >= 0xF8 ) {
      rv = iv & 0x03
      val (r, i) = do_rest(rv, idx, 7)
      rv = r
      idx = i
    } else if (iv >= 0xF4) {
      rv = iv & 0x03
      val (r, i) = do_rest(rv, idx, 6)
      rv = r
      idx = i
    } else if (iv >= 0xF0) {
      rv = iv & 0x03
      val (r, i) = do_rest(rv, idx, 5)
      rv = r
      idx = i
    } else if (iv >= 0xE0) {
      rv = iv & 0x0F
      val (r, i) = do_rest(rv, idx, 4)
      rv = r
      idx = i
    } else if (iv >= 0xD0) {
      rv = iv & 0x0F
      val (r, i) = do_rest(rv, idx, 3)
      rv = r
      idx = i
    } else if (iv >= 0xC0) {
      rv = iv & 0x0F
      val (r, i) = do_rest(rv, idx, 2)
      rv = r
      idx = i
    } else if (iv >= 0x80) {
      rv = iv & 0x3F
      val (r, i) = do_rest(rv, idx, 1)
      rv = r
      idx = i
    } else {
      rv = iv & 0x7F
    }

    (rv, idx, VALUE)
  }


  def readSignedHuff(src:Array[Byte]) : (Long, Int, Int) = {

    val (v, l, bv) = readUnsignedHuff(src)

    if (bv != VALUE) {
      return (0, 1, bv)
    }

    val s = v & 1
    val hv = v >>> 1

    if (s == 1) {
      return (-hv, l, VALUE)
    }

    (hv, l, VALUE)
  }


  def recompose(e:Long, m:Long) : Double = {
    val s = e & 1
    val el = e / 2

    var iv:Long = (
      (m & 0x00000000000000FFL) << (6*8) |
      (m & 0x000000000000FF00L) << (4*8) |
      (m & 0x0000000000FF0000L) << (2*8) |
      (m & 0x00000000FF000000L)          |
      (m & 0x000000FF00000000L) >>> (2*8) |
      (m & 0x0000FF0000000000L) >>> (4*8) |
      (m & 0x00FF000000000000L) >>> (6*8) )
    iv = (iv | (e << 52))
    iv = (iv | (s << 63))

    iv.toDouble
  }

}