package kr.re.keti.spark.qconnector

import java.io.File
import java.math.BigInteger
import java.util.UUID
import com.ceph.rados.{IoCTX, Rados}
import com.google.common.primitives.{UnsignedInteger, UnsignedLong}
import kr.re.keti.spark.qconnector.quasar.types.Coreblock

/** Contains [[kr.re.keti.spark.qconnector]] class that is the main entry point for
  * analyzing Quasar data from Spark. */
package object cephreader {

  //I"m going for one per core on a decent server
  val NUM_RHANDLES = 40

  //We know we won"t get any addresses here, because this is the relocation base as well
  val METADATA_BASE = 0xFF00000000000000L

  //4096 blocks per addr lock
  val ADDR_LOCK_SIZE = 0x1000000000L
  val ADDR_OBJ_SIZE = 0x0001000000L

  //Just over the DBSIZE
  val MAX_EXPECTED_OBJECT_SIZE = 20485

  //The number of RADOS blocks to cache (up to 16MB each, probably only 1.6MB each)
  val RADOS_CACHE_SIZE = NUM_RHANDLES * 2

  val OFFSET_MASK = 0xFFFFFF
  val R_CHUNKSIZE = 1 << 20

  //This is how many uuid/address pairs we will keep to facilitate appending to segments
  //instead of creating new ones.
  val WORTH_CACHING = OFFSET_MASK - MAX_EXPECTED_OBJECT_SIZE
  val SEGCACHE_SIZE = 1024

  // 1MB for write cache, I doubt we will ever hit this tbh
  val WCACHE_SIZE = 1<<20
  val COMP_CAP_STEP = 64
  val OID_SIZE = 43 //32 for uuid, 10 for id, 1 for nul

  val R_ADDRMASK = 0xFFFFFFFFFFF00000L //^((uint64(1) << 20) - 1)
  val R_OFFSETMASK = 0xFFFFF           //  (uint64(1) << 20) - 1

  val Vector = 1
  val Core = 2
  val Bad = 255

  val nibbles:Array[Char] = Array[Char]('0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f')

  def DatablockGetBufferType(buf:Array[Byte]) = {
    buf(0) match {
      case Vector => Vector
      case Core => Core
      case default => Bad
    }
  }


  def make_object_id(uuid:UUID, address:UnsignedLong) : String = {

    val uid = uuid.toString.replace("-","").toCharArray
    val dest:Array[Char] = new Array[Char](OID_SIZE)

    for (i <- 0 until uid.length)
    {
      dest(i) = uid(i)
    }


    for (i <- 0 until 10)
    {
      val nidx = ( address.longValue >> (4*(9 - i)) ) & 0xF
      dest(32+i) = nibbles(nidx.toInt)
    }

    dest(OID_SIZE-1) = 0

    dest.mkString
  }

  def handle_read(uuid:UUID, address:UnsignedLong, len:Int) : Array[Byte] = {

    //The ceph provider uses 24 bits of address per object, and the top 40 bits as an object ID
    val offset = address.longValue & 0xFFFFFF
    val id = address.longValue >> 24
    val oid = make_object_id(uuid, UnsignedLong.valueOf(id))

    //handle_read :: uuid[20] (.CG_SYSTEM_REACT5359) | address 0x3008100000 | len 1048576 | offset 1048576 | id 0x3008 | oid 2e43475f53595354454d5f52454143540000003008
    println("handle_read :: uuid[" + uuid.toString.length + "] (" + uuid.toString + ") | address " + address.toString(16) + " | len " + len.toString +  " | offset " + offset.toString + " | id " + UnsignedLong.valueOf(id).toString(16) + " | oid " + oid )

    val cluster: Rados = new Rados("admin")
    println("Created cluster handle.")

    val f: File = new File("/etc/ceph/ceph.conf")
    cluster.confReadFile(f)
    println("Read the configuration file.")

    cluster.connect
    println("Connected to the cluster.")

    val io:IoCTX = cluster.ioCtxCreate("data")

    val buf:Array[Byte] = new Array[Byte](len.intValue)
    val rv:Int = io.read(oid, len.intValue, offset.longValue, buf)

    println("rc value : " + rv + " length requested : " + len)

    if (rv < 0)
    {
      println("could not read " + oid)
      null
    }

    //errno = 0;
    buf.slice(0, rv)
  }

  def obtainChunk(uuid:UUID, address:UnsignedLong) : Array[Byte] = {
    handle_read(uuid, address, R_CHUNKSIZE)
  }

  def Read(uuid:UUID, address:UnsignedLong) = {

    val rv:Array[Byte] = obtainChunk(uuid, UnsignedLong.valueOf(address.longValue() & R_ADDRMASK))
    var chunk1:Array[Byte] = rv.slice((address.longValue() & R_OFFSETMASK).toInt, rv.length)

    var chunk2:Array[Byte] = null
    var ln:Int = 0

    if (false) {
      val addr = UnsignedLong.valueOf(address.plus(UnsignedLong.valueOf(R_CHUNKSIZE)).longValue() & R_ADDRMASK)
      println("Read Chunk1 : " + chunk1.map("%02X" format _).mkString.toLowerCase)
      println("Addr check : 0x" + addr.toString(16))
    }

    if (chunk1.length < 2) {
      //not even long enough for the prefix, must be one byte in the first chunk, one in teh second
      val addr = UnsignedLong.valueOf(address.plus(UnsignedLong.valueOf(R_CHUNKSIZE)).longValue() & R_ADDRMASK)
      chunk2 = obtainChunk(uuid, addr)

      ln = chunk1(0).toInt + (chunk2(0).toInt << 8)
      chunk2 = chunk2.slice(1, chunk2.length)
      chunk1 = chunk1.slice(1, chunk1.length)

    } else {

      ln = chunk1(0).toInt + (chunk1(1).toInt << 8)
      chunk1 = chunk1.slice(2, chunk1.length)
    }

    if (ln > MAX_EXPECTED_OBJECT_SIZE) {
      throw new Exception("WTUF: " +  String.valueOf(ln))
    }

    var copied:Int = 0
    val buffer:Array[Byte] = new Array[Byte](ln)

    if (chunk1.length > 0)
    {
      //We need some bytes from chunk1
      var end = ln
      if (chunk1.length < ln) {
        end = chunk1.length
      }

      copied = end

      for (i <- 0 until end){
        buffer(i) = chunk1(i)
      }
    }

    println("ln  : " + String.valueOf(ln) + " copied : " + String.valueOf(copied))

    if (copied < ln) {

      //We need some bytes from chunk2
      if (chunk2 == null) {
        val addr = UnsignedLong.valueOf(address.plus(UnsignedLong.valueOf(R_CHUNKSIZE)).longValue() & R_ADDRMASK)
        chunk2 = obtainChunk(uuid, addr)
      }

      //copy(buffer[copied:], chunk2[:ln - copied])
      for (i <- 0 until (ln - copied)){
        buffer(copied + i) = chunk2(i)
      }
    }

    if (ln < 2) {
      println("This is unexpected")
    }

    println("Read final : " + buffer.map("%02x" format _).mkString)

    buffer
  }

  def ReadDatablock(uuid:UUID, addr:UnsignedLong, impl_Generation:UnsignedLong, impl_Pointwidth:UnsignedInteger, impl_StartTime:Long) =  {

    val trimbuf:Array[Byte] = Read(uuid, addr)

    var rv = null
    DatablockGetBufferType(trimbuf) match {
      case Core => {
        rv = new Coreblock(

        )

      }
      case Vector => {

      }
    }

  }




}
