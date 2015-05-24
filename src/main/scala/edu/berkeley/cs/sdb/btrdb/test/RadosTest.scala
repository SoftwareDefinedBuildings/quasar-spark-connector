package edu.berkeley.cs.sdb.btrdb.test

import java.io.File

import com.ceph.rados.jna.{RadosObjectInfo, RadosPoolInfo}
import com.ceph.rados.{IoCTX, Rados}
import edu.berkeley.cs.sdb.btrdb.sparkconn.cephreader._

/**
 * Created by almightykim on 5/9/15.
 */
class RadosTest {

  def testConnectivity() = {
    val cluster: Rados = new Rados("admin")
    println("Created cluster handle.")

    val f: File = new File("/etc/ceph/ceph.conf")
    cluster.confReadFile(f)
    println("Read the configuration file.")

    cluster.connect
    println("Connected to the cluster.")

    val io: IoCTX = cluster.ioCtxCreate("data")

    val objects: Array[String] = io.listObjects


    for (`object` <- objects) {
      val info: RadosObjectInfo = io.stat(`object`)
      println("oid : " + info.getOid)
      println("mTime : " + info.getMtime)
      println("size : " + info.getSize)
      println("Xtra Info : " + io.getExtentedAttribute(`object`, "") + "\n")
    }

    val oid = "2e43475f53595354454d5f52454143540000003008"
    val buf:Array[Byte] = new Array[Byte](R_CHUNKSIZE)
    io.read(oid, R_CHUNKSIZE, 0, buf)

    //println("read result :: " + buf.slice(0, 100).mkString)

    println("auio : " + String.valueOf(io.getAuid) + " id : " + String.valueOf(io.getId))

    val poolStat: RadosPoolInfo = io.poolStat
    println("num_bytes : " + String.valueOf(poolStat.num_bytes))
    println("num_kb : " + String.valueOf(poolStat.num_kb))
    println("num_objects : " + String.valueOf(poolStat.num_objects))
    println("num_object_clones : " + String.valueOf(poolStat.num_object_clones))
    println("num_object_copies : " + String.valueOf(poolStat.num_object_copies))
    println("num_objects_missing_on_primary : " + String.valueOf(poolStat.num_objects_missing_on_primary))
    println("num_objects_unfound : " + String.valueOf(poolStat.num_objects_unfound))
    println("num_objects_degraded : " + String.valueOf(poolStat.num_objects_degraded))
    println("num_rd : " + String.valueOf(poolStat.num_rd))
    println("num_rd_kb : " + String.valueOf(poolStat.num_rd_kb))
    println("num_wr : " + String.valueOf(poolStat.num_wr))
    println("num_wr_kb : " + String.valueOf(poolStat.num_wr_kb))

    cluster.ioCtxDestroy(io)
  }
}
