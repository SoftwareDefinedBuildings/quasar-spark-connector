package edu.berkeley.cs.sdb.btrdb.sparkconn

import java.util.{HashMap, UUID}

import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.{MongoClient, MongoClientURI}
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.qtree.QTree
import edu.berkeley.cs.sdb.btrdb.sparkconn.quasar.types.{StatRecord, Superblock}
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
 * Created by almightykim on 5/10/15.
 */

package object quasar {

  val MICROSECOND = 1000
  val MILLISECOND = 1000 * MICROSECOND
  val SECOND = 1000 * MILLISECOND
  val MINUTE = 60 * SECOND
  val HOUR = 60 * MINUTE
  val DAY = 24 * HOUR
  val ROOTPW = 56 //This makes each bucket at the root ~= 2.2 years
  //so the root spans 146.23 years
  val ROOTSTART = -1152921504606846976L //This makes the 16th bucket start at 1970 (0)
  val MinimumTime = -(16 << 56)
  val MaximumTime = (48 << 56)
  val LatestGeneration = Long.MaxValue //0xFFFFFFFF

  @throws(classOf[Exception])
  def LoadSuperblock(id:UUID, generation:Long) : Superblock = {

    println("quasar::LoadSuperblock")

    var rv: Superblock = null

    val connstr: MongoClientURI = new MongoClientURI("mongodb://192.168.1.110:27017")
    val client: MongoClient = new MongoClient(connstr)
    val db: MongoDatabase = client.getDatabase("quasar")
    val coll: MongoCollection[Document] = db.getCollection("superblocks")

    if (generation == LatestGeneration) {
      val qv: Document = coll.find(new Document("uuid", id.toString)).sort(new Document("gen",-1)).first

      if (qv == null) {
        throw new Exception("sb notfound!")
      }

      rv = new Superblock(qv)

    } else {

      val param:HashMap[String,Object] = new HashMap[String, Object]()
      param.put("uuid",id.toString)
      param.put("gen",Integer.valueOf(generation.intValue))
      val qv: Document = coll.find(new Document(param)).first

      if (qv == null) {
        throw new Exception("sb notfound!")
      }

      rv = new Superblock(qv)
    }

    rv
  }

  @throws(classOf[Exception])
  def NewReadQTree(id:UUID, generation:Long) : QTree = {

    println("quasar::NewReadQTree")

    val sb:Superblock = LoadSuperblock(id, generation)

    if (sb == null){
      throw new Exception("No Such Stream!")
    }

    var rv = new QTree(sb)
    if (sb.root != 0) {
      val rt = rv.LoadNode(sb.root, sb.gen, ROOTPW, ROOTSTART)
      rv.root = rt
    }
    rv
  }

  @throws(classOf[Exception])
  def QueryStatisticalValues(id:UUID, start:Long, end:Long, gen:Long, pointwidth:Int): (Array[StatRecord], Long) = {

    println("quasar::QueryStatisticalValues")

    val bclear = ~((1<<pointwidth.intValue) - 1)
    val st = start & bclear
    val ed = (end & bclear) - 1

    val tr:QTree = NewReadQTree(id, gen)

    val rv:ListBuffer[StatRecord] = tr.QueryStatisticalValuesBlock(st, ed, pointwidth)

    for (st <- rv){
      println(st.toString)
    }

    (rv.toArray, tr.Generation())
  }


}