package edu.berkeley.eecs.btrdb.sparkconn

import java.io.FileInputStream
import java.util.{Properties, HashMap, UUID}

import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.{MongoClient, MongoClientURI}
import edu.berkeley.eecs.btrdb.sparkconn.quasar.qtree.QTree
import edu.berkeley.eecs.btrdb.sparkconn.quasar.types.{StatRecord, Superblock}
import edu.berkeley.eecs.btrdb.sparkconn.util.BTrDBConf
import org.bson.Document

import scala.collection.mutable.ListBuffer

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
  def LoadSuperblock(bconf:BTrDBConf, id:UUID, generation:Long) : Superblock = {

    val mserver = bconf.getMongoServer
    val qcoll:String = bconf.getQuasarCollection
    var rv: Superblock = null

    val connstr: MongoClientURI = new MongoClientURI("mongodb://" + mserver + ":27017")
    val client: MongoClient = new MongoClient(connstr)
    val db: MongoDatabase = client.getDatabase(qcoll)

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
  def NewReadQTree(bconf:BTrDBConf, id:UUID, generation:Long) : QTree = {

    val sb:Superblock = LoadSuperblock(bconf:BTrDBConf, id, generation)

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
  def QueryStatisticalValues(bconf:BTrDBConf, id:UUID, start:Long, end:Long, gen:Long, pointwidth:Int) : Iterator[StatRecord] = {

    val bclear = ~((1<<pointwidth.intValue) - 1)
    val st = start & bclear
    val ed = (end & bclear) - 1

    val tr:QTree = NewReadQTree(bconf, id, gen)

    val rv:ListBuffer[StatRecord] = tr.QueryStatisticalValuesBlock(st, ed, pointwidth)

    rv.iterator
  }


}