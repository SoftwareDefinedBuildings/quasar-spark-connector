package edu.berkeley.cs.sdb.btrdb.test

import java.util.HashMap

import com.mongodb.client.{FindIterable, MongoCollection, MongoDatabase}
import com.mongodb.{MongoClient, MongoClientURI}
import org.bson.Document

/**
 * Created by almightykim on 5/9/15.
 */
class MongoTest {

  def testConnectivity() = {

    val connstr: MongoClientURI = new MongoClientURI("mongodb://192.168.1.110:27017")
    val client: MongoClient = new MongoClient(connstr)
    val db: MongoDatabase = client.getDatabase("quasar")
    val coll: MongoCollection[Document] = db.getCollection("superblocks")

    var cur:FindIterable[Document] = null
    try{
      cur = coll.find()
      val myDoc: Document = coll.find(new Document("uuid", "2e43475f-5048-4153-4531-5f504f574552")).sort(new Document("gen",-1)).first
      println(myDoc)


      val param:HashMap[String,Object] = new HashMap[String, Object]()
      param.put("uuid","2e43475f-5048-4153-4531-5f504f574552")
      param.put("gen",Integer.valueOf(2))
      val qv: Document = coll.find(new Document(param)).first

      println(qv)

    }catch{
      case e: Exception => e.printStackTrace()
    }


  }

}
