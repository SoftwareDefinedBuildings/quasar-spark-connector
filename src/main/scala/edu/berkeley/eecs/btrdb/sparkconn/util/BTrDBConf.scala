package edu.berkeley.eecs.btrdb.sparkconn.util

import java.io.FileInputStream
import java.util.Properties

/**
 * Created by almightykim on 6/13/15.
 */
class BTrDBConf(
  val btrdbconf:String,
  val cephconf:String
) extends Serializable {

  val (cmons, mserver, qcoll ) = {

    // get
    var qconf = new Properties
    qconf.load(new FileInputStream( if (btrdbconf == null || btrdbconf.isEmpty ) "/etc/quasar/quasar.conf" else btrdbconf))
    val mserver = qconf.getProperty("server").replace(" ", "")
    val qcoll:String = qconf.getProperty("collection").replace(" ", "")
    qconf = null


    // get ceph conf to extract initial monitoring members
    var cconf = new Properties
    cconf.load(new FileInputStream( if(cephconf == null || cephconf.isEmpty ) "/etc/ceph/ceph.conf" else cephconf))
    val cmons = cconf.getProperty("mon_initial_members").replace(" ", "").split(",").toSet
    cconf = null

    (cmons, mserver, qcoll)
  }

  def getCephMons : Set[String] = cmons

  def getMongoServer : String = mserver

  def getQuasarCollection : String = qcoll

}
