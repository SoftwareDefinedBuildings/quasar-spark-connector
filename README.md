# BTrDB - Spark Connector
BTrDB - Spark Connector

## Pre-requisites

- [Scala 2.10.4](http://www.scala-lang.org/download/2.10.4.html)  
- [Spark 1.1.1](http://www.apache.org/dyn/closer.cgi/spark/spark-1.1.1/spark-1.1.1-bin-hadoop2.4.tgz)  
- [Ceph 0.8](https://ceph.com/category/releases/)
- [BTrDB 3.0](https://github.com/SoftwareDefinedBuildings/quasar)    

## 1. Setting-up Rados-Java  
&dagger; You should setup rados-java support across the entire cluster prior to run the adapter.  

### Getting librados & libjna

Ubuntu  

    sudo apt-get install librados-dev libjna-java  

Centos  

    sudo yum install librados2-devel jna   
    

### Build rados-java & install  

    git clone --recursive https://github.com/ceph/rados-java.git  
    cd rados-java
    ant  
    
    sudo cp target/rados-0.1.3.jar /usr/share/java/rados-1.0-SNAPSHOT.jar
    sudo ln -s /usr/share/java/jna-3.2.7.jar /usr/lib/jvm/default-java/jre/lib/ext/jna-3.2.7.jar
    sudo ln -s /usr/share/java/rados-1.0-SNAPSHOT.jar  /usr/lib/jvm/default-java/jre/lib/ext/rados-1.0-SNAPSHOT.jar
    
    # documentation
    ant docs

* try run [rados-java example](http://ceph.com/docs/master/rados/api/librados-intro/#id5) from Ceph and make sure it runs without extra java directive. Some platforms give you difficulties to link against librados or libjna.

## 2. Building BTrDB Adapter  

    sbt assembly  


## 3. Examples  

### Statistical Aggregator  
Start spark shell with the adapter  

    $ spark-shell --jars <path to the adapter jar>/quasar-spark-connector-assembly-1.0.jar  

sc.quasarStatQuery("uuid", start-time, end-time, "unit-time", point-width)
    
    scala> import edu.berkeley.eecs.btrdb.sparkconn._
    scala> val qrdd = sc.quasarStatQuery("2e43475f-5359-5354-454d-5f5245414354", 1364823796L, 1398437046L, "ns", 16)
    scala> qrdd.map(i => i.toString).collect.foreach(println)

## TODO  
1. Support creating RDD for unprocessed, raw time-series  
2. Support Spark Stream  
3. Error handling

