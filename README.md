# BTrDB - Spark Connector
BTrDB - Spark Connector

## Pre-requisites

- [Scala 2.10.4](http://www.scala-lang.org/download/2.10.4.html)  
- [Spark 1.1.1](http://www.apache.org/dyn/closer.cgi/spark/spark-1.1.1/spark-1.1.1-bin-hadoop2.4.tgz)  
- [Ceph 0.8](https://ceph.com/category/releases/)
- [BTrDB 3.0](https://github.com/SoftwareDefinedBuildings/quasar)    

## Setup Rados-Java  
* You must setup rados-java support across the entire cluster.  

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

